use serde::{Deserialize, Serialize};
use std::{
    collections::BTreeMap,
    path::PathBuf,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
};
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum State {
    Queued,
    Running,
    Completed,
    Failed,
    Cancelled,
    Interrupted,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Job {
    pub id: String,
    pub request_hash: String,
    pub idempotency_key: String,
    #[serde(default)]
    pub cancel_keys: std::collections::BTreeSet<String>,
    pub state: State,
    pub progress: u8,
    pub artifact: Option<String>,
    pub error: Option<String>,
    pub created_ms: u64,
    pub pinned: bool,
}
#[derive(Clone)]
pub struct Jobs {
    io: Arc<Mutex<()>>,
    path: PathBuf,
    items: Arc<Mutex<BTreeMap<String, Job>>>,
    cancel: Arc<Mutex<BTreeMap<String, Arc<AtomicBool>>>>,
}
impl Jobs {
    pub fn open(path: PathBuf) -> Result<Self, String> {
        std::fs::create_dir_all(path.parent().ok_or("INVALID_CATALOG_PATH")?)
            .map_err(|e| e.to_string())?;
        let mut items: BTreeMap<String, Job> = if path.exists() {
            serde_json::from_slice(&std::fs::read(&path).map_err(|e| e.to_string())?)
                .map_err(|e| e.to_string())?
        } else {
            BTreeMap::new()
        };
        for j in items.values_mut() {
            if matches!(j.state, State::Queued | State::Running) {
                j.state = State::Interrupted;
                j.error = Some("process restarted before completion".into())
            }
        }
        let s = Self {
            io: Default::default(),
            path,
            items: Arc::new(Mutex::new(items)),
            cancel: Default::default(),
        };
        s.persist()?;
        Ok(s)
    }
    fn persist(&self) -> Result<(), String> {
        let _io = self.io.lock().map_err(|_| "CATALOG_LOCK_POISONED")?;
        let bytes = serde_json::to_vec(&*self.items.lock().unwrap()).map_err(|e| e.to_string())?;
        let tmp = self.path.with_extension("tmp");
        use std::io::Write;
        let mut file = std::fs::File::create(&tmp).map_err(|e| e.to_string())?;
        file.write_all(&bytes)
            .and_then(|_| file.sync_all())
            .map_err(|e| e.to_string())?;
        std::fs::rename(tmp, &self.path).map_err(|e| e.to_string())
    }
    pub fn enqueue(
        &self,
        idempotency_key: String,
        hash: String,
    ) -> Result<(Job, bool, Arc<AtomicBool>), String> {
        let mut items = self.items.lock().unwrap();
        if items
            .values()
            .any(|j| j.idempotency_key == idempotency_key && j.request_hash != hash)
        {
            return Err("IDEMPOTENCY_CONFLICT".into());
        }
        if let Some(job) = items.get(&hash).cloned() {
            drop(items);
            let cancel = self.cancel.lock().unwrap().entry(hash).or_default().clone();
            return Ok((job, false, cancel));
        }
        let job = Job {
            id: hash.clone(),
            request_hash: hash.clone(),
            idempotency_key,
            cancel_keys: Default::default(),
            state: State::Queued,
            progress: 0,
            artifact: None,
            error: None,
            created_ms: chrono::Utc::now().timestamp_millis() as u64,
            pinned: false,
        };
        items.insert(hash.clone(), job.clone());
        drop(items);
        let cancel = Arc::new(AtomicBool::new(false));
        self.cancel.lock().unwrap().insert(hash, cancel.clone());
        self.persist()?;
        Ok((job, true, cancel))
    }
    pub fn get(&self, id: &str) -> Result<Job, String> {
        self.items
            .lock()
            .unwrap()
            .get(id)
            .cloned()
            .ok_or("JOB_NOT_FOUND".into())
    }
    pub fn update(
        &self,
        id: &str,
        state: State,
        artifact: Option<String>,
        error: Option<String>,
    ) -> Result<(), String> {
        let mut items = self.items.lock().unwrap();
        let j = items.get_mut(id).ok_or("JOB_NOT_FOUND")?;
        if j.state == State::Cancelled && state != State::Cancelled {
            return Ok(());
        }
        j.progress = match state {
            State::Completed => 100,
            State::Running => 10,
            _ => j.progress,
        };
        j.state = state;
        j.artifact = artifact;
        j.error = error;
        drop(items);
        self.persist()
    }
    pub fn cancel(&self, id: &str, key: &str) -> Result<Job, String> {
        {
            let mut items = self.items.lock().unwrap();
            if items
                .values()
                .any(|j| j.id != id && j.cancel_keys.contains(key))
            {
                return Err("IDEMPOTENCY_CONFLICT".into());
            }
            items
                .get_mut(id)
                .ok_or("JOB_NOT_FOUND")?
                .cancel_keys
                .insert(key.into());
        }
        let job = self.get(id)?;
        if matches!(job.state, State::Queued | State::Running) {
            if let Some(c) = self.cancel.lock().unwrap().get(id) {
                c.store(true, Ordering::Release)
            }
            self.update(id, State::Cancelled, job.artifact, None)?;
        }
        self.get(id)
    }
}

impl Jobs {
    pub fn snapshot(&self) -> Vec<Job> {
        self.items.lock().unwrap().values().cloned().collect()
    }
    pub fn pin(&self, id: &str, pinned: bool) -> Result<(), String> {
        self.items
            .lock()
            .unwrap()
            .get_mut(id)
            .ok_or("JOB_NOT_FOUND")?
            .pinned = pinned;
        self.persist()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn restart_marks_job_interrupted() {
        let p = std::env::temp_dir()
            .join(format!("okx-catalog-{}", std::process::id()))
            .join("jobs.json");
        let _ = std::fs::remove_file(&p);
        let s = Jobs::open(p.clone()).unwrap();
        s.enqueue("fixture-key".into(), "fixture".into()).unwrap();
        s.update("fixture", State::Running, None, None).unwrap();
        let t = Jobs::open(p.clone()).unwrap();
        assert_eq!(t.get("fixture").unwrap().state, State::Interrupted);
        std::fs::remove_file(p).unwrap();
    }
}
