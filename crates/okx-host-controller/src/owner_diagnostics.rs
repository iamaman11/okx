//! One bounded, read-only observation of the existing process owners.
//!
//! This module only projects already-observed CONTROL/Engine/Research facts.
//! It never probes Edge, starts an executable, opens finance SQL, or treats an
//! authenticated process claim as independent installation evidence.

use okx_host_protocol::{ResearchRuntimePhase, ResearchRuntimeStatus};
use serde::Serialize;
use serde_json::Value;

pub const SCHEMA: &str = "okx.host-control.owner-diagnostics/v1";

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
enum Owner {
    Control,
    Engine,
    Research,
    Edge,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
enum Evidence {
    Observed,
    NotObserved,
    Unverified,
    Blocked,
    NotEvaluated,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
enum NextAction {
    None,
    InspectControllerUpdate,
    InspectEngineProvenance,
    InspectEngineLifecycle,
    InspectResearchStatus,
    InspectEdgeRuntime,
}

#[derive(Serialize)]
struct OwnerObservation {
    owner: Owner,
    reference: &'static str,
    contract_schema: &'static str,
    state: Evidence,
    #[serde(skip_serializing_if = "Option::is_none")]
    image_sha256: Option<String>,
    /// Provenance of the hash, not a claim that a running process was rehashed.
    image_basis: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    source_tree: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    process_generation: Option<u64>,
    reason_code: &'static str,
    next_action: NextAction,
}

#[derive(Serialize)]
struct RuntimeObservation {
    schema: &'static str,
    reference: &'static str,
    read_only: bool,
    quality: &'static str,
    as_of_ms: i64,
    first_unproven_owner: Owner,
    first_unproven_stage: &'static str,
    next_action: NextAction,
    owners: [OwnerObservation; 4],
}

fn exact_hex(value: Option<&str>, length: usize) -> Option<String> {
    value
        .filter(|v| v.len() == length && v.bytes().all(|b| b.is_ascii_hexdigit()))
        .map(str::to_owned)
}

fn text_at<'a>(value: &'a Value, pointer: &str) -> Option<&'a str> {
    value.pointer(pointer).and_then(Value::as_str)
}

/// The caller already owns the typed Research status and the independently
/// hashed CONTROL/Engine provenance. All other inputs are immutable snapshots.
pub fn summarize(
    launcher: &Value,
    controller_sha256: Option<&str>,
    installed_agent: &Value,
    engine_running: bool,
    engine_desired_running: bool,
    release_parity: &Value,
    research: Option<&ResearchRuntimeStatus>,
) -> Value {
    let control_image = exact_hex(controller_sha256, 64);
    let expected_control_image = exact_hex(
        text_at(launcher, "/active/value/active/controller_sha256"),
        64,
    );
    let control_verified = control_image.is_some()
        && control_image == expected_control_image
        && text_at(launcher, "/active/state") == Some("READY")
        && launcher.pointer("/entrypoint_is_launcher").and_then(Value::as_bool) == Some(true)
        && text_at(launcher, "/pending/state") == Some("NONE");
    let control = OwnerObservation {
        owner: Owner::Control,
        reference: "active_controller",
        contract_schema: "okx.host-launcher.active/v1",
        state: if control_verified {
            Evidence::Observed
        } else {
            Evidence::Unverified
        },
        image_sha256: control_image,
        image_basis: "current_controller_executable",
        source_tree: exact_hex(text_at(launcher, "/active/value/active/source_tree"), 40),
        process_generation: None,
        reason_code: if control_verified {
            "CONTROL_IMAGE_OBSERVED"
        } else {
            "CONTROL_IMAGE_UNVERIFIED"
        },
        next_action: if control_verified {
            NextAction::None
        } else {
            NextAction::InspectControllerUpdate
        },
    };

    let engine_provenance = text_at(installed_agent, "/state") == Some("VERIFIED");
    let engine_image = if engine_provenance {
        exact_hex(text_at(installed_agent, "/binary_sha256"), 64)
    } else {
        None
    };
    let engine_proved = engine_image.is_some();
    let engine = OwnerObservation {
        owner: Owner::Engine,
        reference: "engine_process_and_installed_image",
        contract_schema: "okx.host-control.installed-agent/v1",
        state: if engine_running && engine_proved {
            Evidence::Observed
        } else if engine_running {
            Evidence::Blocked
        } else if engine_desired_running {
            Evidence::Unverified
        } else {
            Evidence::NotObserved
        },
        image_sha256: engine_image,
        image_basis: "installed_engine_executable",
        source_tree: exact_hex(text_at(installed_agent, "/provenance/source_tree"), 40),
        process_generation: None,
        reason_code: if engine_running && engine_proved {
            "ENGINE_PROCESS_OBSERVED_INSTALLED_IMAGE_VERIFIED"
        } else if engine_running {
            "ENGINE_IMAGE_UNVERIFIED"
        } else if engine_desired_running {
            "ENGINE_PROCESS_NOT_OBSERVED"
        } else {
            "ENGINE_STOPPED_AS_REQUESTED"
        },
        next_action: if engine_running && !engine_proved {
            NextAction::InspectEngineProvenance
        } else if engine_desired_running && !engine_running {
            NextAction::InspectEngineLifecycle
        } else {
            NextAction::None
        },
    };

    let research_proved = research.is_some_and(|r| {
        r.phase == ResearchRuntimePhase::RunningUnverified
            && r.desired_running
            && r.process_generation > 0
            && r.pid.is_some_and(|pid| pid > 0)
            && exact_hex(r.active_worker_sha256.as_deref(), 64).is_some()
            && r.active_worker_sha256 == r.installed_worker_sha256
    });
    let research_expected_stopped = research.is_some_and(|r| {
        r.phase == ResearchRuntimePhase::Stopped && !r.desired_running
    });
    let research_invalid =
        research.is_some_and(|r| r.phase == ResearchRuntimePhase::InvalidProvenance);
    let research_owner = OwnerObservation {
        owner: Owner::Research,
        reference: "research_process_and_installed_image",
        contract_schema: "okx.host-control.research-runtime/v1",
        state: if research_proved {
            Evidence::Observed
        } else if research_invalid {
            Evidence::Blocked
        } else if research_expected_stopped {
            Evidence::NotObserved
        } else {
            Evidence::Unverified
        },
        image_sha256: research.and_then(|r| exact_hex(r.active_worker_sha256.as_deref(), 64)),
        image_basis: "research_supervisor_active_worker",
        source_tree: None,
        process_generation: research.map(|r| r.process_generation),
        reason_code: if research_proved {
            "RESEARCH_PROCESS_AND_IMAGE_OBSERVED_EDGE_UNVERIFIED"
        } else if research_invalid {
            "RESEARCH_IMAGE_INVALID"
        } else if research_expected_stopped {
            "RESEARCH_STOPPED_AS_REQUESTED"
        } else {
            "RESEARCH_STATUS_UNVERIFIED"
        },
        next_action: if research_proved || research_expected_stopped {
            NextAction::None
        } else {
            NextAction::InspectResearchStatus
        },
    };
    // CONTROL has no independent authority to attest the Edge WSS session.
    let edge = OwnerObservation {
        owner: Owner::Edge,
        reference: "authenticated_edge_session",
        contract_schema: "okx.direct-transport.status/v1",
        state: Evidence::NotEvaluated,
        image_sha256: None,
        image_basis: "none",
        source_tree: None,
        process_generation: None,
        reason_code: "EDGE_SESSION_NOT_EVALUATED_BY_CONTROL",
        next_action: NextAction::InspectEdgeRuntime,
    };

    // The first unproven link is deterministic; never infer successful
    // installation from a process-reported PID or a green Cloudflare session.
    let (first_owner, first_stage, next) = if !control_verified {
        (
            Owner::Control,
            "CONTROL_IMAGE",
            NextAction::InspectControllerUpdate,
        )
    } else if text_at(release_parity, "/state") != Some("MATCHED") {
        (
            Owner::Control,
            "RELEASE_PARITY",
            NextAction::InspectEngineProvenance,
        )
    } else if engine_running && !engine_proved {
        (
            Owner::Engine,
            "ENGINE_IMAGE",
            NextAction::InspectEngineProvenance,
        )
    } else if engine_desired_running && !engine_running {
        (
            Owner::Engine,
            "ENGINE_PROCESS",
            NextAction::InspectEngineLifecycle,
        )
    } else if !research_proved && !research_expected_stopped {
        (
            Owner::Research,
            "RESEARCH_PROCESS_OR_IMAGE",
            NextAction::InspectResearchStatus,
        )
    } else {
        (Owner::Edge, "EDGE_AUTHENTICATED_SESSION", NextAction::InspectEdgeRuntime)
    };

    serde_json::to_value(RuntimeObservation {
        schema: SCHEMA,
        reference: "runtime",
        read_only: true,
        quality: "partial",
        as_of_ms: jiff::Timestamp::now().as_millisecond(),
        first_unproven_owner: first_owner,
        first_unproven_stage: first_stage,
        next_action: next,
        owners: [control, engine, research_owner, edge],
    })
    .expect("static owner diagnostic projection serializes")
}

#[cfg(test)]
mod tests {
    use super::*;
    use okx_host_protocol::{ResearchCatalogMode, HOST_RESEARCH_RUNTIME_STATUS_SCHEMA_V1};
    use serde_json::json;

    fn launcher() -> Value {
        json!({"active":{"state":"READY","value":{"active":{
            "controller_sha256":"a".repeat(64),"source_tree":"b".repeat(40)
        }}},"entrypoint_is_launcher":true,"pending":{"state":"NONE"}})
    }
    fn agent() -> Value {
        json!({"state":"VERIFIED","binary_sha256":"c".repeat(64),
            "provenance":{"source_tree":"b".repeat(40)}})
    }
    fn research() -> ResearchRuntimeStatus {
        ResearchRuntimeStatus {
            schema: HOST_RESEARCH_RUNTIME_STATUS_SCHEMA_V1.into(),
            desired_running: true,
            process_generation: 7,
            pid: Some(4321),
            active_worker_sha256: Some("d".repeat(64)),
            installed_worker_sha256: Some("d".repeat(64)),
            phase: ResearchRuntimePhase::RunningUnverified,
            catalog_mode: ResearchCatalogMode::HostIndependent,
            retry_in_ms: None,
            last_error: None,
        }
    }
    fn observe(launcher: &Value, agent: &Value, research: Option<&ResearchRuntimeStatus>) -> Value {
        summarize(
            launcher,
            Some(&"a".repeat(64)),
            agent,
            true,
            true,
            &json!({"state":"MATCHED"}),
            research,
        )
    }

    #[test]
    fn combined_runtime_readback_keeps_owner_boundaries_and_edge_unverified() {
        let result = observe(&launcher(), &agent(), Some(&research()));
        assert_eq!(result["schema"], SCHEMA);
        assert_eq!(result["read_only"], true);
        assert_eq!(result["quality"], "partial");
        assert_eq!(result["first_unproven_owner"], "edge");
        assert_eq!(result["first_unproven_stage"], "EDGE_AUTHENTICATED_SESSION");
        assert_eq!(result["owners"].as_array().unwrap().len(), 4);
        assert_eq!(result["owners"][0]["state"], "OBSERVED");
        assert_eq!(result["owners"][1]["state"], "OBSERVED");
        assert_eq!(result["owners"][1]["image_basis"], "installed_engine_executable");
        assert_eq!(result["owners"][2]["process_generation"], 7);
        assert_eq!(result["owners"][2]["state"], "OBSERVED");
        assert_eq!(result["owners"][3]["state"], "NOT_EVALUATED");
        assert!(result["owners"][3].get("image_sha256").is_none());
        assert!(result.to_string().len() < 4096);
    }

    #[test]
    fn first_missing_fact_is_stable_and_failed_owners_are_not_hidden() {
        let mut control = launcher();
        control["active"]["value"]["active"]["controller_sha256"] = json!("f".repeat(64));
        let result = observe(&control, &agent(), Some(&research()));
        assert_eq!(result["first_unproven_stage"], "CONTROL_IMAGE");
        assert_eq!(result["owners"][0]["state"], "UNVERIFIED");

        let mut untrusted_agent = agent();
        untrusted_agent["state"] = json!("MISMATCH");
        untrusted_agent["binary_sha256"] = json!("SECRET_BEARER_SHOULD_NOT_ESCAPE");
        let result = observe(&launcher(), &untrusted_agent, Some(&research()));
        assert_eq!(result["first_unproven_stage"], "ENGINE_IMAGE");
        assert_eq!(result["owners"][1]["state"], "BLOCKED");
        assert!(!result.to_string().contains("SECRET_BEARER"));

        let mut bad_research = research();
        bad_research.phase = ResearchRuntimePhase::InvalidProvenance;
        let result = observe(&launcher(), &agent(), Some(&bad_research));
        assert_eq!(result["first_unproven_stage"], "RESEARCH_PROCESS_OR_IMAGE");
        assert_eq!(result["owners"][2]["state"], "BLOCKED");
        let mut invalid_sha = research();
        invalid_sha.active_worker_sha256 = Some("invalid".into());
        invalid_sha.installed_worker_sha256 = Some("invalid".into());
        let result = observe(&launcher(), &agent(), Some(&invalid_sha));
        assert_eq!(result["owners"][2]["state"], "UNVERIFIED");
        assert_eq!(result["first_unproven_owner"], "research");
        let result = observe(&launcher(), &agent(), None);
        assert_eq!(result["owners"][2]["reason_code"], "RESEARCH_STATUS_UNVERIFIED");
    }

    #[test]
    fn expected_stopped_owners_are_not_false_failures() {
        let mut stopped = research();
        stopped.desired_running = false;
        stopped.pid = None;
        stopped.active_worker_sha256 = None;
        stopped.phase = ResearchRuntimePhase::Stopped;
        let result = summarize(
            &launcher(),
            Some(&"a".repeat(64)),
            &agent(),
            false,
            false,
            &json!({"state":"MATCHED"}),
            Some(&stopped),
        );
        assert_eq!(result["owners"][1]["state"], "NOT_OBSERVED");
        assert_eq!(result["owners"][2]["state"], "NOT_OBSERVED");
        assert_eq!(result["first_unproven_stage"], "EDGE_AUTHENTICATED_SESSION");
    }
}
