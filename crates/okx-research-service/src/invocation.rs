//! Per-command correlation only; durable job identity remains with Jobs.
use okx_ipc_contracts::research_ipc::InvocationId;
use std::future::Future;

tokio::task_local! {
    static INVOCATION: Option<InvocationId>;
}

pub async fn scope<F: Future>(invocation: Option<InvocationId>, future: F) -> F::Output {
    INVOCATION.scope(invocation, future).await
}

pub fn current() -> Option<InvocationId> {
    INVOCATION.try_with(Clone::clone).ok().flatten()
}
