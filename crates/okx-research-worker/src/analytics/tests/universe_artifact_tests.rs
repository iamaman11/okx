use super::*;

#[test]
fn global_rank_cursor_uses_persisted_pages_without_refetch() {
    let (root, service) = super::artifact_job_fixtures::service();
    let scores = (0..57)
        .map(|i| json!({"instrument":format!("A{i:03}"),"metrics":{"returns":57-i}}))
        .collect::<Vec<_>>();
    let first = service
        .artifacts
        .publish_evidence(&scores[..50].to_vec())
        .unwrap()
        .0;
    let second = service
        .artifacts
        .publish_evidence(&scores[50..].to_vec())
        .unwrap()
        .0;
    let manifest = service
        .artifacts
        .publish_evidence(&json!({"row_pages":[first,second],"total_rows":57}))
        .unwrap()
        .0;
    let result = json!({"result_artifact":manifest,"total_rows":57,"returned_rows":12,
            "page_limit":12,"rows":scores[..12],"next_cursor":"12",
            "universe_page":{"global_ranking":true,"has_more":true,"page_count":12}});
    let page = global_rank_page(
        &service.artifacts,
        result.clone(),
        "48",
        "f".repeat(64).as_str(),
    )
    .unwrap();
    assert_eq!(page["rows"][0]["instrument"], "A048");
    assert_eq!(page["returned_rows"], 9);
    assert_eq!(page["next_cursor"], Value::Null);
    assert_eq!(page["universe_page"]["has_more"], false);
    assert_eq!(
        global_rank_page(&service.artifacts, result, "57", "f".repeat(64).as_str()).unwrap_err(),
        "GLOBAL_RANK_CURSOR_OUT_OF_RANGE"
    );
    drop(service);
    std::fs::remove_dir_all(root).unwrap();
}
#[test]
fn ordinary_market_cursor_reads_exact_persisted_pages() {
    let (root, service) = super::artifact_job_fixtures::service();
    let scores = (0..105)
        .map(|i| json!({"instrument":format!("I{i:03}")}))
        .collect::<Vec<_>>();
    let mut pages = Vec::new();
    for chunk in scores.chunks(50) {
        pages.push(service.artifacts.publish_evidence(&chunk).unwrap().0);
    }
    let manifest = service
        .artifacts
        .publish_evidence(&json!({"row_pages":pages,"total_rows":105}))
        .unwrap()
        .0;
    let job = "a".repeat(64);
    let first = market_result_with_context(
        json!({"kind":"market","result_artifact":manifest,"rows":scores[..13],
                "total_rows":105,"returned_rows":13,"next_cursor":"13"}),
        &job,
        None,
    )
    .unwrap();
    assert_eq!(first["next_cursor"], format!("{job}:13"));
    assert_eq!(first["page_limit"], 13);
    let next = global_rank_page(&service.artifacts, first, "49", &job).unwrap();
    assert_eq!(next["rows"][0]["instrument"], "I049");
    assert_eq!(next["rows"][1]["instrument"], "I050");
    assert_eq!(next["returned_rows"], 13);
    assert_eq!(next["next_cursor"], format!("{job}:62"));
    assert!(next["universe_page"].is_null());
    assert_eq!(
        global_rank_page(&service.artifacts, next, "105", &job).unwrap_err(),
        "MARKET_CURSOR_OUT_OF_RANGE"
    );
    drop(service);
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn market_universe_metadata_persisted_with_final_artifact_and_queue() {
    let page = json!({
        "reference_generation":"generation-a",
        "next_after_instrument":"BTC-USDT-SWAP",
        "has_more":true,
        "global_ranking":false
    });
    let complete = market_result_with_context(
        json!({"kind":"market","rows":[],"total_rows":0}),
        "fixture-job",
        Some(&page),
    )
    .unwrap();
    assert_eq!(complete["universe_page"], page);
    assert_eq!(complete["job_id"], "fixture-job");
    let (root, service) = super::artifact_job_fixtures::service();
    let artifact = service.artifacts.publish_evidence(&complete).unwrap().0;
    let persisted: Value = service.artifacts.read_evidence_json(&artifact).unwrap();
    assert_eq!(
        persisted["universe_page"]["reference_generation"],
        "generation-a"
    );
    assert_eq!(
        persisted["universe_page"]["next_after_instrument"],
        "BTC-USDT-SWAP"
    );
    assert_eq!(
        with_universe_page(json!({"state":"queued"}), Some(&page)).unwrap()["universe_page"],
        page
    );
    drop(service);
    std::fs::remove_dir_all(root).unwrap();
}
