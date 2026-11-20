use super::*;

fn composed_request(bars: usize) -> MarketRequest {
    serde_json::from_value(json!({
        "action":"screen", "instruments":["BTC-USDT-SWAP"], "fields":["returns"],
        "timeframe_ms":60000, "start_ms":0, "end_ms":bars as u64 * 60000,
        "composition":{"version":"okx.market.composition/v1", "steps":[
            {"id":"price_change","op":"returns_series","input":"close"},
            {"id":"oi_change","op":"returns_series","input":"open_interest"},
            {"id":"price_after_funding","op":"subtract_series","input":"price_change","other":"funding_rate"},
            {"id":"combined","op":"add_series","input":"price_after_funding","other":"oi_change"}
        ], "result_steps":["combined"]}
    }))
    .unwrap()
}

fn composed_row(bars: usize) -> Loaded {
    Loaded {
        instrument: "BTC-USDT-SWAP".into(),
        bars: Ok(Arc::new(
            (0..bars)
                .map(|i| Bar {
                    time: i as u64 * 60000,
                    open: 100.0,
                    high: 101.0,
                    low: 99.0,
                    close: 100.0 + i as f64,
                    volume: 1.0,
                    funding_rate: None,
                })
                .collect(),
        )),
        ticker: None,
        funding: Some(Ok(vec![
            (60000, 0.001),
            (60001, 0.005),
            ((bars - 1) as u64 * 60000, -0.002),
        ])),
        oi: Some(Ok((0..bars)
            .map(|i| (i as u64 * 60000, 10.0 + i as f64))
            .collect())),
        excluded: None,
    }
}

#[test]
fn t06_t13_all_three_sources_are_immutable_chunked_and_reconstructable() {
    let (root, service) = super::super::artifact_job_fixtures::service();
    let count = MARKET_SOURCE_EVIDENCE_CHUNK + 1;
    let query = composed_request(count);
    let plan = planner::AnalysisPlan::compile_market(&query).unwrap();
    let cancellation = super::super::executor::Cancel::default();
    let inputs = composed_row(count);
    let sources = publish_composition_source_evidence(
        &service.artifacts,
        &inputs,
        &query,
        &plan,
        &cancellation,
    )
    .unwrap();
    assert_eq!(sources.len(), 3);
    let original = compute_row_planned(inputs, &query, None, &plan, true).unwrap();
    let mut linked = attach_composition_source_evidence(original.clone(), sources.clone()).unwrap();
    let integrity =
        verify_composition_source_pages(&service.artifacts, &linked, &query, &plan, &cancellation)
            .unwrap();
    assert_eq!(integrity["quality"], "verified_normalized_sources");
    assert_eq!(integrity["verified_sources"].as_array().unwrap().len(), 3);
    assert_eq!(integrity["unavailable_sources"], json!([]));
    assert_eq!(integrity["replay_metric_reexecution_verified"], false);
    assert_eq!(integrity["historical_publication_time_verified"], false);
    for source in integrity["verified_sources"].as_array().unwrap() {
        assert_eq!(source["quality"], "verified_normalized_capture");
        assert!(source["capture_hash"].is_string());
        assert_eq!(source["publication_time_verified"], false);
    }
    linked["source_integrity"] = integrity;
    let proof = replay_proof::verify_persisted_market_metrics(
        &service.artifacts, &linked, &query, &plan, true, &cancellation,
    )
    .unwrap();
    assert_eq!(proof["quality"], "verified_reexecution");
    assert_eq!(proof["observations_rehydrated"], 2 * count + 3);
    assert!(proof["semantic_row_hash"].is_string());
    let mut altered_metric = linked.clone();
    altered_metric["metrics"]["combined"] = json!(42.0);
    assert_eq!(
        replay_proof::verify_persisted_market_metrics(
            &service.artifacts, &altered_metric, &query, &plan, true, &cancellation,
        )
        .unwrap_err(),
        "COMPOSITION_SOURCE_REEXECUTION_MISMATCH"
    );
    let mut altered_diagnostics = linked.clone();
    altered_diagnostics["composition_quality"]["combined"]["status"] = json!("available_at_closed_bar");
    assert_eq!(
        replay_proof::verify_persisted_market_metrics(
            &service.artifacts, &altered_diagnostics, &query, &plan, true, &cancellation,
        )
        .unwrap_err(),
        "COMPOSITION_SOURCE_REEXECUTION_MISMATCH"
    );
    assert!(linked["metrics"]["combined"].is_number());
    assert_eq!(
        linked["composition_quality"]["combined"]["status"],
        "retrospective_publication_unverified"
    );
    for (source, hash, artifact) in &sources {
        assert_eq!(
            linked["source_captures"][source]["evidence_artifact"],
            *artifact
        );
        assert_eq!(linked["source_captures"][source]["capture_hash"], *hash);
        let manifest: Value = service.artifacts.read_evidence_json(artifact).unwrap();
        assert_eq!(manifest["schema"], "okx.market.normalized-source/v1");
        assert_eq!(manifest["instrument"], "BTC-USDT-SWAP");
        assert_eq!(manifest["source"], *source);
        assert_eq!(manifest["capture_hash"], *hash);
        assert_eq!(manifest["publication_time_verified"], false);
        let chunks = manifest["chunks"].as_array().unwrap();
        let count_expected = if *source == "funding_rate" { 3 } else { count };
        assert_eq!(manifest["observed_points"], count_expected);
        assert_eq!(
            chunks.len(),
            count_expected.div_ceil(MARKET_SOURCE_EVIDENCE_CHUNK)
        );
        if *source == "candles" {
            let mut reconstructed: Vec<Bar> = Vec::new();
            for chunk in chunks {
                reconstructed.extend(
                    service
                        .artifacts
                        .read_evidence_json::<Vec<Bar>>(chunk.as_str().unwrap())
                        .unwrap(),
                );
            }
            assert_eq!(reconstructed.len(), count);
            assert_eq!(
                reconstructed.last().unwrap().time,
                (count - 1) as u64 * 60000
            );
            let roundtrip = okx_research::canonical_sha256(&(
                manifest["hash_domain"].as_str().unwrap(),
                "BTC-USDT-SWAP",
                0u64,
                count as u64 * 60000,
                60000u64,
                &reconstructed,
            ))
            .unwrap();
            assert_eq!(roundtrip, *hash);
        } else {
            let mut reconstructed: Vec<(u64, f64)> = Vec::new();
            for chunk in chunks {
                reconstructed.extend(
                    service
                        .artifacts
                        .read_evidence_json::<Vec<(u64, f64)>>(chunk.as_str().unwrap())
                        .unwrap(),
                );
            }
            assert_eq!(reconstructed.len(), count_expected);
            let roundtrip = okx_research::canonical_sha256(&(
                manifest["hash_domain"].as_str().unwrap(),
                "BTC-USDT-SWAP",
                0u64,
                count as u64 * 60000,
                60000u64,
                &reconstructed,
            ))
            .unwrap();
            assert_eq!(roundtrip, *hash);
        }
    }
    let repeated = publish_composition_source_evidence(
        &service.artifacts,
        &composed_row(count),
        &query,
        &plan,
        &cancellation,
    )
    .unwrap();
    assert_eq!(
        sources, repeated,
        "same normalized inputs share immutable identities"
    );
    let mut changed = composed_row(count);
    changed
        .oi
        .as_mut()
        .unwrap()
        .as_mut()
        .unwrap()
        .last_mut()
        .unwrap()
        .1 += 1.0;
    let changed_sources = publish_composition_source_evidence(
        &service.artifacts,
        &changed,
        &query,
        &plan,
        &cancellation,
    )
    .unwrap();
    assert_eq!(sources[0], changed_sources[0]);
    assert_ne!(sources[1], changed_sources[1]);
    assert_eq!(sources[2], changed_sources[2]);
    assert_eq!(
        attach_composition_source_evidence(original, changed_sources).unwrap_err(),
        "COMPOSITION_SOURCE_CAPTURE_HASH_MISMATCH"
    );
    drop(service);
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn t13_market_source_integrity_rejects_rehashed_forged_metadata_or_reordered_pages() {
    let (root, service) = super::super::artifact_job_fixtures::service();
    let count = MARKET_SOURCE_EVIDENCE_CHUNK + 1;
    let query = composed_request(count);
    let plan = planner::AnalysisPlan::compile_market(&query).unwrap();
    let cancel = super::super::executor::Cancel::default();
    let input = composed_row(count);
    let base = compute_row_planned(input.clone(), &query, None, &plan, true).unwrap();
    let captures =
        publish_composition_source_evidence(&service.artifacts, &input, &query, &plan, &cancel)
            .unwrap();
    let linked = attach_composition_source_evidence(base, captures).unwrap();
    let candle_id = linked["source_captures"]["candles"]["evidence_artifact"]
        .as_str()
        .unwrap();
    let original: Value = service.artifacts.read_evidence_json(candle_id).unwrap();
    let changes = [
        ("forged_count", {
            let mut m = original.clone();
            m["observed_points"] = json!(count - 1);
            m
        }),
        ("wrong_range", {
            let mut m = original.clone();
            m["requested_end_ms"] = json!(99_999_999);
            m
        }),
        ("false_time_proof", {
            let mut m = original.clone();
            m["publication_time_verified"] = json!(true);
            m
        }),
        ("changed_hash_domain", {
            let mut m = original.clone();
            m["hash_domain"] = json!("other-domain");
            m
        }),
        ("reordered_pages", {
            let mut m = original.clone();
            m["chunks"].as_array_mut().unwrap().reverse();
            m
        }),
        ("duplicate_page", {
            let mut m = original.clone();
            let first = m["chunks"][0].clone();
            m["chunks"][1] = first;
            m
        }),
        ("missing_page", {
            let mut m = original.clone();
            m["chunks"].as_array_mut().unwrap().pop();
            m
        }),
    ];
    for (name, metadata) in changes {
        let id = service.artifacts.publish_evidence(&metadata).unwrap().0;
        let mut forged = linked.clone();
        forged["source_captures"]["candles"]["evidence_artifact"] = json!(id);
        assert_eq!(
            verify_composition_source_pages(&service.artifacts, &forged, &query, &plan, &cancel)
                .unwrap_err(),
            "COMPOSITION_SOURCE_EVIDENCE_MISMATCH",
            "forged {name} must not be treated as verified evidence"
        );
    }
    // Every individual artifact is content-addressed and the attacker can
    // publish a perfectly valid new page and manifest. Neither SHA alone
    // proves it is the page that actually fed the original market metric.
    let first_chunk = original["chunks"][0].as_str().unwrap();
    let mut altered: Vec<Bar> = service.artifacts.read_evidence_json(first_chunk).unwrap();
    altered[0].close += 10.0;
    let altered_chunk = service.artifacts.publish_evidence(&altered).unwrap().0;
    let mut altered_manifest = original.clone();
    altered_manifest["chunks"][0] = json!(altered_chunk);
    let altered_id = service.artifacts.publish_evidence(&altered_manifest).unwrap().0;
    let mut forged = linked.clone();
    forged["source_captures"]["candles"]["evidence_artifact"] = json!(altered_id);
    assert_eq!(
        verify_composition_source_pages(&service.artifacts, &forged, &query, &plan, &cancel)
            .unwrap_err(),
        "COMPOSITION_SOURCE_EVIDENCE_MISMATCH"
    );
    let mut mismatched_capture = linked;
    mismatched_capture["source_captures"]["open_interest"]["capture_hash"] = json!("sha256:forged");
    assert_eq!(
        verify_composition_source_pages(&service.artifacts, &mismatched_capture, &query, &plan, &cancel)
            .unwrap_err(),
        "COMPOSITION_SOURCE_EVIDENCE_MISMATCH"
    );
    drop(service);
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn t06_t13_flat_projection_sources_are_included_in_composed_reexecution() {
    let (root, service) = super::super::artifact_job_fixtures::service();
    let mut request = composed_request(8);
    request.fields = vec!["returns".into(), "oi_change".into(), "funding_sum".into()];
    request.composition = Some(serde_json::from_value(json!({
        "version":"okx.market.composition/v1",
        "steps":[{"id":"price_change","op":"returns_series","input":"close"}],
        "result_steps":["price_change"]
    })).unwrap());
    let plan = planner::AnalysisPlan::compile_market(&request).unwrap();
    let input = composed_row(8);
    let cancel = super::super::executor::Cancel::default();
    let computed = compute_row_planned(input.clone(), &request, None, &plan, true).unwrap();
    let evidence = publish_composition_source_evidence(
        &service.artifacts, &input, &request, &plan, &cancel,
    ).unwrap();
    assert_eq!(evidence.len(), 3, "flat sources must also be captured");
    let mut linked = attach_composition_source_evidence(computed, evidence).unwrap();
    linked["source_integrity"] = verify_composition_source_pages(
        &service.artifacts, &linked, &request, &plan, &cancel,
    ).unwrap();
    assert_eq!(linked["source_integrity"]["verified_sources"].as_array().unwrap().len(), 3);
    assert_eq!(
        replay_proof::verify_persisted_market_metrics(
            &service.artifacts, &linked, &request, &plan, true, &cancel,
        ).unwrap()["quality"],
        "verified_reexecution"
    );
    drop(service);
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn t10_t13_large_composition_proves_sources_without_unbounded_reexecution() {
    let (root, service) = super::super::artifact_job_fixtures::service();
    let count = 32_767;
    let query = composed_request(count);
    let plan = planner::AnalysisPlan::compile_market(&query).unwrap();
    let cancel = super::super::executor::Cancel::default();
    let input = composed_row(count);
    let computed = compute_row_planned(input.clone(), &query, None, &plan, true).unwrap();
    let evidence = publish_composition_source_evidence(
        &service.artifacts, &input, &query, &plan, &cancel,
    ).unwrap();
    let mut linked = attach_composition_source_evidence(computed, evidence).unwrap();
    linked["source_integrity"] = verify_composition_source_pages(
        &service.artifacts, &linked, &query, &plan, &cancel,
    ).unwrap();
    assert_eq!(linked["source_integrity"]["quality"], "verified_normalized_sources");
    let proof = replay_proof::verify_persisted_market_metrics(
        &service.artifacts, &linked, &query, &plan, true, &cancel,
    ).unwrap();
    assert_eq!(proof["quality"], "not_verified");
    assert_eq!(proof["reason"], "REEXECUTION_OBSERVATION_BUDGET_EXCEEDED");
    assert_eq!(proof["max_observations"], 65_536);
    drop(service);
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn t06_missing_sources_have_no_false_capture_and_cancel_does_not_publish() {
    let (root, service) = super::super::artifact_job_fixtures::service();
    let query = composed_request(100);
    let plan = planner::AnalysisPlan::compile_market(&query).unwrap();
    let cancel = super::super::executor::Cancel::default();
    let mut incomplete = composed_row(100);
    incomplete.oi = Some(Err("OI_SOURCE_UNAVAILABLE".into()));
    incomplete.funding = None;
    let evidence = publish_composition_source_evidence(
        &service.artifacts,
        &incomplete,
        &query,
        &plan,
        &cancel,
    )
    .unwrap();
    assert_eq!(evidence.len(), 1);
    assert_eq!(evidence[0].0, "candles");
    let result = compute_row_planned(incomplete, &query, None, &plan, true).unwrap();
    let linked = attach_composition_source_evidence(result, evidence).unwrap();
    assert_eq!(linked["source_captures"]["open_interest"], Value::Null);
    assert_eq!(linked["source_captures"]["funding_rate"], Value::Null);
    assert!(linked["metrics"]["combined"].is_null());
    let integrity =
        verify_composition_source_pages(&service.artifacts, &linked, &query, &plan, &cancel)
            .unwrap();
    assert_eq!(integrity["quality"], "partial_source_unavailable");
    assert_eq!(
        integrity["unavailable_sources"],
        json!(["open_interest", "funding_rate"])
    );
    assert_eq!(integrity["verified_sources"].as_array().unwrap().len(), 1);
    let mut source_limited = linked.clone();
    source_limited["source_integrity"] = integrity;
    assert_eq!(
        replay_proof::verify_persisted_market_metrics(
            &service.artifacts, &source_limited, &query, &plan, true, &cancel,
        )
        .unwrap()["reason"],
        "SOURCE_UNAVAILABLE"
    );
    let mut missing_bars = composed_row(100);
    missing_bars.bars = Err("CANDLES_UNAVAILABLE".into());
    let evidence = publish_composition_source_evidence(
        &service.artifacts,
        &missing_bars,
        &query,
        &plan,
        &cancel,
    )
    .unwrap();
    assert_eq!(evidence.len(), 2);
    let result = compute_row_planned(missing_bars, &query, None, &plan, true).unwrap();
    assert_eq!(result["source_captures"]["candles"], Value::Null);
    attach_composition_source_evidence(result, evidence).unwrap();
    cancel.store(true, Ordering::Release);
    assert_eq!(
        publish_composition_source_evidence(
            &service.artifacts,
            &composed_row(100),
            &query,
            &plan,
            &cancel
        )
        .unwrap_err(),
        "CANCELLED"
    );
    drop(service);
    std::fs::remove_dir_all(root).unwrap();
}
