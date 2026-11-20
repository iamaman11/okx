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
    let linked = attach_composition_source_evidence(original.clone(), sources.clone()).unwrap();
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
