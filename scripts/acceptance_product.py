#!/usr/bin/env python3
"""Collect executable #398 evidence; never promotes absent physical evidence to PASS."""
import argparse,datetime,hashlib,json,pathlib,re,subprocess,sys
ROOT=pathlib.Path(__file__).resolve().parents[1]
REQUIRED={
    "CLEAN-01":{"clean_initialization_deletes_only_old_financial_keys","accounts::tests::initialize_without_any_legacy_files","accounts::tests::legacy_stop_claims_and_epochs_are_never_read","account_store::tests::corrupt_or_missing_db_never_reinitialized"},
    "ACCESS-01":{"accounts::tests::access_survives_restart","access_restart_and_account_registration"},
    "EXEC-01":{"trade_service::execute::tests::duplicate_execute_one_post","trade_service::execute::tests::lost_response_after_post_returns_unknown","trade_service::execute::tests::restart_posting_reconciles_without_resubmit"},
    "RISK-01":{"risk_concurrent_accounts_campaign_limit"},
    "FIN-01":{"account_store::tests::fee_fill_bill_counted_once","account_store::tests::sql_ingest_retry_exactly_once","finance::attribution::tests::funding_allocation_sums_to_account","finance::nav::tests::deposit_is_not_profit"},
    "MULTI-01":{"trade_service::execute::tests::two_accounts_two_instances","finance::attribution::tests::two_instances_same_instrument_lot_attribution"},
    "ANALYTICS-01":{"strategy_registry::tests::ema_cross_next_bar_no_lookahead","operators::tests::correlation_timestamp_alignment","strategy_registry::tests::test_partition_not_used_for_selection"},
    "OPS-01":{"analytics::jobs::tests::restart_marks_job_interrupted","analytics::executor::tests::cancel_releases_queue_capacity","account_store::tests::disk_full_before_post"},
    "F01":{"account_governance_lower_campaign_limit_monotonic"},
    "F02":{"trade_service::plan::tests::auto_fok_buy_rounds_inside_slippage","trade_service::plan::tests::auto_fok_sell_rounds_inside_slippage"},
    "F03":{"mcp_nested_failure_preserves_reason_code","mcp_queued_job_remains_accepted","native_completed_business_envelope_once"},
    "F04":{"analytics::jobs::tests::transient_rate_defer_is_requeued_instead_of_cached_as_failed"},
    "F05":{"analytics::tests::grid_result_includes_selected_train_test_and_equity"},
    "F06":{"portfolio::tests::portfolio_before_t0_is_partial"},
    "F07":{"account_store::tests::strategy_crash_pending_decision_to_intent","account_store::tests::strategy_policy_retry_keeps_immutable_lineage_and_one_intent"},
    "F08":{"trade_service::strategies::budget_contract_tests::strategy_risk_budget_not_notional","analytics::jobs::tests::job_status_reads_bounded_rows_at_100k_jobs"},
}

# Required user workflows, not a count of public tools. Each capability is
# PRESERVED only with its strict MCP action AND separate positive/negative
# physical evidence. No legacy alias, stub output, or default retirement.
CAPABILITY_ACTIONS={
    "runtime_status":("diagnostics.runtime",),
    "account_control":("accounts.list","accounts.status","accounts.connect","accounts.pause","accounts.resume","accounts.set_limits"),
    "find_instruments":("market.instruments",),
    "market_overview":("market.snapshot",),
    "market_intelligence":("market.microstructure",),
    "market_research":("market.compare",),
    "query_capabilities":("market.capabilities",),
    "query":("market.screen",),
    "research_capabilities":("research.capabilities",),
    "research":("research.backtest","research.grid","research.status","research.result","research.cancel","research.validate"),
    "account_summary":("portfolio.summary","portfolio.history","diagnostics.explain"),
    "execution_status":("trade.status","diagnostics.explain"),
    "portfolio_risk":("portfolio.scenario",),
    "trading_capabilities":("trade.capabilities",),
    "executor_preflight":("trade.preflight",),
    "execution_action":("trade.preview","trade.execute","trade.status","trade.cancel","trade.close"),
}

# Only mock mutation scenarios may be evidenced through an actual fake venue.
# This never promotes a fixture to physical Real trading acceptance.
FAKE_VENUE_ACTIONS={
    "accounts.connect","accounts.pause","accounts.resume","accounts.set_limits",
    "strategies.create","strategies.update","strategies.start","strategies.stop",
    "trade.execute","trade.cancel","trade.close",
}
# Per-action Rust business owner; no green unrelated test may certify a route.
# Unmapped fake mutations remain blocked until an actual native test exists.
FAKE_NATIVE_OWNERS={
    "strategies.create":frozenset({"account_store::tests::strategy_create_sql_persists_receipt_replay_and_rejects_conflicts"}),
    "strategies.update":frozenset({"account_store::tests::immutable_strategy_versions_bind_old_intent_and_cas_start_stop"}),
    "strategies.start":frozenset({"account_store::tests::immutable_strategy_versions_bind_old_intent_and_cas_start_stop"}),
    "strategies.stop":frozenset({"account_store::tests::immutable_strategy_versions_bind_old_intent_and_cas_start_stop"}),
    "trade.execute":frozenset({"trade_service::execute::tests::duplicate_execute_one_post",
                                  "trade_service::execute::tests::lost_response_after_post_returns_unknown"}),
    "trade.cancel":frozenset({"account_store::tests::duplicate_cancel_never_posts_twice"}),
    "trade.close":frozenset({"trade_service::execute::tests::close_under_stop"}),
}
FAKE_WORKER_ROUTE_TEST="mcp_fake_venue_mutation_routes"
# Schema-only negative proofs are allowed exclusively for no-argument read-only
# actions. Every other route must still demonstrate a native business refusal.
SCHEMA_ONLY_NEGATIVE_ACTIONS=frozenset({
    "diagnostics.runtime","accounts.list","market.capabilities","research.capabilities",
})
def checked_schema_negative(route,arguments,response,proof,executed_tests,tool_contract):
    """Require observed pre-dispatch schema error, exact strict contract, and Worker CI."""
    if (route not in SCHEMA_ONLY_NEGATIVE_ACTIONS
        or proof.get("rejection_stage")!="tool_schema"
        or proof.get("transport")!="live_mcp"
        or proof.get("worker_test_name")!="strict_action_fixtures"
        or "strict_action_fixtures" not in executed_tests):
        return False
    tool_name,action=route.split(".",1)
    if arguments!={"action":action,"raw_rpc":"forbidden"}:
        return False
    if not isinstance(response,dict) or set(response)!={"tool_error"}:
        return False  # In particular, no fabricated native structuredContent.
    variants=next((tool["inputSchema"].get("oneOf",[]) for tool in tool_contract["tools"]
                   if tool.get("name")==tool_name),[])
    selected=[variant for variant in variants
              if variant.get("properties",{}).get("action",{}).get("const")==action]
    if (len(selected)!=1 or set(selected[0].get("properties",{}))!={"action"}
        or selected[0].get("required")!=["action"]
        or selected[0].get("additionalProperties") is not False):
        return False
    error=response["tool_error"]
    return (isinstance(error,str)
        and len(error)<=65536
        and "not valid under any of the given schemas" in error
        and "Failed validating 'oneOf' in schema:" in error
        and "'additionalProperties': False" in error
        and "On instance:" in error
        and repr(arguments) in error)

def admissible_transport(route,proof,executed_tests):
    transport=proof.get("transport")
    if transport=="live_mcp":
        # Real live mutation POST is never acceptance evidence. Even a genuine
        # read-only account is not permission to exercise a live mutation.
        return route not in FAKE_VENUE_ACTIONS
    if transport!="fake_venue_fixture" or route not in FAKE_VENUE_ACTIONS:
        return False
    if proof.get("venue")!="fake" or type(proof.get("real_exchange_post_count")) is not int or proof["real_exchange_post_count"]!=0:
        return False
    native,worker=proof.get("native_test_name"),proof.get("worker_test_name")
    return (isinstance(native,str) and isinstance(worker,str)
        and native in FAKE_NATIVE_OWNERS.get(route,())
        and worker==FAKE_WORKER_ROUTE_TEST
        and native in executed_tests and worker in executed_tests)

def mutation_business_result(route,request,data):
    """A fake venue success must prove route-specific business state, not only
    the presence of an MCP completed wrapper. Never certify a simulated POST
    as a physical Real trade or a central risk reservation."""
    if not isinstance(data,dict):
        return False
    if route=="accounts.connect":
        account=data.get("account")
        config=data.get("configuration")
        return (data.get("status")=="CONNECTED"
            and isinstance(account,dict) and account.get("id")==request.get("alias")
            and account.get("identity_verified") is True
            and account.get("enabled") is request.get("enabled")
            and account.get("venue")=="okx"
            and account.get("environment") in ("real","demo")
            and isinstance(account.get("account_key"),str)
            and account["account_key"].startswith("okx:"+account["environment"]+":")
            and isinstance(config,dict) and isinstance(config.get("accounts"),list)
            and any(isinstance(row,dict) and row==account for row in config["accounts"]))
    if route in ("accounts.pause","accounts.resume","accounts.set_limits"):
        config=data.get("configuration")
        if not isinstance(config,dict) or not isinstance(config.get("revision"),int) or not isinstance(config.get("accounts"),list):
            return False
        target=next((row for row in config["accounts"] if isinstance(row,dict)
                     and request.get("account") in (row.get("id"),row.get("account_key"))),None)
        if target is None or data.get("status")!="CONFIGURED":
            return False
        if route=="accounts.pause":
            return target.get("explicit_pause") is True
        if route=="accounts.resume":
            return target.get("explicit_pause") is False
        return isinstance(request.get("limits"),dict) and target.get("limits")==request["limits"]
    if route.startswith("strategies."):
        instance=data.get("instance")
        if not isinstance(instance,dict) or instance.get("instance_id")!=request.get("instance_id") or not isinstance(instance.get("policy_revision"),int):
            return False
        if route=="strategies.create":
            return (instance.get("definition_id")==request.get("definition_id")
                and instance.get("enabled") is False and instance["policy_revision"]==1)
        if route=="strategies.start":
            return instance.get("enabled") is True
        if route=="strategies.stop":
            return instance.get("enabled") is False
        return route=="strategies.update" and isinstance(instance.get("parameters"),str)
    if route in ("trade.execute","trade.cancel","trade.close"):
        intent=data.get("intent")
        # Native responses after verified readback expose the persisted intent;
        # only an acknowledged/filled or cancelled identity is positive.
        if not isinstance(intent,dict) or not isinstance(intent.get("intent_id"),str) or not intent["intent_id"]:
            return False
        state=intent.get("state")
        if route=="trade.cancel":
            return state=="CANCELLED"
        return state in ("ACKNOWLEDGED","PARTIALLY_FILLED","FILLED") and isinstance(intent.get("plan"),dict)
    return False

def checked_capability_outcome(route, request, envelope, polarity):
    """Check observed business values; self-declared PASS cannot substitute for them."""
    if not isinstance(envelope,dict):
        return False
    status=envelope.get("status")
    if polarity=="negative":
        return (status in ("rejected","failed","unknown")
            and isinstance(envelope.get("reason_code"),str)
            and envelope["reason_code"] not in ("", "REQUEST_FAILED")
            and isinstance(envelope.get("next_action"),str)
            and bool(envelope["next_action"]))
    if status=="accepted" and route in ("research.backtest","research.grid"):
        data=envelope.get("data")
        job_id=envelope.get("job_id")
        return (envelope.get("reason_code") is None
            and isinstance(data,dict)
            and isinstance(job_id,str) and re.fullmatch(r"[0-9a-f]{64}",job_id)
            and data.get("id")==job_id
            and data.get("state") in ("queued","running")
            and data.get("error") is None)
    if status!="completed" or envelope.get("reason_code") is not None:
        return False
    data=envelope.get("data")
    if not isinstance(data,dict) or data.get("failure") is not None:
        return False
    if envelope.get("quality") in ("NOT_READY","DEGRADED","partial","stale"):
        return False
    if route in FAKE_VENUE_ACTIONS:
        return mutation_business_result(route,request,data)
    # A generic completed envelope is insufficient proof of the live market
    # snapshot: #416 fixed an unbounded full order-book projection.
    if route=="market.snapshot":
        market=data.get("market")
        if not (data.get("schema")=="okx.market.overview/v1"
                and data.get("source_mode")=="websocket"
                and isinstance(market,dict)
                and isinstance(market.get("ticker"),dict)
                and isinstance(data.get("book_sequence"),int)
                and data["book_sequence"]>0
                and "order_book" not in data
                and "order_book" not in market
                and len(json.dumps(data,ensure_ascii=False).encode("utf-8"))<12288):
            return False
    if route=="market.microstructure":
        bids,asks=data.get("bids"),data.get("asks")
        if not (data.get("schema")=="okx.market.microstructure/v1"
                and isinstance(data.get("book_sequence"),int) and data["book_sequence"]>0
                and isinstance(bids,list) and 0<len(bids)<=10
                and isinstance(asks,list) and 0<len(asks)<=10
                and all(isinstance(level,dict) and level.get("price") and level.get("size")
                        for level in bids+asks)
                and data.get("quality")=="complete"):
            return False
    if route=="market.screen":
        if not isinstance(data.get("rows"),list) or not isinstance(data.get("total_rows"),int):
            return False
        if "universe" in request:
            page=data.get("universe_page")
            if not isinstance(request["universe"],dict):return False
            requested_global=request["universe"].get("rank_globally") is True
            if not (isinstance(page,dict)
                and page.get("global_ranking") is requested_global
                and isinstance(page.get("reference_generation"),str)
                and bool(page["reference_generation"])
                and isinstance(page.get("has_more"),bool)):
                return False
            if requested_global:
                job_id=data.get("job_id")
                cursor=data.get("next_cursor")
                if not (page.get("scope")=="complete_venue_universe"
                    and isinstance(page.get("universe_size"),int)
                    and 0<page["universe_size"]<=512
                    and data["total_rows"]==page["universe_size"]
                    and isinstance(job_id,str) and re.fullmatch(r"[0-9a-f]{64}",job_id)
                    and isinstance(data.get("result_artifact"),str)
                    and re.fullmatch(r"sha256:[0-9a-f]{64}",data["result_artifact"])
                    and isinstance(data.get("rank_request_fingerprint"),str)
                    and re.fullmatch(r"sha256:[0-9a-f]{64}",data["rank_request_fingerprint"])
                    and ((isinstance(cursor,str) and cursor.startswith(job_id+":"))
                         if page["has_more"] else cursor is None)
                    and len(data["rows"])<=request.get("limit",50)
                    and all(isinstance(row,dict) and row.get("quality")=="complete"
                            and row.get("gaps")==[] and isinstance(row.get("metrics"),dict)
                            for row in data["rows"])):
                    return False
            elif page["has_more"] and not isinstance(page.get("next_after_instrument"),str):
                return False
    elif route=="portfolio.scenario":
        if not (data.get("schema")=="okx.portfolio.scenario/v1"
                and data.get("exchange_post") is False
                and data.get("execution")=="virtual_read_only"
                and isinstance(data.get("net_stress_pnl_usd"),str)
                and isinstance(data.get("venue_margin_verified"),bool)):
            return False
    elif route=="diagnostics.runtime":
        attestation=data.get("release_attestation")
        if not isinstance(attestation,dict) or attestation.get("verified") is not True:
            return False
    return True

def capability_matrix(physical=None,executed_tests=None):
    physical=physical or {}
    executed_tests=executed_tests or set()
    schema=json.loads((ROOT/"cloudflare/okx-mcp/contracts/tools.schema.json").read_text(encoding="utf-8-sig"))
    exposed=set()
    for tool in schema["tools"]:
        for variant in tool["inputSchema"].get("oneOf",[]):
            selector=variant.get("properties",{}).get("action",{})
            actions=[selector["const"]] if isinstance(selector.get("const"),str) else selector.get("enum",[])
            exposed.update(f"{tool['name']}.{a}" for a in actions if isinstance(a,str))
    claims=physical.get("capabilities",{})
    rows=[]
    for legacy,routes in CAPABILITY_ACTIONS.items():
        claim=claims.get(legacy,{}) if isinstance(claims,dict) else {}
        missing=[route for route in routes if route not in exposed]
        pos=claim.get("positive_evidence",[])
        neg=claim.get("negative_evidence",[])
        release=physical.get("release_hashes",{})
        expected_tree=release.get("source_tree")
        installed_tree=release.get("installed_agent_tree")
        worker_tree=release.get("worker_tree")
        # A green CI and a file's existence cannot certify an installed
        # business scenario. Require exact release provenance and an observed
        # native MCP request/response for *every* required action and polarity.
        def checked_evidence(paths,polarity):
            if not (isinstance(paths,list) and paths and
                    all(isinstance(p,str) and p.startswith("docs/acceptance/398/")
                        and p.endswith(".json") and ".." not in pathlib.PurePosixPath(p).parts
                        and (ROOT/p).is_file() for p in paths)):
                return False
            observed=set()
            for p in paths:
                try: proof=json.loads((ROOT/p).read_text(encoding="utf-8-sig"))
                except (OSError,ValueError):return False
                route=proof.get("action")
                request=proof.get("request")
                response=proof.get("response")
                if not (isinstance(request,dict) and isinstance(response,dict)
                        and isinstance(request.get("arguments"),dict)):
                    return False
                if (proof.get("schema")!="okx.acceptance.capability-evidence/v1"
                    or proof.get("legacy_tool")!=legacy
                    or proof.get("polarity")!=polarity
                    or proof.get("result")!="PASS"
                    or route not in routes
                    or route!=request.get("tool")+"."+str(request["arguments"].get("action"))
                    or proof.get("source_tree")!=expected_tree
                    or proof.get("installed_agent_tree")!=installed_tree
                    or proof.get("worker_tree")!=worker_tree
                    or not isinstance(proof.get("observed_at_utc"),str)
                    or not proof.get("observed_at_utc")
                    or not admissible_transport(route,proof,executed_tests)):
                    return False
                if polarity=="negative" and proof.get("rejection_stage")=="tool_schema":
                    if not checked_schema_negative(
                        route,request["arguments"],response,proof,executed_tests,schema):
                        return False
                    observed.add(route)
                    continue
                envelope=response.get("structuredContent")
                if not isinstance(envelope,dict):
                    return False
                if not checked_capability_outcome(route,request["arguments"],envelope,polarity):
                    return False
                observed.add(route)
            return observed==set(routes)
        provenance=(isinstance(expected_tree,str) and len(expected_tree)==40
            and installed_tree==expected_tree and worker_tree==expected_tree
            and physical.get("INSTALLED_MATCHED",{}).get("result")=="PASS"
            and physical.get("mcp",{}).get("physical_network")=="PASS")
        proven=(not missing and provenance and claim.get("result")=="PASS"
            and isinstance(claim.get("code_owner"),str) and bool(claim["code_owner"])
            and isinstance(claim.get("positive_assertion"),str) and bool(claim["positive_assertion"])
            and isinstance(claim.get("negative_assertion"),str) and bool(claim["negative_assertion"])
            and checked_evidence(pos,"positive") and checked_evidence(neg,"negative"))
        rows.append({"legacy_tool":legacy,"required_actions":list(routes),
            "missing_actions":missing,"status":"PRESERVED" if proven else "GAP",
            "reason":None if proven else ("ACTION_NOT_EXPOSED" if missing else "PHYSICAL_EVIDENCE_NOT_ACCEPTED"),
            "code_owner":claim.get("code_owner"),"positive_assertion":claim.get("positive_assertion"),
            "negative_assertion":claim.get("negative_assertion"),
            "positive_evidence":pos,"negative_evidence":neg})
    return rows
FINAL=["CODE_PASSED","INSTALLED_MATCHED","CAPABILITY_PARITY","ANALYTICS_READY","PHYSICAL_MULTI_ACCOUNT","REAL_CYCLE_ACCEPTED"]
STATES={"PASS","FAIL","BLOCKED","NOT_RUN"}
def executed_tests(log):
    rust=[(m.group(1),m.group(2))for m in re.finditer(r"^test ([\w:]+) \.\.\. (ok|FAILED|ignored)\s*$",log,re.M)]
    worker=[(m.group(1),"ok" if m.group(2)=="PASS" else "FAILED")for m in re.finditer(r"^([a-z][a-z0-9_]*) (PASS|FAIL)\s*$",log,re.M)]
    return rust+worker
def validate(value):
    if not value.get("test_counts",{}).get("executed",0):raise ValueError("zero executed tests are not acceptance evidence")
    counts=value.get("test_counts",{})
    parsed=counts.get("parsed_executed")
    if parsed is not None and parsed!=counts.get("executed"):raise ValueError("test count does not match parsed logs")
    if counts.get("failed",0):raise ValueError("failed executed test")
    cases=value.get("tests",[])
    seen={}
    for case in cases:
        if not all(case.get(k)for k in ("requirement_id","test_name","result","evidence")):raise ValueError("incomplete test record")
        if case["result"]not in STATES:raise ValueError("invalid result")
        key=(case["requirement_id"],case["test_name"])
        if key in seen:raise ValueError("duplicate test record "+case["requirement_id"]+":"+case["test_name"])
        seen[key]=case
        if case["result"]=="PASS"and not any((ROOT/p).is_file()for p in case["evidence"]):raise ValueError("PASS requires an existing executable evidence file")
    for requirement,names in REQUIRED.items():
        missing=[name for name in names if (requirement,name)not in seen]
        if missing:raise ValueError("missing mandatory test "+requirement+":"+missing[0])
        failed=[name for name in names if seen[(requirement,name)]["result"]!="PASS"]
        if failed:raise ValueError("mandatory test did not pass "+requirement+":"+failed[0])
    for key in FINAL:
        if value.get(key)not in STATES:raise ValueError("missing final field "+key)
    rows=value.get("capabilities")
    if not isinstance(rows,list) or len(rows)!=len(CAPABILITY_ACTIONS) or {
        item.get("legacy_tool") for item in rows}!=set(CAPABILITY_ACTIONS):
        raise ValueError("incomplete 16-to-7 capability coverage")
    if value["CAPABILITY_PARITY"]=="PASS" and any(
        item.get("status")!="PRESERVED" or item.get("missing_actions")
        for item in rows):
        raise ValueError("CAPABILITY_PARITY cannot pass with any unproven legacy workflow")
    if value["CODE_PASSED"]=="PASS"and(any(c["result"]!= "PASS"for c in cases)or any(c["exit_code"]!=0 for c in value["commands"])):raise ValueError("CODE_PASSED conflicts with unfinished checks")
    for key in FINAL[1:]:
        if value[key]=="PASS":
            paths=value.get("physical",{}).get(key,{}).get("evidence",[])
            if not paths or not all((ROOT/p).is_file() for p in paths):raise ValueError(key+" requires existing physical evidence files")
    return value

def command(cmd,cwd,output):
    with output.open("w",encoding="utf-8") as log:
        completed=subprocess.run(cmd,cwd=cwd,stdout=log,stderr=subprocess.STDOUT)
    return {"command":cmd,"exit_code":completed.returncode,"evidence":str(output.relative_to(ROOT)).replace("\\","/")}
def build(args):
    out=ROOT/args.output;out.parent.mkdir(parents=True,exist_ok=True);evidence=out.parent/"verification";evidence.mkdir(exist_ok=True)
    commands=[];logs=[]
    if args.run:
        for name,cmd,cwd in [("rust-format",["cargo","fmt","--all","--","--check"],ROOT),("architecture",[sys.executable,"scripts/check_architecture.py"],ROOT),("sql-contract",[sys.executable,"scripts/check_ledger_sql.py"],ROOT),("product-contract",[sys.executable,"scripts/check_product_contract.py"],ROOT),("rust-tests",["cargo","test","--locked","--workspace","--all-targets","--","--nocapture"],ROOT),("rust-clippy",["cargo","clippy","--locked","--workspace","--all-targets","--","-D","warnings"],ROOT),("worker",["cmd","/c","npm","run","check"]if sys.platform=="win32"else["npm","run","check"],ROOT/"cloudflare/okx-mcp")]:
            result=command(cmd,cwd,evidence/(name+".log"));result["check"]=name;commands.append(result)
            if name in {"rust-tests","worker"}:logs.append((result["evidence"],(ROOT/result["evidence"]).read_text(encoding="utf-8")))
    for path in args.code_log:
        p=ROOT/path;logs.append((str(p.relative_to(ROOT)).replace("\\","/"),p.read_text(encoding="utf-8")));commands.append({"command":["import",path],"exit_code":0 if "test result: FAILED"not in logs[-1][1]else 1,"evidence":logs[-1][0]})
    tests=[];all_tests=[]
    for path,log in logs:
        for name,status in executed_tests(log):
            if status!="ignored":all_tests.append((name,status))
            mapped=False
            for requirement,tokens in REQUIRED.items():
                if status!="ignored"and name in tokens:
                    mapped=True;tests.append({"requirement_id":requirement,"test_name":name,"result":"PASS"if status=="ok"else"FAIL","evidence":[path]})
            if status!="ignored"and not mapped:tests.append({"requirement_id":"REGRESSION","test_name":name,"result":"PASS"if status=="ok"else"FAIL","evidence":[path]})
    for requirement,names in REQUIRED.items():
        observed={(t["requirement_id"],t["test_name"])for t in tests}
        for name in names:
            if (requirement,name)not in observed:tests.append({"requirement_id":requirement,"test_name":name,"result":"NOT_RUN","evidence":["acceptance runner: no matching executed test"]})
    perf_path=ROOT/args.performance;perf=json.loads(perf_path.read_text(encoding="utf-8-sig"))if perf_path.exists()else None
    if perf is not None:
        for _,log in logs:
            for match in re.finditer(r"ANALYTICS_PERF_JSON:(\{[^\n]+\})",log):
                for key,record in json.loads(match.group(1)).items():
                    if perf.get(key,{}).get("build")!="release"or record.get("build")=="release":perf[key]=record
    perf_ok=bool(perf and perf.get("ema_90d_15m",{}).get("measurements",0)>=30 and perf["ema_90d_15m"]["p95_ms"]<=500 and perf.get("grid_100variants_100k",{}).get("incremental_peak_bytes",2**63)<=512*1024**2 and perf.get("screen_8x100",{}).get("measurements",0)>=30 and perf["screen_8x100"]["p95_ms"]<=1000 and perf["screen_8x100"]["summary_bytes"]<=8192 and perf["screen_8x100"].get("build")=="release" and perf.get("two_heavy_stop",{}).get("measurements",0)>=30 and perf["two_heavy_stop"]["p95_ms"]<=100 and perf["two_heavy_stop"]["cancel_max_ms"]<=1000 and perf["two_heavy_stop"].get("build")=="release")

    physical=json.loads((ROOT/args.physical).read_text(encoding="utf-8"))if args.physical else {}
    mandatory_checks={"rust-format","architecture","sql-contract","product-contract","rust-tests","rust-clippy","worker"}
    code_ok=bool(all_tests and mandatory_checks.issubset({c.get("check") for c in commands})and all(c["exit_code"]==0 for c in commands)and all(t["result"]=="PASS"for t in tests))
    summaries=[tuple(map(int,m.groups()))for _,log in logs for m in re.finditer(r"test result: (?:ok|FAILED)\. (\d+) passed; (\d+) failed; (\d+) ignored;",log)]
    passed=sum(p for p,_,_ in summaries)+sum(1 for name,status in all_tests if "::"not in name and name in set().union(*REQUIRED.values()) and status=="ok");failed=sum(f for _,f,_ in summaries)
    head=subprocess.check_output(["git","rev-parse","HEAD^{tree}"],cwd=ROOT,text=True).strip();diff=subprocess.check_output(["git","diff","--binary","HEAD"],cwd=ROOT)
    value={"schema":"okx.acceptance/398.1","created_at":datetime.datetime.now(datetime.timezone.utc).isoformat(),"source_tree":head,"working_diff_sha256":hashlib.sha256(diff).hexdigest(),"release_hashes":{},"test_counts":{"executed":passed+failed,"parsed_executed":passed+failed,"passed":passed,"failed":failed},"commands":commands,"tests":tests,"performance":{"result":"PASS"if perf_ok else"NOT_RUN","scope":"prepared native compute fixtures; physical MCP/network evidence is separate","evidence":args.performance,"measurements":perf},"physical":physical,"CODE_PASSED":"PASS"if code_ok else"FAIL","ANALYTICS_READY":physical.get("ANALYTICS_READY",{}).get("result","NOT_RUN") if perf_ok else "NOT_RUN"}
    sql_evidence=ROOT/"docs/verification/398-sql-million.json"
    if sql_evidence.exists():
        sql=json.loads(sql_evidence.read_text(encoding="utf-8-sig"));value["sqlite"]={"version":sql.get("sqlite_version"),"source_id":sql.get("sqlite_source_id"),"evidence":"docs/verification/398-sql-million.json","scope":"executed Linux bundled SQLite benchmark; installed binary checked separately"}
    else:value["sqlite"]={"result":"NOT_RUN"}
    worker_log=next((log for path,log in logs if path.endswith("worker.log")),"")
    inventory=re.search(r"tools_list_actual_serialized PASS (\d+) tools (\d+) bytes",worker_log)
    value["mcp"]={"physical_network":"NOT_RUN","account_count":None,"instance_count":None,"reason":"installed MCP observations are required","tool_count":int(inventory[1])if inventory else None,"tools_list_utf8_bytes":int(inventory[2])if inventory else None}
    value["mcp"].update(physical.get("mcp",{}))
    value["release_hashes"]=physical.get("release_hashes",{})
    value["physical_evidence_location"]=None  # final exact release tag is unknown before F09
    value["capabilities"]=capability_matrix(physical,{case["test_name"] for case in tests if case["result"]=="PASS"})
    value["CAPABILITY_PARITY"]="PASS" if all(
        row["status"]=="PRESERVED" for row in value["capabilities"]) else "BLOCKED"
    for key in ["INSTALLED_MATCHED","PHYSICAL_MULTI_ACCOUNT","REAL_CYCLE_ACCEPTED"]:value[key]=physical.get(key,{}).get("result","NOT_RUN")
    validate(value);out.write_text(json.dumps(value,ensure_ascii=False,indent=2)+"\n",encoding="utf-8");print(out)
    return 0 if code_ok else 1

def self_test():
    import unittest
    class Tests(unittest.TestCase):
        def test_counts_actual_test_results(self):self.assertEqual(executed_tests("test a::real_fixture ... ok\ntest b ... ignored\n"),[("a::real_fixture","ok"),("b","ignored")])
        def test_zero_is_rejected(self):
            with self.assertRaisesRegex(ValueError,"zero"):validate({"test_counts":{"executed":0}})
        def test_missing_requirement_rejected(self):
            with self.assertRaisesRegex(ValueError,"missing mandatory"):validate({"test_counts":{"executed":1},"tests":[]})
        def test_duplicate_mandatory_record_rejected(self):
            with self.assertRaisesRegex(ValueError,"duplicate"):
                validate({"test_counts":{"executed":1},"tests":[{"requirement_id":"CLEAN-01","test_name":"x","result":"PASS","evidence":["scripts/acceptance_product.py"]},{"requirement_id":"CLEAN-01","test_name":"x","result":"PASS","evidence":["scripts/acceptance_product.py"]}]})
        def test_count_log_mismatch_rejected(self):
            with self.assertRaisesRegex(ValueError,"count"):
                validate({"test_counts":{"executed":1,"parsed_executed":2},"tests":[]})
        def test_unrelated_preserves_test_does_not_prove_central_risk(self):
            name="market_data::tests::ticker_preserves_exchange_decimal_text_exactly"
            self.assertFalse(any(name in names for names in REQUIRED.values()))

        def test_fake_venue_fixture_requires_native_and_worker_proof(self):
            case={"transport":"fake_venue_fixture","venue":"fake",
                  "real_exchange_post_count":0,
                  "native_test_name":"trade_service::execute::tests::duplicate_execute_one_post",
                  "worker_test_name":FAKE_WORKER_ROUTE_TEST}
            native,worker=case["native_test_name"],case["worker_test_name"]
            self.assertFalse(admissible_transport("trade.execute",case,set()))
            self.assertFalse(admissible_transport("trade.execute",case,{native}))
            self.assertTrue(admissible_transport("trade.execute",case,{native,worker}))
            self.assertFalse(admissible_transport("trade.preview",case,{native,worker}))
            self.assertFalse(admissible_transport("market.snapshot",case,{native,worker}))
            self.assertFalse(admissible_transport("accounts.connect",case,{native,worker}))
            self.assertFalse(admissible_transport("accounts.pause",case,{native,worker}))
            self.assertFalse(admissible_transport("strategies.create",case,{native,worker}))
            self.assertFalse(admissible_transport("trade.cancel",case,{native,worker}))
            self.assertFalse(admissible_transport("trade.execute",{
                **case,"worker_test_name":"strict_action_fixtures"},{native,worker}))
            self.assertFalse(admissible_transport("trade.execute",{**case,"venue":"real"},{native,worker}))
            self.assertFalse(admissible_transport("trade.execute",{**case,"real_exchange_post_count":1},{native,worker}))
            self.assertFalse(admissible_transport("trade.execute",{**case,"real_exchange_post_count":False},{native,worker}))
            self.assertTrue(admissible_transport("market.snapshot",{"transport":"live_mcp"},set()))
            for route in FAKE_VENUE_ACTIONS:
                self.assertFalse(admissible_transport(route,{"transport":"live_mcp"},set()),route)
            self.assertFalse(admissible_transport("trade.execute",{
                **case,"real_exchange_post_count":0,"native_test_name":"unrelated::test"},
                {"unrelated::test",worker}))

        def test_mutation_positive_must_prove_specific_business_state(self):
            # Former validator incorrectly treated any completed+data={} or
            # generic fake DO route receipt as full business execution.
            for route in FAKE_VENUE_ACTIONS:
                for data in ({},{"venue":"fake","exchange_post_count":0},
                             {"schema":"okx.fake-venue.fixture/v1","venue":"fake",
                              "exchange_post_count":0}):
                    self.assertFalse(checked_capability_outcome(route,{},{
                        "status":"completed","quality":"FRESH","reason_code":None,
                        "data":data},"positive"),route)
            account={"id":"fixture_a","identity_verified":True,"enabled":True,
                     "venue":"okx","environment":"demo",
                     "account_key":"okx:demo:"+"a"*64}
            self.assertTrue(checked_capability_outcome("accounts.connect",{
                "alias":"fixture_a","enabled":True},{
                "status":"completed","data":{"status":"CONNECTED","account":account,
                  "configuration":{"accounts":[account]}}},"positive"))
            for action,paused in (("pause",True),("resume",False)):
                state={"id":"fixture_a","account_key":account["account_key"],
                       "explicit_pause":paused}
                self.assertTrue(checked_capability_outcome("accounts."+action,
                    {"account":"fixture_a"},{"status":"completed",
                    "data":{"status":"CONFIGURED","configuration":{
                    "revision":2,"accounts":[state]}}},"positive"))
                state["explicit_pause"]=not paused
                self.assertFalse(checked_capability_outcome("accounts."+action,
                    {"account":"fixture_a"},{"status":"completed",
                    "data":{"status":"CONFIGURED","configuration":{
                    "revision":2,"accounts":[state]}}},"positive"))
            limits={"max_notional_usdt":"20"}
            state={"id":"fixture_a","limits":limits}
            self.assertTrue(checked_capability_outcome("accounts.set_limits",
                {"account":"fixture_a","limits":limits},{"status":"completed",
                "data":{"status":"CONFIGURED","configuration":{
                "revision":3,"accounts":[state]}}},"positive"))
            self.assertFalse(checked_capability_outcome("accounts.set_limits",
                {"account":"other","limits":limits},{"status":"completed",
                "data":{"status":"CONFIGURED","configuration":{
                "revision":3,"accounts":[state]}}},"positive"))
            for action,enabled in (("create",False),("start",True),("stop",False)):
                instance={"instance_id":"ema_fixture","definition_id":"ema_cross_v1",
                          "policy_revision":1,"enabled":enabled}
                self.assertTrue(checked_capability_outcome("strategies."+action,
                    {"instance_id":"ema_fixture","definition_id":"ema_cross_v1"},
                    {"status":"completed","data":{"instance":instance}},"positive"))
                self.assertFalse(checked_capability_outcome("strategies."+action,
                    {"instance_id":"other","definition_id":"ema_cross_v1"},
                    {"status":"completed","data":{"instance":instance}},"positive"))
            intent={"intent_id":"fake-intent-1","state":"FILLED","plan":{"operation":"open"}}
            self.assertTrue(checked_capability_outcome("trade.execute",{},
                {"status":"completed","data":{"intent":intent}},"positive"))
            self.assertTrue(checked_capability_outcome("trade.close",{},
                {"status":"completed","data":{"intent":intent}},"positive"))
            self.assertFalse(checked_capability_outcome("trade.cancel",{},
                {"status":"completed","data":{"intent":intent}},"positive"))
            intent["state"]="CANCELLED"
            self.assertTrue(checked_capability_outcome("trade.cancel",{},
                {"status":"completed","data":{"intent":intent}},"positive"))

        def test_mutation_negative_never_accepts_unknown_or_generic_failure(self):
            self.assertFalse(checked_capability_outcome("trade.execute",{},
                {"status":"failed","reason_code":"REQUEST_FAILED",
                "next_action":"trade.status"},"negative"))
            self.assertFalse(checked_capability_outcome("strategies.create",{},
                {"status":"rejected","reason_code":"INVALID_STRATEGY_REQUEST"},"negative"))
            self.assertTrue(checked_capability_outcome("strategies.create",{},
                {"status":"rejected","reason_code":"INVALID_STRATEGY_REQUEST",
                "next_action":"inspect_diagnostics"},"negative"))
        def test_schema_only_negative_requires_strict_contract_and_observed_tool_error(self):
            contract=json.loads((ROOT/"cloudflare/okx-mcp/contracts/tools.schema.json").read_text(encoding="utf-8-sig"))
            proof={"transport":"live_mcp","rejection_stage":"tool_schema",
                   "worker_test_name":"strict_action_fixtures"}
            for route in SCHEMA_ONLY_NEGATIVE_ACTIONS:
                action=route.split(".",1)[1]
                arguments={"action":action,"raw_rpc":"forbidden"}
                error=(repr(arguments)+" is not valid under any of the given schemas\\n"
                       "Failed validating 'oneOf' in schema:\\n"
                       " {'additionalProperties': False}\\n"
                       "On instance:\\n"+repr(arguments))
                response={"tool_error":error}
                self.assertTrue(checked_schema_negative(
                    route,arguments,response,proof,{"strict_action_fixtures"},contract),route)
                self.assertFalse(checked_schema_negative(
                    route,arguments,response,proof,set(),contract))
                self.assertFalse(checked_schema_negative(
                    route,arguments,response,{**proof,"transport":"fake_venue_fixture"},
                    {"strict_action_fixtures"},contract))
                self.assertFalse(checked_schema_negative(
                    route,arguments,{"structuredContent":{"status":"rejected",
                    "reason_code":"SYNTHETIC"}},proof,{"strict_action_fixtures"},contract))
                self.assertFalse(checked_schema_negative(
                    route,arguments,{"tool_error":"invalid request"},
                    proof,{"strict_action_fixtures"},contract))
                self.assertFalse(checked_schema_negative(
                    route,{"action":action},response,proof,{"strict_action_fixtures"},contract))
            self.assertFalse(checked_schema_negative(
                "trade.execute",{"action":"execute","raw_rpc":"forbidden"},
                {"tool_error":"not valid under any of the given schemas"},
                proof,{"strict_action_fixtures"},contract))
            self.assertFalse(checked_schema_negative(
                "accounts.status",{"action":"status","raw_rpc":"forbidden"},
                {"tool_error":"not valid under any of the given schemas"},
                proof,{"strict_action_fixtures"},contract))
        def test_schema_negative_rejects_relaxed_contract_and_spoofed_result(self):
            contract=json.loads((ROOT/"cloudflare/okx-mcp/contracts/tools.schema.json").read_text(encoding="utf-8-sig"))
            contract=json.loads(json.dumps(contract))
            for tool in contract["tools"]:
                if tool["name"]=="diagnostics":
                    for variant in tool["inputSchema"]["oneOf"]:
                        if variant.get("properties",{}).get("action",{}).get("const")=="runtime":
                            variant["additionalProperties"]=True
            args={"action":"runtime","raw_rpc":"forbidden"}
            proof={"transport":"live_mcp","rejection_stage":"tool_schema",
                   "worker_test_name":"strict_action_fixtures"}
            response={"tool_error":repr(args)+" is not valid under any of the given schemas\\n"
                       "Failed validating 'oneOf' in schema:\\n"
                       " {'additionalProperties': False}\\nOn instance:\\n"+repr(args)}
            self.assertFalse(checked_schema_negative(
                "diagnostics.runtime",args,response,proof,{"strict_action_fixtures"},contract))
        def test_f07_f08_required_names_are_explicitly_bound(self):
            self.assertEqual(REQUIRED["F07"],{
                "account_store::tests::strategy_crash_pending_decision_to_intent",
                "account_store::tests::strategy_policy_retry_keeps_immutable_lineage_and_one_intent"})
            self.assertIn("trade_service::strategies::budget_contract_tests::strategy_risk_budget_not_notional",REQUIRED["F08"])
        def test_capability_parity_is_fail_closed(self):
            rows=capability_matrix()
            self.assertEqual(len(rows),16)
            self.assertEqual(len({r["legacy_tool"] for r in rows}),16)
            self.assertTrue(all(row["status"]=="GAP" for row in rows))
            self.assertEqual(next(r for r in rows if r["legacy_tool"]=="market_intelligence")["missing_actions"],[])
            self.assertEqual(next(r for r in rows if r["legacy_tool"]=="market_intelligence")["status"],"GAP")
        def test_capability_proof_requires_matching_installed_release(self):
            physical={"INSTALLED_MATCHED":{"result":"PASS"},
                "mcp":{"physical_network":"PASS"},
                "release_hashes":{"source_tree":"a"*40,"installed_agent_tree":"b"*40,
                                  "worker_tree":"a"*40},
                "capabilities":{"runtime_status":{"result":"PASS","code_owner":"Worker",
                    "positive_assertion":"PASS","negative_assertion":"PASS",
                    "positive_evidence":["docs/acceptance/398/none.json"],
                    "negative_evidence":["docs/acceptance/398/none.json"]}}}
            rows=capability_matrix(physical)
            self.assertEqual(next(r for r in rows if r["legacy_tool"]=="runtime_status")["status"],"GAP")
        def test_partial_and_generic_errors_never_count_as_capability_proof(self):
            self.assertFalse(checked_capability_outcome("market.snapshot",{},{
                "status":"partial","data":{"market":{}}},"positive"))
            self.assertFalse(checked_capability_outcome("research.status",{},{
                "status":"rejected","reason_code":"REQUEST_FAILED","next_action":"inspect"},"negative"))
            self.assertFalse(checked_capability_outcome("market.screen",{"universe":{"instrument_type":"SWAP"}},{
                "status":"completed","data":{"rows":[],"total_rows":0}},"positive"))
        def test_live_market_positive_requires_bounded_typed_book(self):
            overview={"status":"completed","quality":"complete","data":{
                "schema":"okx.market.overview/v1","source_mode":"websocket",
                "book_sequence":42,"market":{"ticker":{"last":"100"}}}}
            self.assertTrue(checked_capability_outcome("market.snapshot",{},overview,"positive"))
            overview["data"]["order_book"]=[{"price":"100","size":"1"}]*4000
            self.assertFalse(checked_capability_outcome("market.snapshot",{},overview,"positive"))
            del overview["data"]["order_book"]
            overview["data"]["source_mode"]="rest"
            self.assertFalse(checked_capability_outcome("market.snapshot",{},overview,"positive"))
            book={"status":"completed","quality":"complete","data":{
                "schema":"okx.market.microstructure/v1","book_sequence":42,
                "quality":"complete","bids":[{"price":"100","size":"1"}],
                "asks":[{"price":"101","size":"1"}]}}
            self.assertTrue(checked_capability_outcome("market.microstructure",{},book,"positive"))
            book["data"]["bids"]*=11
            self.assertFalse(checked_capability_outcome("market.microstructure",{},book,"positive"))
        def test_scenario_has_read_only_semantics_not_exchange_margin_proof(self):
            scenario={"status":"completed","data":{
                "schema":"okx.portfolio.scenario/v1","execution":"virtual_read_only",
                "exchange_post":False,"venue_margin_verified":False,
                "net_stress_pnl_usd":"-1"}}
            self.assertTrue(checked_capability_outcome("portfolio.scenario",{},scenario,"positive"))
            scenario["data"]["exchange_post"]=True
            self.assertFalse(checked_capability_outcome("portfolio.scenario",{},scenario,"positive"))
        def test_global_rank_requires_verified_complete_immutable_artifact(self):
            request={"action":"screen","limit":1,"universe":{"instrument_type":"SWAP","rank_globally":True}}
            job="a"*64
            proof={"status":"completed","quality":"FRESH","reason_code":None,"data":{
                "rows":[{"instrument":"BTC-USDT-SWAP","quality":"complete","gaps":[],
                         "metrics":{"returns":0.01}}],
                "total_rows":2,"job_id":job,"result_artifact":"sha256:"+"b"*64,
                "rank_request_fingerprint":"sha256:"+"c"*64,"next_cursor":job+":1",
                "universe_page":{"global_ranking":True,"scope":"complete_venue_universe",
                                 "universe_size":2,"reference_generation":"reference-1",
                                 "has_more":True}}}
            self.assertTrue(checked_capability_outcome("market.screen",request,proof,"positive"))
            proof["data"]["universe_page"]["global_ranking"]=False
            self.assertFalse(checked_capability_outcome("market.screen",request,proof,"positive"))
            proof["data"]["universe_page"]["global_ranking"]=True
            proof["data"]["rows"][0]["quality"]="partial"
            self.assertFalse(checked_capability_outcome("market.screen",request,proof,"positive"))
            proof["data"]["rows"][0]["quality"]="complete"
            proof["data"]["result_artifact"]=None
            self.assertFalse(checked_capability_outcome("market.screen",request,proof,"positive"))
            proof["data"]["result_artifact"]="sha256:"+"b"*64
            proof["data"]["next_cursor"]="wrong-job:1"
            self.assertFalse(checked_capability_outcome("market.screen",request,proof,"positive"))
            proof["data"]["next_cursor"]=None
            proof["data"]["universe_page"]["has_more"]=False
            self.assertTrue(checked_capability_outcome("market.screen",request,proof,"positive"))
            proof["data"]["universe_page"]["universe_size"]=1
            self.assertFalse(checked_capability_outcome("market.screen",request,proof,"positive"))
        def test_ordinary_universe_page_remains_name_paged(self):
            request={"action":"screen","limit":1,"universe":{"instrument_type":"SWAP"}}
            proof={"status":"completed","quality":"FRESH","data":{"rows":[],"total_rows":0,
                "universe_page":{"global_ranking":False,"reference_generation":"ref",
                                 "has_more":True,"next_after_instrument":"BTC-USDT-SWAP"}}}
            self.assertTrue(checked_capability_outcome("market.screen",request,proof,"positive"))
            proof["data"]["universe_page"]["next_after_instrument"]=None
            self.assertFalse(checked_capability_outcome("market.screen",request,proof,"positive"))
        def test_research_async_positive_requires_durable_job_not_fake_completion(self):
            job="e"*64
            proof={"status":"accepted","quality":"FRESH","reason_code":None,
                   "job_id":job,"data":{"id":job,"state":"queued","progress":0}}
            for route in ("research.backtest","research.grid"):
                self.assertTrue(checked_capability_outcome(route,{},proof,"positive"))
            self.assertFalse(checked_capability_outcome("trade.execute",{},proof,"positive"))
            proof["data"]["id"]="different-id"
            self.assertFalse(checked_capability_outcome("research.backtest",{},proof,"positive"))
            proof["data"]["id"]=job
            proof["data"]["state"]="failed"
            self.assertFalse(checked_capability_outcome("research.grid",{},proof,"positive"))
            proof["data"]["state"]="running"
            proof["reason_code"]="REQUEST_FAILED"
            self.assertFalse(checked_capability_outcome("research.grid",{},proof,"positive"))
        def test_worker_claims_require_complete_named_pass_line(self):
            self.assertEqual(executed_tests("risk_concurrent_accounts_campaign_limit PASS\n"),[("risk_concurrent_accounts_campaign_limit","ok")])
            self.assertEqual(executed_tests("risk_concurrent_accounts_campaign_limit FAIL\n"),[("risk_concurrent_accounts_campaign_limit","FAILED")])
            self.assertEqual(executed_tests("clean_initialization_deletes_only_old_financial_keys PASS\n"),[("clean_initialization_deletes_only_old_financial_keys","ok")])
    return 0 if unittest.TextTestRunner().run(unittest.defaultTestLoader.loadTestsFromTestCase(Tests)).wasSuccessful()else 1
if __name__=="__main__":
    parser=argparse.ArgumentParser(description=__doc__);parser.add_argument("--run",action="store_true");parser.add_argument("--self-test",action="store_true");parser.add_argument("--code-log",action="append",default=[]);parser.add_argument("--performance",default="docs/acceptance/398/analytics-performance.json");parser.add_argument("--physical");parser.add_argument("--output",default="acceptance.json");args=parser.parse_args()
    try:sys.exit(self_test()if args.self_test else build(args))
    except (ValueError,OSError,subprocess.SubprocessError)as error:print(str(error),file=sys.stderr);sys.exit(2)

