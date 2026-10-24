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
    "F03":{"mcp_nested_failure_preserves_reason_code","mcp_queued_job_remains_accepted"},
    "F04":{"analytics::jobs::tests::transient_rate_defer_is_requeued_instead_of_cached_as_failed"},
    "F05":{"analytics::tests::grid_result_includes_selected_train_test_and_equity"},
    "F06":{"portfolio::tests::portfolio_before_t0_is_partial"},
    "F07":{"trade_service::strategies::tests::decision_claim_creates_durable_intent_once"},
    "F08":{"trade_service::strategies::tests::strategy_risk_budget_not_notional","analytics::jobs::tests::job_status_reads_bounded_rows_at_100k_jobs"},
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
    if status!="completed" or envelope.get("reason_code") is not None:
        return False
    data=envelope.get("data")
    if not isinstance(data,dict) or data.get("failure") is not None:
        return False
    if envelope.get("quality") in ("NOT_READY","DEGRADED","partial","stale"):
        return False
    if route=="market.screen":
        if not isinstance(data.get("rows"),list) or not isinstance(data.get("total_rows"),int):
            return False
        if "universe" in request:
            page=data.get("universe_page")
            if not (isinstance(page,dict)
                and page.get("global_ranking") is False
                and isinstance(page.get("reference_generation"),str)
                and bool(page["reference_generation"])
                and isinstance(page.get("has_more"),bool)
                and (not page["has_more"] or isinstance(page.get("next_after_instrument"),str))):
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

def capability_matrix(physical=None):
    physical=physical or {}
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
                    or proof.get("transport")!="live_mcp"):
                    return False
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
    value["capabilities"]=capability_matrix(physical)
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
        def test_scenario_has_read_only_semantics_not_exchange_margin_proof(self):
            scenario={"status":"completed","data":{
                "schema":"okx.portfolio.scenario/v1","execution":"virtual_read_only",
                "exchange_post":False,"venue_margin_verified":False,
                "net_stress_pnl_usd":"-1"}}
            self.assertTrue(checked_capability_outcome("portfolio.scenario",{},scenario,"positive"))
            scenario["data"]["exchange_post"]=True
            self.assertFalse(checked_capability_outcome("portfolio.scenario",{},scenario,"positive"))
        def test_worker_claims_require_complete_named_pass_line(self):
            self.assertEqual(executed_tests("risk_concurrent_accounts_campaign_limit PASS\n"),[("risk_concurrent_accounts_campaign_limit","ok")])
            self.assertEqual(executed_tests("risk_concurrent_accounts_campaign_limit FAIL\n"),[("risk_concurrent_accounts_campaign_limit","FAILED")])
            self.assertEqual(executed_tests("clean_initialization_deletes_only_old_financial_keys PASS\n"),[("clean_initialization_deletes_only_old_financial_keys","ok")])
    return 0 if unittest.TextTestRunner().run(unittest.defaultTestLoader.loadTestsFromTestCase(Tests)).wasSuccessful()else 1
if __name__=="__main__":
    parser=argparse.ArgumentParser(description=__doc__);parser.add_argument("--run",action="store_true");parser.add_argument("--self-test",action="store_true");parser.add_argument("--code-log",action="append",default=[]);parser.add_argument("--performance",default="docs/acceptance/398/analytics-performance.json");parser.add_argument("--physical");parser.add_argument("--output",default="acceptance.json");args=parser.parse_args()
    try:sys.exit(self_test()if args.self_test else build(args))
    except (ValueError,OSError,subprocess.SubprocessError)as error:print(str(error),file=sys.stderr);sys.exit(2)

