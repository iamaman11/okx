#!/usr/bin/env python3
"""Executable contract guard for the clean Engine production surface."""
import argparse
import json
import pathlib
import re
import sys
ROOT = pathlib.Path(__file__).resolve().parents[1]
FORBIDDEN = re.compile(r'\b(?:DurableExecutionLedger|ExecutionLedgerStore|ExecutionRuntimeMode|ProductionTradingPolicy|BeginStrategyPeriod|FinalizeStrategyPeriod|PrepareExecution|SubmitExecution|RecoverReadOnlySelection|SelectAccount|StoredState|sealed_pre_v14)\b')
TOOLS = {'accounts','strategies','market','research','trade','portfolio','diagnostics'}
def check_source(text):
    return sorted(set(FORBIDDEN.findall(text)))
def check(root):
    errors=[]
    for base in ['crates/okx-agent/src','crates/okx-execution/src','crates/okx-protocol/src','crates/okx-host-control/src','cloudflare/okx-mcp/src']:
        for path in (root/base).rglob('*'):
            if path.suffix not in {'.rs','.ts'} or '.test.' in path.name:continue
            for symbol in check_source(path.read_text(encoding='utf-8-sig')):errors.append(f'{path.relative_to(root)}: obsolete production API {symbol}')
    schema=json.loads((root/'cloudflare/okx-mcp/contracts/tools.schema.json').read_text(encoding='utf-8-sig'))
    tools=schema['tools']
    if {t['name'] for t in tools}!=TOOLS or len(tools)!=7:errors.append('public tool set must contain exactly seven product tools')
    if len(json.dumps({'jsonrpc':'2.0','id':1,'result':{'tools':tools}},ensure_ascii=False,separators=(',',':')).encode())>24576:errors.append('tools/list exceeds 24 KiB')
    for tool in tools:
        variants=tool['inputSchema'].get('oneOf',[])
        if not variants:errors.append(f"{tool['name']}: missing strict action union")
        for variant in variants:
            if variant.get('additionalProperties') is not False:errors.append(f"{tool['name']}: action accepts unknown fields")
    return errors
if __name__=='__main__':
    parser=argparse.ArgumentParser();parser.add_argument('--self-test',action='store_true');args=parser.parse_args()
    if args.self_test:
        assert check_source('pub struct ExecutionLedgerStore;')==['ExecutionLedgerStore']
        assert not check_source('pub struct AccountStore;')
        print('PRODUCT_CONTRACT_SELF_TEST=PASS');sys.exit(0)
    errors=check(ROOT)
    for error in errors:print(error,file=sys.stderr)
    print('PRODUCT_CONTRACT='+('FAIL' if errors else 'PASS'));sys.exit(bool(errors))
