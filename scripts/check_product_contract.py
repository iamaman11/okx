#!/usr/bin/env python3
"""Executable contract guard for the clean Engine production surface."""
import argparse
import json
import pathlib
import re
import sys
ROOT = pathlib.Path(__file__).resolve().parents[1]
FORBIDDEN = re.compile(r'\b(?:DurableExecutionLedger|ExecutionLedgerStore|ExecutionRuntimeMode|ProductionTradingPolicy|BeginStrategyPeriod|FinalizeStrategyPeriod|PrepareExecution|SubmitExecution|RecoverReadOnlySelection|SelectAccount|StoredState|sealed_pre_v14)\b')
# A per-action trading interface makes read-only and consequential
# financial operations independently inspectable by the MCP host.
TRADE_ACTIONS = ('preview','execute','status','cancel','close','capabilities','preflight')
CORE_TOOLS = {'accounts','strategies','market','research','portfolio','diagnostics'}
TOOLS = CORE_TOOLS | {'trade.' + action for action in TRADE_ACTIONS}
READ_TRADE = {'preview','status','capabilities','preflight'}
WRITE_TRADE = {'execute','cancel','close'}
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
    if {t['name'] for t in tools}!=TOOLS or len(tools)!=len(TOOLS):
        errors.append('public tool set must expose six core tools and seven explicit trade operations')
    if len(json.dumps({'jsonrpc':'2.0','id':1,'result':{'tools':tools}},ensure_ascii=False,separators=(',',':')).encode())>24576:
        errors.append('tools/list exceeds 24 KiB')
    for tool in tools:
        name=tool['name']
        ann=tool.get('annotations',{})
        if any(type(ann.get(key)) is not bool for key in ('readOnlyHint','destructiveHint','openWorldHint')):
            errors.append(f'{name}: explicit boolean risk annotations required')
        if not tool.get('description') or len(tool['description'])>200:
            errors.append(f'{name}: missing or overly long behavior description')
        if name.startswith('trade.'):
            action=name.split('.',1)[1]
            if ('action' in tool['inputSchema'].get('properties',{}) or
                'action' in tool['inputSchema'].get('required',[])):
                errors.append(f'{name}: caller must not override tool identity with action')
            if (ann.get('readOnlyHint')!=(action in READ_TRADE) or
                ann.get('destructiveHint')!=(action in WRITE_TRADE) or
                ann.get('openWorldHint') is not False):
                errors.append(f'{name}: unsafe or misleading trading risk annotation')
            variants=[tool['inputSchema']]
        else:
            variants=tool['inputSchema'].get('oneOf',[])
            if not variants:errors.append(f'{name}: missing strict action union')
        for variant in variants:
            if variant.get('additionalProperties') is not False:
                errors.append(f'{name}: action accepts unknown fields')
    # Generated MCP contract must reflect the live Rust strategy v1 and
    # all four read-only research result views (one source, no stale snapshot).
    catalog={t['name']:t for t in tools}
    def variant(tool,action):
        return next((v for v in catalog[tool]['inputSchema']['oneOf']
                     if v.get('properties',{}).get('action',{}).get('const')==action),None)
    view=variant('research','result')
    if (view is None or view['properties'].get('view',{}).get('enum')!=
        ['summary','trades','equity','variants']):
        errors.append('research.result is missing one or more supported result views')
    for action in ('create','update'):
        rule=variant('strategies',action)
        if rule is None:
            errors.append('strategies.'+action+' missing')
            continue
        params=rule['properties'].get('parameters',{}).get('properties',{})
        if (params.get('fast',{}).get('maximum')!=999 or
            params.get('slow',{}).get('maximum')!=1000):
            errors.append('strategies.'+action+' does not match Rust StrategyParametersV1 bounds')
        if action=='create' and rule['properties'].get('version',{}).get('maximum')!=1:
            errors.append('strategies.create must reject unsupported version > 1')
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
