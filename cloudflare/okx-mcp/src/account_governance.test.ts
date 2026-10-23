import {validGovernance,reviseGovernance,projectRiskPolicy,type Governance,type Account,type Limits} from "./account_governance.js";
function yes(v:boolean,s:string){if(!v)throw Error(s);}
export const limits:Limits={max_modelled_loss_usdt:"5",max_order_loss_usdt:"1",max_notional_usdt:"100",
 max_leverage:"3",max_daily_realized_loss_usdt:"2",max_drawdown_ratio:"0.1",allowed_instruments:["BTC-USDT-SWAP"]};
export const initial:Governance={schema:"okx.governance/v3",revision:0,installation_id:"installation_test",
 campaign_id:"sha256:"+"c".repeat(64),campaign_loss_limit_usd:"20",max_total_modelled_loss_usdt:"5",accounts:[]};
export const account:Account={id:"primary",label:"Primary",venue:"okx",environment:"real",uid_fingerprint:"a".repeat(64),
 identity_verified:true,enabled:true,explicit_pause:false,policy_revision:1,account_key:"okx:real:"+"a".repeat(64),limits,strategies:[]};
const policy:Governance={...initial,revision:1,accounts:[account]};
yes(validGovernance(initial)&&validGovernance(policy),"clean account policy validates");
yes(!validGovernance({...policy,trading_requested:true}),"old grant fields are not decoded");
yes(!validGovernance({...policy,accounts:[{...account,account_key:"okx:real:"+"b".repeat(64)}]}),"identity and key cannot disagree");
yes(projectRiskPolicy(policy)?.enabled===true,"saved access enables authority, never native POST");
const paused={...policy,accounts:[{...account,explicit_pause:true}]};
yes(projectRiskPolicy(paused)?.enabled===false,"pause blocks new risk");
yes(Object.keys(projectRiskPolicy(paused)!.account_limits_usdt).length===1,"disabled_account_with_exposure_remains_in_census");
const change={expected_revision:1,max_total_modelled_loss_usdt:"4",accounts:[{...account,limits:{...limits,max_modelled_loss_usdt:"4"}}]};
const revised=reviseGovernance(policy,change);
yes(revised!==null&&revised.revision===2&&revised.accounts[0].policy_revision===2,"policy CAS is atomic");
yes(reviseGovernance(revised!,change)===null,"stale limits do not overwrite policy");
yes(reviseGovernance(policy,{...change,accounts:[]})===null,"cannot remove exposure through configuration");
yes(reviseGovernance(policy,{...change,accounts:[{...account,limits:{...limits,max_order_loss_usdt:"6"}}]})===null,"per order bounds");
yes(reviseGovernance(policy,{...change,accounts:[{...account,limits:{...limits,max_drawdown_ratio:"1.1"}}]})===null,"ratio bounds");
