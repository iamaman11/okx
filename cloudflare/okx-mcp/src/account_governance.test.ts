import {defaultAccountPolicy} from "./account_policy.js";
import {migrateV1,validGovernance,reviseGovernance,projectRiskPolicy} from "./account_governance.js";
function yes(v:boolean,s:string){if(!v)throw Error(s);}
const a="a".repeat(64), old=defaultAccountPolicy();
const migrated=migrateV1(old);
yes(migrated!==null&&validGovernance(migrated),"migration parses");
yes(migrated!.operator_authorization.status==="UNVERIFIED"
  &&!migrated!.trading_requested&&!projectRiskPolicy(migrated!)?.enabled,
  "migration preserves fail-closed rights");
const p=migrateV1({...old,revision:3,trading_requested:true,allowed_account_uids:[a]})!;
yes(p.accounts.length===1&&!p.accounts[0].enabled&&!p.accounts[0].identity_verified
  &&!p.trading_requested,"old allowlist not verified account");
const limits={...p.accounts[0].limits,max_modelled_loss_usdt:"2.5",max_order_loss_usdt:"1",
  max_notional_usdt:"100",max_leverage:"3",max_daily_realized_loss_usdt:"1",
  max_drawdown_ratio:"0.1",allowed_instruments:["BTC-USDT-SWAP"]};
const change={expected_revision:3,trading_requested:false,max_total_modelled_loss_usdt:"5",
  accounts:[{id:p.accounts[0].id,label:"Primary",enabled:false,limits,strategies:[]}]};
const updated=reviseGovernance(p,change);
yes(updated!==null&&updated.revision===4&&updated.accounts[0].limits.max_order_loss_usdt==="1",
  "bounded atomic config");
yes(reviseGovernance(updated!,change)===null,"stale CAS");
yes(reviseGovernance(p,{...change,accounts:[{...change.accounts[0],enabled:true}]})===null,
  "unverified UID cannot enable");
yes(reviseGovernance(p,{...change,trading_requested:true})===null,"unverified grant cannot enable");
yes(reviseGovernance(p,{...change,max_total_modelled_loss_usdt:"11"})===null,
  "ordinary config cannot elevate grant");
yes(reviseGovernance(p,{...change,accounts:[{...change.accounts[0],
  limits:{...limits,max_order_loss_usdt:"3"}}]})===null,"per-order cap");
yes(reviseGovernance(p,{...change,accounts:[{...change.accounts[0],
  limits:{...limits,max_drawdown_ratio:"1.01"}}]})===null,"ratio bound");

const strategy={id:"momentum_A",enabled:true,allocated_capital_usdt:"2",
 max_modelled_loss_usdt:"1",max_daily_realized_loss_usdt:"0.5",max_drawdown_ratio:"0.1"};
const allocated=reviseGovernance(p,{...change,accounts:[{...change.accounts[0],strategies:[strategy]}]});
yes(allocated!==null&&allocated.accounts[0].strategies.length===1,"strategy capital policy typed");
yes(reviseGovernance(p,{...change,accounts:[{...change.accounts[0],
 strategies:[strategy,{...strategy,id:"momentum_B",max_modelled_loss_usdt:"2"}]}]})===null,
 "sum of strategy risks cannot exceed account loss cap");
yes(reviseGovernance(p,{...change,accounts:[{...change.accounts[0],
 strategies:[{...strategy,max_drawdown_ratio:null}]}]})===null,
 "enabled strategy needs explicit drawdown gate");

const verified={...p,accounts:p.accounts.map(a=>({...a,identity_verified:true,enabled:false}))};
const cohort=projectRiskPolicy(verified);
yes(cohort!==null&&Object.keys(cohort.account_limits_usdt).length===1,
 "disabled registered Real accounts remain in the risk cohort");
yes(projectRiskPolicy(p)===null,"unverified account cannot masquerade as zero exposure");
