use super::*;
use okx_api::MarginMode;

/// Select the actual venue price before calculating stop, size or reserving risk.
///
/// A limit is a hard adverse-price bound.  Rounding it in the adverse direction
/// (the old behaviour) can silently turn an otherwise valid command into a
/// price outside that bound.  We instead select the best tick that remains
/// inside the bound and require it to still cross the current quote for orders
/// which must be immediately executable.
fn quantize_executable_price(
    quote: Decimal,
    explicit_limit: Option<Decimal>,
    max_slippage_bps: u32,
    tick: Decimal,
    buy: bool,
    order_type: &str,
) -> Result<Decimal, String> {
    if quote <= Decimal::ZERO || tick <= Decimal::ZERO {
        return Err("INVALID_INSTRUMENT_RULES".into());
    }
    let slip = Decimal::from(max_slippage_bps) / Decimal::from(10_000_u32);
    let market_bound = quote
        * if buy {
            Decimal::ONE + slip
        } else {
            Decimal::ONE - slip
        };
    let bound = match (buy, explicit_limit) {
        (true, Some(limit)) if limit > market_bound => return Err("ORDER_SLIPPAGE_LIMIT".into()),
        (false, Some(limit)) if limit < market_bound => {
            return Err("ORDER_SLIPPAGE_LIMIT".into());
        }
        (true, Some(limit)) => limit.min(market_bound),
        (false, Some(limit)) => limit.max(market_bound),
        (_, None) => market_bound,
    };

    // Post-only is deliberately not made marketable: it must keep the user's
    // price constraint and must not cross the current book after quantization.
    if order_type == "post_only" {
        let limit = explicit_limit.ok_or("POST_ONLY_LIMIT_REQUIRED")?;
        let price = if buy {
            (limit / tick).floor() * tick
        } else {
            (limit / tick).ceil() * tick
        };
        if (buy && price >= quote) || (!buy && price <= quote) {
            return Err("POST_ONLY_WOULD_CROSS".into());
        }
        return Ok(price);
    }

    // For a buy, floor the upper bound; for a sell, ceil the lower bound.  Both
    // choices preserve the requested slippage and any explicit limit.
    let price = if buy {
        (bound / tick).floor() * tick
    } else {
        (bound / tick).ceil() * tick
    };
    if price <= Decimal::ZERO
        || (buy && (price > bound || price < quote))
        || (!buy && (price < bound || price > quote))
    {
        return Err("NO_EXECUTABLE_PRICE_WITHIN_SLIPPAGE".into());
    }
    Ok(price)
}

impl TradeService {
    #[allow(clippy::too_many_arguments)] // Explicit typed command fields share one admission path.
    pub(super) async fn plan(
        &self,
        r: &AccountResources,
        c: &AccountControl,
        operation: TradeOperation,
        instance: &str,
        instrument: &str,
        side: PositionSide,
        sizing: &Sizing,
        order: &OrderConstraints,
        protection: Option<&Protection>,
        until: Instant,
        record_risk_state: bool,
    ) -> Result<IntentPlan, String> {
        let increase = matches!(
            operation,
            TradeOperation::Open | TradeOperation::Add | TradeOperation::Replace
        );
        if increase && !c.access_enabled() {
            return Err("ACCOUNT_NEW_RISK_PAUSED".into());
        }
        if !c.limits.allowed_instruments.iter().any(|s| s == instrument) && increase {
            return Err("INSTRUMENT_NOT_ALLOWED".into());
        }
        self.public_ws
            .demand_instrument(instrument)
            .await
            .map_err(err)?;
        let market = loop {
            match self
                .public_ws
                .risk_fresh_snapshot(
                    instrument,
                    now() as u64,
                    crate::config::QUOTE_MAX_AGE_MS,
                    crate::config::DERIVATIVE_CONTEXT_MAX_AGE_MS,
                    Utc::now().to_rfc3339(),
                )
                .await
            {
                Ok(m) => break m,
                Err(_) if Instant::now() < until => {
                    tokio::time::sleep(Duration::from_millis(50)).await
                }
                _ => return Err("PRICE_FRESHNESS_DEADLINE".into()),
            }
        };
        let rules = self
            .public_ws
            .instrument_rules(instrument)
            .await
            .ok_or("INSTRUMENT_RULES_UNAVAILABLE")?;
        if rules.reference_generation != market.market.reference_generation
            || rules.instrument.state != "live"
            || rules.instrument.settle_currency.as_deref() != Some("USDT")
            || rules.instrument.contract_type.as_deref() != Some("linear")
        {
            return Err("INSTRUMENT_RULES_OR_GENERATION_UNSUPPORTED".into());
        }
        if increase && !r.source_identity_ready().await {
            return Err("ACCOUNT_SOURCE_GENERATION_UNVERIFIED".into());
        }
        let private_generation = r.private_ws.status().await.connection_generation;
        let snap = r.observer.snapshot().await.map_err(err)?;
        let received = chrono::DateTime::parse_from_rfc3339(&snap.source_received_at)
            .map_err(err)?
            .timestamp_millis();
        if !crate::config::evidence_fresh(received, now(), crate::config::ACCOUNT_MAX_AGE_MS)
            || received > now()
            || snap.account_uid_fingerprint != r.configuration.account_key.verified_uid
            || snap.position_mode != "long_short_mode"
        {
            return Err("ACCOUNT_EVIDENCE_STALE_OR_MISMATCHED".into());
        }
        let side_name = if side == PositionSide::Long {
            "long"
        } else {
            "short"
        };
        let buy = if increase {
            side == PositionSide::Long
        } else {
            side == PositionSide::Short
        };
        let quote = dec(if buy {
            &market.market.ticker.best_ask
        } else {
            &market.market.ticker.best_bid
        })?;
        let tick = dec(&rules.instrument.tick_size)?;
        let lot = dec(&rules.instrument.lot_size)?;
        let minimum = dec(&rules.instrument.min_size)?;
        let ct = dec(rules
            .instrument
            .contract_value
            .as_deref()
            .ok_or("CONTRACT_VALUE_UNAVAILABLE")?)?;
        if [tick, lot, minimum, ct, quote]
            .iter()
            .any(|v| *v <= Decimal::ZERO)
        {
            return Err("INVALID_INSTRUMENT_RULES".into());
        }
        let explicit_limit = order.limit_price.as_ref().map(|p| dec(p)).transpose()?;
        let price = quantize_executable_price(
            quote,
            explicit_limit,
            order.max_slippage_bps,
            tick,
            buy,
            &order.order_type,
        )?;
        let slip = Decimal::from(order.max_slippage_bps) / Decimal::from(10_000_u32);
        let stop = match protection {
            Some(Protection::Price { stop_price }) => Some(dec(stop_price)?),
            Some(Protection::Distance { stop_distance_bps }) => Some(
                price
                    * if side == PositionSide::Long {
                        Decimal::ONE - Decimal::from(*stop_distance_bps) / Decimal::from(10000)
                    } else {
                        Decimal::ONE + Decimal::from(*stop_distance_bps) / Decimal::from(10000)
                    },
            ),
            None => None,
        }
        .map(|v| {
            if side == PositionSide::Long {
                (v / tick).floor() * tick
            } else {
                (v / tick).ceil() * tick
            }
        });
        if increase
            && stop.is_none_or(|s| {
                s <= Decimal::ZERO
                    || (side == PositionSide::Long && s >= price)
                    || (side == PositionSide::Short && s <= price)
            })
        {
            return Err("PROTECTION_INVALID".into());
        }
        let fee_rate = if increase {
            let fee = r.observer.fee_schedule(&rules).await.map_err(err)?;
            if !fee.exact_for_instrument || fee.reference_generation != rules.reference_generation {
                return Err("FEE_SOURCE_GENERATION_MISMATCH".into());
            }
            dec(&fee.taker_rate)?.abs().max(dec(&fee.maker_rate)?.abs())
        } else {
            Decimal::ZERO
        };
        // Includes both fee legs, configured slippage and a funding/gap reserve. Profit is never risk capacity.
        let per_contract = ct
            * ((price - stop.unwrap_or(price)).abs()
                + price * (fee_rate * Decimal::from(2) + slip + Decimal::new(1, 3)));
        let fx = if increase {
            let conversion = Self::usd_conversion(&snap)?;
            dec(&conversion.usd_per_usdt)?
        } else {
            Decimal::ONE
        };
        let entry_ceiling = if increase {
            order.max_entry_usd.as_deref().map(dec).transpose()?
        } else {
            None
        };
        let budgeted = entry_budget::size_order(
            sizing, increase, lot, minimum, ct, price, per_contract, fx, fee_rate,
            entry_ceiling,
        )?;
        let qty = budgeted.quantity;
        if let Some(v) = &rules.instrument.max_limit_size
            && qty > dec(v)?
        {
            return Err("ORDER_SIZE_VENUE_LIMIT".into());
        }
        let notional = budgeted.notional_usdt;
        if increase {
            if let Some(v) = &c.limits.max_notional_usdt
                && notional > dec(v)?
            {
                return Err("ORDER_NOTIONAL_LIMIT".into());
            }
            let leverage = dec(&r
                .observer
                .configured_leverage(instrument, MarginMode::Cross, side_name)
                .await
                .map_err(err)?)?;
            if leverage <= Decimal::ZERO
                || c.limits
                    .max_leverage
                    .as_ref()
                    .map(|v| dec(v))
                    .transpose()?
                    .is_some_and(|cap| leverage > cap)
            {
                return Err("LEVERAGE_LIMIT".into());
            }
            let available = dec(snap
                .balance
                .details
                .iter()
                .find(|b| b.currency == "USDT")
                .and_then(|b| {
                    b.available_equity
                        .as_deref()
                        .or(b.available_balance.as_deref())
                })
                .ok_or("AVAILABLE_MARGIN_UNAVAILABLE")?)?;
            if available < notional / leverage + qty * per_contract {
                return Err("INSUFFICIENT_MARGIN".into());
            }
        } else {
            let position = snap
                .positions
                .iter()
                .filter(|p| p.instrument_id == instrument && p.position_side == side_name)
                .try_fold(Decimal::ZERO, |sum, p| {
                    dec(&p.position).map(|q| sum + q.abs())
                })?;
            if qty > position {
                return Err("REDUCE_WOULD_INCREASE_OR_REVERSE".into());
            }
            let inst = instrument.to_owned();
            let owned_instance = instance.to_owned();
            let owned = r
                .execution
                .store
                .call(deadline(), move |s| {
                    Ok(s.active_lots(&inst)?
                        .into_iter()
                        .filter(|l| {
                            l.instance_id.as_deref() == Some(&owned_instance) && l.side == side_name
                        })
                        .fold(Decimal::ZERO, |n, l| n + l.remaining_qty))
                })
                .await
                .map_err(err)?;
            if qty > owned {
                return Err("REDUCE_EXCEEDS_INSTANCE_OWNED_LOTS".into());
            }
        }
        let loss = if increase {
            (qty * per_contract * fx)
                .round_dp_with_strategy(9, RoundingStrategy::ToPositiveInfinity)
        } else {
            Decimal::ZERO
        };
        if increase {
            self.financial_admission(r, c, instance, &snap, loss, record_risk_state)
                .await?;
        }
        if increase && qty * per_contract > dec(&c.limits.max_order_loss_usdt)? {
            return Err("MAX_ORDER_LOSS_EXCEEDED".into());
        }
        let instance_id = instance.to_owned();
        let instances = r
            .execution
            .store
            .call(deadline(), |s| s.instances())
            .await
            .map_err(err)?;
        if instance != "manual" {
            let i = instances
                .iter()
                .find(|i| i.instance_id == instance)
                .ok_or("STRATEGY_INSTANCE_NOT_FOUND")?;
            if increase && loss > i.budget {
                return Err("STRATEGY_BUDGET_EXCEEDED".into());
            }
        }
        if increase && r.private_ws.status().await.connection_generation != private_generation {
            return Err("PRIVATE_SOURCE_GENERATION_CHANGED".into());
        }
        let fresh = self
            .public_ws
            .risk_fresh_snapshot(
                instrument,
                now() as u64,
                crate::config::QUOTE_MAX_AGE_MS,
                crate::config::DERIVATIVE_CONTEXT_MAX_AGE_MS,
                Utc::now().to_rfc3339(),
            )
            .await
            .map_err(err)?;
        if fresh.generation != market.generation
            || fresh.market.reference_generation != rules.reference_generation
        {
            return Err("PUBLIC_SOURCE_GENERATION_CHANGED".into());
        }
        if !crate::config::evidence_fresh(received, now(), crate::config::ACCOUNT_MAX_AGE_MS) {
            return Err("ACCOUNT_EVIDENCE_EXPIRED_DURING_PLAN".into());
        }
        if Instant::now() >= until {
            return Err("COMMAND_EXPIRED".into());
        }
        let binding_id = instance_id.clone();
        let definition = r
            .execution
            .store
            .call(until, move |s| {
                if binding_id == "manual" {
                    if record_risk_state {
                        s.ensure_manual_instance(now())
                    } else {
                        s.manual_definition_preview()
                    }
                } else {
                    s.strategy_definition(&binding_id)
                }
            })
            .await
            .map_err(err)?;
        Ok(IntentPlan {
            strategy_definition_id: definition.definition_id,
            strategy_definition_version: definition.version,
            strategy_parameters_digest: definition.parameters_digest,
            operation: format!("{operation:?}").to_lowercase(),
            side: if buy { "buy" } else { "sell" }.into(),
            quantity: qty,
            limit_price: price,
            stop_price: stop,
            strategy_instance: instance_id,
            planned_loss_usd: loss,
            source_generation: market.generation as i64,
            position_side: side_name.into(),
            private_source_generation: private_generation as i64,
            contract_value: ct,
            inverse: false,
            settlement_currency: "USDT".into(),
        })
    }
    async fn financial_admission(
        &self,
        r: &AccountResources,
        control: &AccountControl,
        instance: &str,
        snapshot: &okx_observation::AccountSnapshot,
        candidate: Decimal,
        record_risk_state: bool,
    ) -> Result<(), String> {
        let rate = dec(&Self::usd_conversion(snapshot)?.usd_per_usdt)?;
        let daily = dec(control
            .limits
            .max_daily_realized_loss_usdt
            .as_deref()
            .ok_or("DAILY_LOSS_LIMIT_UNCONFIGURED")?)?
            * rate;
        let drawdown = dec(control
            .limits
            .max_drawdown_ratio
            .as_deref()
            .ok_or("DRAWDOWN_LIMIT_UNCONFIGURED")?)?;
        let instance = instance.to_owned();
        let utc_day = Utc::now().format("%Y-%m-%d").to_string();
        let decision = r
            .execution
            .store
            .call(deadline(), move |s| {
                let risk = s.financial_risk(now(), None)?;
                if !risk.nav_complete
                    || !crate::config::evidence_fresh(
                        risk.source_as_of,
                        now(),
                        crate::config::ACCOUNT_MAX_AGE_MS,
                    )
                {
                    return Err(okx_execution::StoreError::Invalid(
                        "FINANCIAL_SOURCE_INCOMPLETE_OR_STALE",
                    ));
                }
                // A preview may inspect historical stops, but must not resolve
                // even a stale latch: that is a write owned by admission.
                if record_risk_state {
                    for (id, _, reason, _) in s.stops()? {
                        if reason == "DAILY_LOSS_LIMIT" && !id.ends_with(&utc_day) {
                            s.resolve_stop(&id, "fresh_UTC_window_rollover")?;
                        }
                    }
                }
                if risk.daily_loss_usd >= daily {
                    let id = format!("daily:{utc_day}");
                    if record_risk_state {
                        s.latch_stop(&id, "account", "DAILY_LOSS_LIMIT", now())?;
                    }
                    return Ok(Some("DAILY_LOSS_LIMIT"));
                }
                if risk.daily_loss_usd + candidate > daily {
                    return Ok(Some("DAILY_LOSS_HEADROOM_INSUFFICIENT"));
                }
                if risk.drawdown_ratio >= drawdown {
                    if record_risk_state {
                        s.latch_stop("drawdown", "account", "DRAWDOWN_LIMIT", now())?;
                    }
                    return Ok(Some("DRAWDOWN_LIMIT"));
                }
                if s.instances()?.iter().any(|i| i.instance_id == instance) {
                    let own = s.financial_risk(now(), Some(&instance))?;
                    if !own.nav_complete {
                        return Err(okx_execution::StoreError::Invalid(
                            "INSTANCE_NAV_INCOMPLETE",
                        ));
                    }
                    if own.daily_loss_usd >= daily || own.drawdown_ratio >= drawdown {
                        if record_risk_state {
                            s.latch_stop(
                                &format!("instance:{instance}:risk"),
                                &instance,
                                "INSTANCE_LOSS_LIMIT",
                                now(),
                            )?;
                        }
                        return Ok(Some("INSTANCE_LOSS_LIMIT"));
                    }
                }
                Ok(None)
            })
            .await
            .map_err(err)?;
        if let Some(reason) = decision {
            return Err(reason.into());
        }
        Ok(())
    }
    pub(super) fn usd_conversion(
        snapshot: &okx_observation::AccountSnapshot,
    ) -> Result<UsdConversion, String> {
        let usdt = snapshot
            .balance
            .details
            .iter()
            .find(|d| d.currency == "USDT")
            .ok_or("USD_CONVERSION_SOURCE_UNAVAILABLE")?;
        let equity = dec(&usdt.equity)?;
        let usd = dec(usdt
            .equity_usd
            .as_deref()
            .ok_or("USD_CONVERSION_SOURCE_UNAVAILABLE")?)?;
        if equity <= Decimal::ZERO || usd <= Decimal::ZERO {
            return Err("USD_CONVERSION_SOURCE_INVALID".into());
        }
        let rate = (usd / equity).round_dp_with_strategy(9, RoundingStrategy::ToPositiveInfinity);
        Ok(UsdConversion {
            usd_per_usdt: exact(rate),
            source_digest: digest(&(
                snapshot.account_uid_fingerprint.clone(),
                snapshot.account_generation.clone(),
                usdt,
            ))?,
            observed_at_ms: chrono::DateTime::parse_from_rfc3339(&snapshot.source_received_at)
                .map_err(err)?
                .timestamp_millis() as u64,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn d(value: &str) -> Decimal {
        Decimal::from_str(value).unwrap()
    }

    #[test]
    fn auto_fok_buy_rounds_inside_slippage() {
        let price =
            quantize_executable_price(d("100.01"), None, 5, d("0.01"), true, "fok").unwrap();
        assert_eq!(price, d("100.06"));
        assert!(price <= d("100.01") * d("1.0005"));
    }

    #[test]
    fn auto_fok_sell_rounds_inside_slippage() {
        let price =
            quantize_executable_price(d("100.01"), None, 5, d("0.01"), false, "fok").unwrap();
        assert_eq!(price, d("99.96"));
        assert!(price >= d("100.01") * d("0.9995"));
    }

    #[test]
    fn exact_tick_bound_unchanged() {
        assert_eq!(
            quantize_executable_price(d("100"), None, 10, d("0.1"), true, "ioc").unwrap(),
            d("100.1")
        );
    }

    #[test]
    fn coarse_tick_no_marketable_price() {
        let result = quantize_executable_price(d("100.01"), None, 0, d("1"), true, "fok");
        assert_eq!(result.unwrap_err(), "NO_EXECUTABLE_PRICE_WITHIN_SLIPPAGE");
    }

    #[test]
    fn explicit_limit_never_worsened() {
        let price =
            quantize_executable_price(d("100.01"), Some(d("100.065")), 10, d("0.01"), true, "fok")
                .unwrap();
        assert_eq!(price, d("100.06"));
        assert!(price <= d("100.065"));
    }

    #[test]
    fn close_price_respects_limit() {
        // Closing a long sells; its lower limit must remain intact after rounding.
        let price = quantize_executable_price(
            d("100.01"),
            Some(d("99.959995")),
            5,
            d("0.01"),
            false,
            "ioc",
        )
        .unwrap();
        assert_eq!(price, d("99.96"));
        assert!(price >= d("99.959995"));
    }

    #[test]
    fn post_only_does_not_cross() {
        let result = quantize_executable_price(
            d("100.01"),
            Some(d("100.015")),
            10,
            d("0.01"),
            true,
            "post_only",
        );
        assert_eq!(result.unwrap_err(), "POST_ONLY_WOULD_CROSS");
    }
}
