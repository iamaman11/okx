# #580. Исходная карта разделения компонентов

Исходный `main`: `ce3238d09ff83a8a2ff19aebde060c8a93108a09` (19 Cargo packages). Первый автономный кандидат PR #581: `36add9b7a6793be24d097cacf66376a5f4f7e309` (21 package после выделения `okx-engine-domain` и `okx-research-domain`/`okx-research-protocol` и других переносов). Это **исходное и рабочее CODE-свидетельство**, не отметка о принятом установленном релизе. Полный перечень исходников закреплён в #580 §7; для приёмки обновлять точный candidate SHA/tree и Cargo closure на финальном HEAD.

| Исходный пакет | Владелец | Действие |
| --- | --- | --- |
| okx-trading-engine, okx-trade-execution, okx-engine-domain | Engine | Один торговый runtime, SQLite writer и RiskBook. Engine вычисляет решения установленных живых стратегий. Удалить оставшуюся тестовую ResearchClient/IPC оснастку после переноса обязательных свидетелей. |
| okx-api, okx-ws, okx-market-runtime, okx-observation | Engine | Сохранить подписанные и публичные источники, которые использует Engine. Research использует собственный публичный клиент. Проверить, что Research tests также не подтягивают эти пакеты. |
| okx-research-service, okx-research, okx-analysis, okx-strategy-core, okx-research-domain, okx-research-protocol | Research | Исследовательские расчёты, задания, артефакты, публичные источники, строго типизированные Research Edge команды. Удалить исторический child IPC после переноса его свидетельств. |
| okx-host-controller, okx-host-launcher, okx-host-protocol | CONTROL | Сохранить установленный жизненный цикл и артефакты. Подключить один Research supervisor. |
| okx-bridge-mcp | Edge | Сохранить штатный GitHub mailbox вход, определить назначение каждого маршрута после удаления Engine fallback. |
| okx-ipc-contracts | Engine/CONTROL общий wire | Убрать Research-протокол из Engine normal/build closure. Временная `legacy-research-test` feature и dev-only тестовая связь допустимы только до миграции T06–T10, при явном S1 `PARTIAL`; строгая dev-граница обязательна для полного PASS. |
| okx-github | Библиотека GitHub | Сохранить для действующих потребителей. |
| okx-cli, okx-native-finance-check | Инструменты | Сохранить read-only CLI и финансовые проверки. |

| Вход | Целевой владелец и production-путь | Существующий эталон и проверка перехода |
| --- | --- | --- |
| Trade, live strategies, account summary, фактический account portfolio | Engine: `query::dispatch`, `trade_service`, `FinanceDispatch` и один финансовый SQLite | Native financial collector; стратегия CRUD/claim/replay, stop fence, потерянный ACK и reopen, защита и сокращение риска |
| Research backtest/grid/status/result и исследовательское заключение | Research: `edge_runtime`, `AnalyticsService`, собственный каталог/артефакты | Rust job/decision tests, MCP positive/negative fixtures, повтор и чтение прежнего артефакта. Engine намеренно отказывает `RESEARCH_OWNER_REQUIRED` |
| Market overview, find instruments, microstructure и market universe | Research: публичный источник и прямой Edge маршрут | Сравнить с нынешним Engine query на одинаковых фактах. Самостоятельный Research пока сообщает причину недоступности для этих живых представлений или требует явный список инструментов |
| Портфельная симуляция | Research: portfolio_scenario | Контракт гипотетического портфеля на заданных данных и маршрутизация через MCP |
| Обновить / откатить компонент | CONTROL: штатные команды и проверка артефакта | Windows lifecycle, SHA, PID, readback и отказ совместимости |
| Подключить / авторизовать / объяснить недоступность | Edge: MCP `runtime_session` до типизированного Engine или Research | MCP fixtures, реальные WSS-проверки и отказ отсутствующего владельца без fallback через Engine |

### Что удалить при проверенном переходе

- Engine runtime и query больше не запускают Research, не проксируют его задачи и не читают его хранилище. Удалить ResearchClient, диагностику дочернего процесса и устаревшие маршруты, когда их заменяющие операции пройдут приёмку.
- Research перестаёт принимать Engine-child команды и сигнальные запросы торговли после перехода живых стратегий на Engine. Сохранить исследовательские БД, ключи заданий и артефакты.
- Edge удаляет legacy_research и маршрутизацию исследовательского запроса через Engine. Отказ устаревшему финансовому RPC продолжают проверять.
- Финансовый сборщик сохраняет действующие свойства. Местоположение исходника и идентификаторы тестов сверяются при каждом переносе.

### Воспроизводимые проверки

Из корня рабочего дерева с подключённой штатной средой Rust/MSVC:

```powershell
git ls-tree -r --name-only HEAD
cargo metadata --locked --format-version 1 --filter-platform x86_64-pc-windows-msvc
cargo tree --locked -p okx-trading-engine --target x86_64-pc-windows-msvc --edges normal,build,dev
cargo tree --locked -p okx-research-service --target x86_64-pc-windows-msvc --edges normal,build,dev
python scripts/check_architecture.py --self-test
python scripts/check_architecture.py
python scripts/check_architecture.py --strict-component-test-boundary
```

На промежуточной базе `2435cbf` Engine не тянул реализацию Research, но **всё ещё компилировал `okx-research-protocol` через `okx-ipc-contracts`**. Это недостаточно для независимого owner-only CI: изменение Research-протокола могло заново компилировать торговый бинарник. Исследовательские dev-зависимости на `okx-api`/`okx-market-runtime` в этом срезе уже были устранены. Следующий кандидат выводит Research-протокол из Engine normal/build, но временно сохраняет `legacy-research-test` для ранее обязательных T06–T10. Это **S1 PARTIAL**: обычный guard запрещает normal/build нарушение и показывает существующий dev-долг, а `--strict-component-test-boundary` обязан стать зелёным только после переноса этих свидетелей.

### Исходные замеры

- [CI 45473110288](https://github.com/iamaman11/okx/actions/runs/45473110288), кандидат #578. Windows acceptance job 381 с. Проверка финансов 190 с, восстановление Rust-кэша 31 с и сборка Research для process-контрактов 35 с. Release job параллельно 278 с, его шаг сборки четырёх EXE 232 с.
- Локальная холодная сборка четырёх Windows EXE на 2435cbf, Rust 1.100.0, MSVC 14.44.35207 и SDK 10.0.26100.0. Cargo release завершился за 426 с. Это отдельный замер времени сборки.
- Исходный профиль нагрузки #518/T10 — тест mcp_http_durable_object_wss_native_finance_stays_readable_under_heavy_research в accounts_native_engine_research_contract_tests.rs. Он выполняет два CPU grid в дочернем Worker, затем повторяет проверку с отключённым Worker. В отчёте T10_MCP_NATIVE_LOAD_JSON для каждого нормируемого запроса 100 наблюдений с p95 ≤100 мс. Для S2 воспроизвести тот же рынок и финансовые результаты при запуске Research через CONTROL. Записать версию runner и измеренные CPU/RAM лимиты.


### Проверки первого среза S1

- Отдельные Windows-графы Cargo с normal/build должны подтвердить, что Research не собирает Engine market/finance packages, а production Engine не собирает ни реализацию Research, ни `okx-research-domain`/`okx-research-protocol`. Дополнительно сохранить точный test-only manifest T06–T10 и выполнить `--strict-component-test-boundary`: до замены этих свидетелей его красный результат — честный незакрытый долг, а не причина ослаблять финансовые тесты.
- `python scripts/check_architecture.py`, `--self-test` и `--k10-purity` прошли. Контрольные графы подставляют запрещённые normal/build/dev зависимости в обоих направлениях.
- `cargo test --offline --locked -p okx-trade-execution --lib risk::` — 63 PASS. Шесть свидетельств финансового манифеста сохранили прежние имена после перемещения в торговый пакет.
- `cargo test --offline --locked -p okx-trading-engine --lib strategy` — 10 PASS.
- `cargo test --offline --locked -p okx-research-service --lib -- --test-threads 2` — 181 PASS, один ignored. Все перенесённые регрессии источника прошли на standalone-клиенте Research.
- `node scripts/test-ci-component-impact.cjs` — PASS. Файл чистых финансовых правил относится к Engine, изменения действительно общих моделей сохраняют общий маршрут.
