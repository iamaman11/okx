# #580. Исходная карта разделения компонентов

Исходный `main`: `ce3238d09ff83a8a2ff19aebde060c8a93108a09` (19 Cargo packages). Первый автономный кандидат PR #581: `36add9b7a6793be24d097cacf66376a5f4f7e309` (21 package после выделения `okx-engine-domain` и `okx-research-domain`/`okx-research-protocol` и других переносов). Это **исходное и рабочее CODE-свидетельство**, не отметка о принятом установленном релизе. Полный перечень исходников закреплён в #580 §7; для приёмки обновлять точный candidate SHA/tree и Cargo closure на финальном HEAD.

### Принятая граница S1 и открытая физическая приёмка S2–S5

Актуальный контрольный `main` —
[`ef9060bd`](https://github.com/iamaman11/okx/commit/ef9060bd30a00225ea78050103a1768aa98217c5),
source tree `ed0884d1402565378a7c61265700eeeb42be1ad0` (25.11.2026).
Это исходная точка **нового** среза, а не автоматическая приёмка любого
последующего SHA.

| Граница | Доказано на `ef9060bd` | Что ещё принять |
| --- | --- | --- |
| Engine + финансовый SQLite/RiskBook | Собственный live-strategy turn; Engine/CONTROL/Launcher/Bridge не имеют Research protocol/domain в Windows normal/build/dev closures, включая test-only клиент. Строгий architecture guard обязателен | Engine без установленного Research, реальный Windows PID/Job и сохранность торговли при update Research |
| Research process и данные | Research-owned `edge_runtime`, публичный источник, самостоятельные 10 Windows IPC/process tests, causal/replay и собственный catalog | Чистая Research-only установка, прежние job/result, cancel, reboot/rollback, изоляция от финансовых ресурсов |
| CONTROL/Launcher | Версионированное исполнение Windows и manifest/readback; самостоятельный supervisor в коде | Физический LPAC/AppContainer запуск, точные ACL/Low IL и CredMan/finance DENY, действующий один владелец запуска |
| Edge | Маршрутизация Research WSS и отдельный `RESEARCH_RUNTIME_OFFLINE` для публичных операций в `main` (`d0703b6a`) | Авторизованный Edge↔Research WSS на установленной системе и отсутствие Engine fallback при отказе |
| CI и публикация | Ready [#45684845788](https://github.com/iamaman11/okx/actions/runs/45684845788) успешно проверил четыре Windows regression suites, 62/107 финансовых свидетельств и обе T10 нагрузки; `main` [#45685939326](https://github.com/iamaman11/okx/actions/runs/45685939326) повторно не компилировал Engine | Четыре owner-only diff + отрицательные контроли, 12 прогретых измерений по #580, каждый ≤120 с; полный Ready #595 занял 5:55 |

В PR #595 устранены прежние Engine `ResearchClient` тесты, старые
`legacy-research-test` features и тестовые Cargo связи с Research.
T06–T12 перенесены в Research-owned IPC-проверки, а обе настоящие
финансовые T10 нагрузки используют отдельный non-shipping Research fixture.
**Нельзя возвращать `S1 DEV DEBT` в документы как актуальный статус**;
одновременно нельзя засчитывать успешную статическую границу как физическую
изоляцию Windows. Старые замеры ниже — исторический baseline.

| Исходный пакет | Владелец | Действие |
| --- | --- | --- |
| okx-trading-engine, okx-trade-execution, okx-engine-domain | Engine | Один торговый runtime, SQLite writer и RiskBook. Engine вычисляет решения установленных живых стратегий. Тестовая оснастка ResearchClient уже удалена в #595. |
| okx-api, okx-ws, okx-market-runtime, okx-observation | Engine | Сохранить подписанные и публичные источники, которые использует Engine. Research использует собственный публичный клиент. Проверить, что Research tests также не подтягивают эти пакеты. |
| okx-research-service, okx-research, okx-analysis, okx-strategy-core, okx-research-domain, okx-research-protocol | Research | Исследовательские расчёты, задания, артефакты, публичные источники, строго типизированные Research Edge команды. Удалить исторический child IPC после переноса его свидетельств. |
| okx-host-controller, okx-host-launcher, okx-host-protocol | CONTROL | Сохранить установленный жизненный цикл и артефакты. Подключить один Research supervisor. |
| okx-bridge-mcp | Edge | Сохранить штатный GitHub mailbox вход, определить назначение каждого маршрута после удаления Engine fallback. |
| okx-ipc-contracts | Engine/CONTROL общий wire | Research-протокол уже выведен из normal/build/dev защищённых closures. Не вводить обратную тестовую зависимость; строгий dev guard обязателен. |
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

- Engine runtime и query больше не запускают Research, не проксируют его задачи и не читают его хранилище. Старый Engine ResearchClient удалён; не возвращать его ради совместимости тестовых fixtures.
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

На промежуточной базе `2435cbf` Engine ещё компилировал
`okx-research-protocol` через общий IPC. Это **исторический дефект**, устранённый
в #595. Сравнивать с ним актуальную границу следует по точному
Windows Cargo normal/build/dev resolver и успешному
`--strict-component-test-boundary`, а не по старому описанию S1 PARTIAL.

### Исходные замеры

- [CI 45473110288](https://github.com/iamaman11/okx/actions/runs/45473110288), кандидат #578. Windows acceptance job 381 с. Проверка финансов 190 с, восстановление Rust-кэша 31 с и сборка Research для process-контрактов 35 с. Release job параллельно 278 с, его шаг сборки четырёх EXE 232 с.
- Локальная холодная сборка четырёх Windows EXE на 2435cbf, Rust 1.100.0, MSVC 14.44.35207 и SDK 10.0.26100.0. Cargo release завершился за 426 с. Это отдельный замер времени сборки.
- Исходный профиль нагрузки #518/T10 — тест mcp_http_durable_object_wss_native_finance_stays_readable_under_heavy_research в accounts_native_engine_research_contract_tests.rs. Он выполняет два CPU grid в дочернем Worker, затем повторяет проверку с отключённым Worker. В отчёте T10_MCP_NATIVE_LOAD_JSON для каждого нормируемого запроса 100 наблюдений с p95 ≤100 мс. Для S2 воспроизвести тот же рынок и финансовые результаты при запуске Research через CONTROL. Записать версию runner и измеренные CPU/RAM лимиты.


### Проверки первого среза S1

- Отдельные Windows-графы Cargo с normal/build/dev должны подтвердить, что Research не собирает Engine market/finance packages, а production Engine не собирает ни реализацию Research, ни `okx-research-domain`/`okx-research-protocol`. Сверить перенесённые T06–T12 Research IPC tests и выполнить `--strict-component-test-boundary`: красный результат теперь означает **регрессию**, а не допустимый прежний долг.
- `python scripts/check_architecture.py`, `--self-test` и `--k10-purity` прошли. Контрольные графы подставляют запрещённые normal/build/dev зависимости в обоих направлениях.
- `cargo test --offline --locked -p okx-trade-execution --lib risk::` — 63 PASS. Шесть свидетельств финансового манифеста сохранили прежние имена после перемещения в торговый пакет.
- `cargo test --offline --locked -p okx-trading-engine --lib strategy` — 10 PASS.
- `cargo test --offline --locked -p okx-research-service --lib -- --test-threads 2` — 181 PASS, один ignored. Все перенесённые регрессии источника прошли на standalone-клиенте Research.
- `node scripts/test-ci-component-impact.cjs` — PASS. Файл чистых финансовых правил относится к Engine, изменения действительно общих моделей сохраняют общий маршрут.
