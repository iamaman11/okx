# #580. Исходная карта разделения компонентов

Исходный main: ce3238d09ff83a8a2ff19aebde060c8a93108a09. Рабочая база PR #581: 2435cbfeb5e82b5ca53b117260aa652b7dc5d28b. Полный перечень исходников закреплён в #580 §7. Здесь указаны границы, которые исполнитель проверяет при переносе файлов.

| Исходный пакет | Владелец | Действие |
| --- | --- | --- |
| okx-trading-engine, okx-trade-execution | Engine | Сохранить один торговый runtime, SQLite writer и правила риска. Убрать ResearchClient и старый research dispatch после готовности независимых маршрутов. |
| okx-api, okx-ws, okx-market-runtime, okx-observation | Engine | Сохранить подписанные и публичные источники, которые использует Engine. Research использует собственный публичный клиент. Проверить, что Research tests также не подтягивают эти пакеты. |
| okx-research-service, okx-research, okx-analysis, okx-strategy-core | Research | Сохранить расчёты, задания, артефакты и публичный источник. Удалить child IPC после перехода к собственным командам Edge. |
| okx-host-controller, okx-host-launcher, okx-host-protocol | CONTROL | Сохранить установленный жизненный цикл и артефакты. Подключить один Research supervisor. |
| okx-bridge-mcp | Edge | Сохранить штатный GitHub mailbox вход, определить назначение каждого маршрута после удаления Engine fallback. |
| okx-domain-core | Контракты / Engine / Research | Перенести финансовые правила к торговому SQL-владельцу. У оставшихся типов проверить фактических потребителей. |
| okx-ipc-contracts | Контракты Engine, Research и управления хостом | Разобрать торговые и исследовательские поля, Research child IPC и общий envelope вместе с переносом маршрутов. До этого пакет входит в оба графа. |
| okx-github | Библиотека GitHub | Сохранить для действующих потребителей. |
| okx-cli, okx-native-finance-check | Инструменты | Сохранить read-only CLI и финансовые проверки. |

| Вход | Целевой владелец и production-путь | Существующий эталон и проверка перехода |
| --- | --- | --- |
| Trade, strategies, account summary, portfolio | Engine: query dispatch, TradeService, portfolio и финансовый SQL | Native financial collector, стратегия CRUD/claim/replay, потерянный ACK и reopen, защита и сокращение риска |
| Research backtest/grid/status/result и решения | Research: edge_runtime, AnalyticsService и исследовательский каталог | Rust job/decision tests, MCP positive/negative fixtures, повтор и чтение прежнего артефакта |
| Market overview, find instruments, microstructure и market universe | Research: публичный источник и прямой Edge маршрут | Сравнить с нынешним Engine query на одинаковых фактах. Самостоятельный Research пока сообщает причину недоступности для этих живых представлений или требует явный список инструментов |
| Портфельная симуляция | Research: portfolio_scenario | Контракт гипотетического портфеля на заданных данных и маршрутизация через MCP |
| Обновить / откатить компонент | CONTROL: штатные команды и проверка артефакта | Windows lifecycle, SHA, PID, readback и отказ совместимости |
| Подключить / авторизовать / объяснить недоступность | Edge: MCP/runtime_session до типизированного владельца | Worker fixtures, реальные WSS-проверки и ответ каждого недоступного владельца |

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
```

На рабочей базе 2435cbf Engine не тянет реализации Research. Research через dev-зависимости тянет okx-api и okx-market-runtime. Оба компонента используют okx-ipc-contracts и okx-domain-core. Для разрешения независимой сборки фактический вывод Cargo должен подтвердить отсутствие чужой реализации.

### Исходные замеры

- [CI 45473110288](https://github.com/iamaman11/okx/actions/runs/45473110288), кандидат #578. Windows acceptance job 381 с. Проверка финансов 190 с, восстановление Rust-кэша 31 с и сборка Research для process-контрактов 35 с. Release job параллельно 278 с, его шаг сборки четырёх EXE 232 с.
- Локальная холодная сборка четырёх Windows EXE на 2435cbf, Rust 1.100.0, MSVC 14.44.35207 и SDK 10.0.26100.0. Cargo release завершился за 426 с. Это отдельный замер времени сборки.
- Исходный профиль нагрузки #518/T10 — тест mcp_http_durable_object_wss_native_finance_stays_readable_under_heavy_research в accounts_native_engine_research_contract_tests.rs. Он выполняет два CPU grid в дочернем Worker, затем повторяет проверку с отключённым Worker. В отчёте T10_MCP_NATIVE_LOAD_JSON для каждого нормируемого запроса 100 наблюдений с p95 ≤100 мс. Для S2 воспроизвести тот же рынок и финансовые результаты при запуске Research через CONTROL. Записать версию runner и измеренные CPU/RAM лимиты.


### Проверки первого среза S1

- Отдельные Windows-графы Cargo с normal/build/dev подтверждают, что Research больше не собирает okx-api и okx-market-runtime. Engine не собирает пакеты Research. Обе цепочки пока используют общий протокол и часть моделей в okx-domain-core.
- `python scripts/check_architecture.py`, `--self-test` и `--k10-purity` прошли. Контрольные графы подставляют запрещённые normal/build/dev зависимости в обоих направлениях.
- `cargo test --offline --locked -p okx-trade-execution --lib risk::` — 63 PASS. Шесть свидетельств финансового манифеста сохранили прежние имена после перемещения в торговый пакет.
- `cargo test --offline --locked -p okx-trading-engine --lib strategy` — 10 PASS.
- `cargo test --offline --locked -p okx-research-service --lib -- --test-threads 2` — 181 PASS, один ignored. Все перенесённые регрессии источника прошли на standalone-клиенте Research.
- `node scripts/test-ci-component-impact.cjs` — PASS. Файл чистых финансовых правил относится к Engine, изменения действительно общих моделей сохраняют общий маршрут.
