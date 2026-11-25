# Карта разделения Rust-крейтов и CI

Карта описывает два переноса в ветке `codex/537-runtime-feedback`.
Исходное дерево — `0e037d8d`. Переносы внесены в `4af52eb4`, форматирование
нового протокола — в `b8cd06e8`.
Временные ряды перешли в существующий `okx-strategy-core`, а контракты
управления Windows-хостом — в новый `okx-host-protocol`.
После переноса правки Rust-кода только в этих областях выбирают маршрут
Research или Host. Торговый Engine из этих маршрутов исключён.

## 1. Граф Cargo на Windows

| Компонент | Пакеты, определяющие границу |
| --- | --- |
| Engine · `okx-agent.exe` | `okx-trade-execution`, `okx-engine-domain`, `okx-ipc-contracts` и торговые адаптеры |
| Research · `okx-research-service.exe` | `okx-research`, `okx-analysis`, `okx-strategy-core`, `okx-research-protocol`, `okx-research-domain` |
| CONTROL · `okx-host-control.exe` | `okx-host-controller`, `okx-host-launcher`, `okx-host-protocol`, `okx-ipc-contracts`, `okx-github` |
| Launcher · `okx-host-launcher.exe` | Внешние зависимости |
| Edge | `cloudflare/okx-mcp` и Rust MCP Bridge |

Проверить каждый бинарник можно командой `cargo tree --locked -p <package>
--target x86_64-pc-windows-msvc -e normal,build,dev`. Research собирается
со своими моделями рынка и аналитическими входами. Историческая передача
Research-протокола через `okx-ipc-contracts` устранена в PR #595,
принятом в `main` `ef9060bd`. Engine, CONTROL, Launcher и MCP Bridge не
должны получать `okx-research-protocol` или `okx-research-domain` ни через
обычные, ни через dev/build зависимости. Это проверяет
`scripts/check_architecture.py --strict-component-test-boundary`.

## 2. Перенос A — временные ряды в Strategy Core

| До переноса | После переноса |
| --- | --- |
| `crates/okx-domain-core/src/time_series.rs` | [`crates/okx-strategy-core/src/time_series.rs`](../crates/okx-strategy-core/src/time_series.rs), включая тесты |
| Публичный модуль Domain Core | [Публичный модуль Strategy Core](../crates/okx-strategy-core/src/lib.rs) |
| `okx_domain_core::time_series::ClosedRange` в Query Plan | `crate::time_series::ClosedRange` в [`query_plan.rs`](../crates/okx-strategy-core/src/query_plan.rs) |
| `okx_analysis::time_series` через реэкспорт из Domain Core | [Тот же API Analysis](../crates/okx-analysis/src/time_series.rs) через реэкспорт из Strategy Core |
| Research Service обращается к Analysis из `analytics/{mod,market,planner}.rs` | Существующие импорты и потребители Analysis сохранены |

Перенесены `ClosedRange`, `EventGrid`, `BarCoverage`, `TimedValue`,
`AlignmentCounts` и функции выравнивания. В модуле сохранены полуоткрытые
интервалы, точное покрытие сетки свечей, диагностика пропусков и проверка
`expected_bars` при десериализации. Отдельный крейт для этих функций
избыточен. Реэкспорт из прежнего Domain Core потребовал бы зависимости
`okx-domain-core → okx-strategy-core` и создал бы цикл.

В [`check_p0.py`](../scripts/check_p0.py) тест
`time_series::tests::c13_missing_internal_candle_not_healed_by_duplicate_or_unclosed`
обязателен в том же запуске Strategy Core, что и тест валидации Query Plan.
Ветка `kernel` отдельно запускает тесты Domain Core. Windows-маршрут
Research проверяет Strategy Core, Analysis, Research и Research Service
до сборки и упаковки.

## 3. Перенос B — контракты управления Windows-хостом

| Что | Владелец и потребители после переноса |
| --- | --- |
| `HostControlRequest`, `HostControlOperation`, `HostControlResult`, `HostControlStatus`, `HostControlFailure`, константы двух схем | [`okx-host-protocol/src/lib.rs`](../crates/okx-host-protocol/src/lib.rs). Прямые потребители — Host Controller и Rust MCP Bridge |
| `validate_host_control_request_id`, `validate_account_wire`, валидация вариантов `HostControlOperation` | В новом крейте вместе с типами |
| `validate_request_id`, `validate_sha256`, `validate_source_tree`, `accounts::valid_installation`, `ProtocolError` | В [`okx-ipc-contracts`](../crates/okx-ipc-contracts/src/lib.rs). Первые три функции открыты для использования из нового крейта |
| `NativeChildRequest`, `NativeChildReply`, `NativeChildOperation`, префикс и лимит кадра | Общий IPC. [Host Controller](../crates/okx-host-controller/src/lifecycle_executor.rs) посылает запрос на остановку, [Engine](../crates/okx-trading-engine/src/runtime.rs) принимает его и подтверждает по унаследованным каналам. Launcher предоставляет запуск процесса и каналы |
| `AgentRequest` / `AgentResponse`, `research_ipc`, `host_state_proto`, `account_catalog_proto`, рыночные DTO | Общий IPC. Например, `account_catalog_proto` используют и Controller, и Engine |

Новый крейт зависит от `okx-ipc-contracts`, `serde` с `derive` и
`serde_json`. Эта связь позволяет использовать общие проверки. Стрелки
из IPC в `okx-host-protocol` нет, поэтому изменения в нём остаются за
пределами Windows-графа Engine и Research. Архитектурный
[`check_architecture.py`](../scripts/check_architecture.py) проверяет
прямые зависимости, включая тестовые, и выполняет отдельные транзитивные
проверки производственных границ.

Импорты переключены в Host Controller в `runtime.rs`, `control_ack.rs`,
`lifecycle_executor.rs` и тестах `controller_update.rs` и `root_migration.rs`.
Локальный псевдоним `okx_host_controller::HostControlResult<T>` по-прежнему
обозначает результат функций контроллера. В
[Rust MCP Bridge](../crates/okx-bridge-mcp/src/lib.rs) переключены типы для
`control_status_request` и `wait_for_control_result`; Agent DTO и
криптография приходят из общего IPC.

Схемы `okx.windows.control/v1` и `okx.windows.control.result/v1` сохраняют
имена полей, `operation.type` в `snake_case`, `deny_unknown_fields` и
статусы `PASS` / `FAIL`. Тесты нового крейта фиксируют JSON запроса,
схему и статус ответа, отклонение лишних полей и неверных ID, а также
два различных точных SHA-256 для Research rollback. Проверки автора,
корреляция по `request_id` и публикация отказов остаются у потребителей.
[`check-research-handoff.py`](../scripts/check-research-handoff.py)
продолжает читать квитанции того же формата.

RiskBook, финансовый SQLite/WAL, допуск и исполнение заявок и восстановление
остаются у Engine и Trade Execution. Общие `fee_model`,
`observation_model`, `risk` и `strategy_contract` остаются в Domain Core.
Например, общий IPC использует `CompositionRequest` в `analytics.rs`.
Работа Engine и Research как процессов описана в
[карте границы Research](research-boundaries.md).

## 4. Какой CI вызывается

| Изменение Ready PR | Маршрут и результат |
| --- | --- |
| Только `crates/okx-strategy-core/src/time_series.rs`, возможно с документацией | `RESEARCH_OWNER_ONLY` → `research-service-tests-windows`. Engine/Execution отсутствуют в Cargo-запусках. `okx-agent.exe`, `okx-host-control.exe` и `okx-host-launcher.exe` берутся из принятой Windows-базы |
| Только Rust-код `crates/okx-host-protocol/src/` или `crates/okx-host-controller/src/` | `HOST_WINDOWS_OWNER_AND_CONSUMER`, scope `host-controller` → `host-controller-launcher-checks`. Clippy и тесты охватывают новый протокол, Launcher, Controller и Rust Bridge; артефакт проверки содержит хэши двух host-бинарников |
| Только Rust-код `crates/okx-host-launcher/src/` | Тот же Windows job и набор проверок, scope `host-launcher` |
| Только Rust-код Research и Host в одном PR | `research-host`: обе проверки владельцев параллельно. Свежие бинарники Research и Host сохраняются с SHA и деревом исходников. Принятый Engine сверяется с опубликованной базой и Windows Cargo-графом, включая тесты |
| Только документация или только Cloudflare MCP | Существующий маршрут соответствующего компонента, без запуска Windows-сборки |
| Общий IPC, Domain Core, Engine/Execution, корневой Cargo, манифест нового Host-протокола, CI или неполный diff | Полный маршрут с финансовыми проверками. Для разрешённых манифестов Research или Host Controller/Launcher есть отдельный допуск по совпадению Windows Cargo closures |

Единый [workflow](../.github/workflows/ci.yml) сохраняет обязательные
`classify`, `linux-core` и `windows-native`. Узкие Rust-маршруты задают
явные пакеты для `cargo test`, `cargo clippy` и `cargo build`.
[`check_ci_native_scope.py`](../scripts/check_ci_native_scope.py)
проверяет полный diff. Сомнительный путь, переименование или неполные
данные оставляют полный маршрут.
[`ci-cargo-closure-proof.cjs`](../scripts/ci-cargo-closure-proof.cjs)
учитывает Windows targets, виды зависимостей и Cargo features, включая
граф тестов. Инвентарь [`ci-component-impact.cjs`](../scripts/ci-component-impact.cjs)
показывает влияние на релизные Windows-бинарники для диагностики.

[`research-release-scope.cjs`](../scripts/research-release-scope.cjs)
проверяет дерево кандидата относительно принятой Windows-базы. Путь
`okx-host-protocol` теперь защищён как транзитивная зависимость Controller.
Если код или зависимости Host поменялись после принятой базы, исследовательская
сборка сама по себе не даёт зелёный Ready. Host-only job подтверждает
сборку Host. Для публикации нового Native base нужен полный маршрут.
Смешанный маршрут подтверждает происхождение Engine и свежих бинарников
компонентов отдельными свидетельствами. Установочные операции с пакетом
по-прежнему используют собственную полную проверку совместимости релиза.

## 5. Как закрыть приёмку

1. **Принять дерево с переносами.** В первом PR затронуты общий IPC,
   Domain Core, корневой манифест и workflow, поэтому выполняется полный
   маршрут. Для следующих проверок нужна принятая Windows-база с кодом
   после переноса.
2. **Снять два независимых Windows-прогона.** От принятого дерева
   проверить отдельные Ready PR с одиночной правкой
   `okx-strategy-core/src/time_series.rs` и `okx-host-protocol/src/lib.rs`.
   Зафиксировать SHA и scope, успех обязательных проверок, пропуск
   `trading-engine-acceptance-windows`,
   всех исполнителей `trading-engine-regressions-windows (suite)` и
   `trading-engine-build-windows`. В Cargo-логах проверить отсутствие
   Engine/Execution. Для Research сравнить SHA-256 `okx-agent.exe` с
   принятой базой. Отрицательный пример с общим IPC должен получить full.
3. **Проверить предел 120 секунд.** Считать время от создания Ready-run
   до завершения обязательных проверок и появления нужного артефакта.
   Отдельно записать очередь runner, кэш, компиляцию, тесты, проверку базы
   и упаковку. Нужны серии холодных и прогретых запусков Research, Host и
   Engine. Пропуск Engine в двух узких маршрутах следует из графа и условий
   workflow; предел 120 секунд требует замеров. Для полного маршрута
   отдельно разбирать длительность финансовых фикстур. Бизнес-сроки
   можно привязать к явному тестовому времени после инвентаризации.
   Проверки реальной остановки процесса и восстановления SQLite остаются
   на Windows.

Установленные имена EXE и служб, форматы хранения и JSON-схемы
сохранены. При полном откате сначала обращается коммит форматирования
`b8cd06e8`, затем переносы `4af52eb4`. Раздельный откат потребует снять
изменения исходников, Cargo-графа и маршрутов CI соответствующей границы
одним согласованным изменением.
