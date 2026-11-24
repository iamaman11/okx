# Границы файлов vNext

Эта карта определяет назначение исходников в ветке `vnext`. У каждого файла
один ответственный владелец. Для переходного смешанного файла владелец отвечает
за разбор функций по указанным назначениям.

- [legacy-files.tsv](legacy-files.tsv) содержит по одной строке на каждый файл исходного дерева. Поля — путь, владелец перехода, действие и целевое размещение.
- Файлы под `vnext/{engine,research,control,edge}/` принадлежат владельцу своего корня. Тесты, схемы, fixtures, lockfile и настройки сборки лежат в том же корне.
- `vnext/integration/` отведён межкомпонентным проверкам. Он работает с опубликованным контрактом или артефактом владельца. Приложения не включают его в свои сборки.
- В корне репозитория остаются общая навигация, карта миграции и CI. Прежние workflows и скрипты маршрутизации не переносятся в производственные сборочные корни.

| Действие | Смысл |
| --- | --- |
| `move` | Перенести поведение и его тесты в указанный корень. Целевой путь — закреплённое место, а не отметка о завершении. |
| `split` | Разобрать файл по таблице ниже и переносить функции и типы по владельцам. |
| `oracle` | Перенести сценарий и критерий проверки в тесты владельца или межкомпонентный набор. |
| `retire` | Действующий переходный адаптер. Вывод после миграции последнего потребителя и проверки совместимости. |
| `archive` | Исторический документ, отчёт или прежняя оркестрация. Используется как материал для сравнения. |
| `keep` | Файл инфраструктуры репозитория на своём месте. |

## Рабочие корни

| Корень | Владеет | Входы и предел полномочий |
| --- | --- | --- |
| `vnext/engine` | Денежные типы и правила в `crates/okx-engine-risk`. Транзакции, резервы, RiskBook, состояние ордера, fill/bill/ledger, сверка и SQLite/WAL в `crates/okx-trade-execution`. Торговые команды и adapters биржи в целевом `crates/okx-trading-engine`. | Только Engine владеет приватными ключами OKX, `finance.sqlite` и правом отправки ордера. Публичные факты для риска он получает и проверяет в своём адаптере. |
| `vnext/research` | План, численные операторы и causal stream в `crates/okx-strategy-core`. Запуск replay в `src/application/replay.rs`, публичный HTTP в `src/adapters/okx_candles.rs`, CLI в `src/main.rs`. Jobs, каталог, источники и артефакты при переносе — в `crates/okx-research-service`. | Получает публичные данные или импортированные snapshots, выдаёт воспроизводимый результат. Хранит собственный исследовательский каталог. |
| `vnext/control` | Launcher, переключение версий, rollback и состояние установки. В целевом `crates/okx-host-controller` — lifecycle и проверка артефактов. `policy/` — операторская политика после подтверждённого переноса из Worker. | Control управляет версиями, процессами и политикой доступа. Фактическая проверка риска и отправка остаются в Engine. |
| `vnext/edge` | MCP, OAuth/KV, проверка доступа, маршрутизация, транспортные очереди и состояние доставки в Durable Object. | Передаёт команды владельцам через их внешние контракты. Статус доставки не заменяет финансовую квитанцию Engine. |

Один и тот же рыночный факт может требоваться двум владельцам. Например,
исследовательский снимок стакана принадлежит Research, а снимок для защиты
ордера — Engine. Каждый адаптер валидирует пригодность факта для своей
операции. Общий mutable cache и общая БД между корнями запрещены. Контракт
взаимодействия появляется по конкретному сценарию после автономной проверки.

## Разбор смешанных файлов

| Исходник или группа | Конкретное разделение |
| --- | --- |
| `okx-domain-core/src/{lib,observation_model}.rs` и `okx-observation/src/{lib,market,order_book,reference}.rs` | `AccountSnapshot`, `AccountAuthorityEvidence`, `AccountLedgerSummary`, `ExchangeFillIdentity` и факты допуска идут в Engine. `HistoryCandle`, `MarketHistorySnapshot`, `MarketTrade`, `FundingHistoryEvent`, `OpenInterestHistoryPoint` идут в Research. `MarketSnapshot`, `OrderBookSnapshot` и `InstrumentRulesSnapshot` разделяются на снимки конкретного потребителя. TCA получает явный импорт исполнений. `lib.rs` каждого корня экспортирует только его типы. |
| `okx-api/src/client.rs`, `config.rs`, `auth.rs` | `OkxRestClient`, `Credentials`, `WsLoginMaterial` и подпись — Engine. Публичный GET, `CapturedPublicRows` и `CapturedHttpDelivery` — публичный адаптер Research. Engine сохраняет собственный публичный клиент для фактов риска. `Region` и адрес endpoint задаёт конфигурация каждого адаптера. |
| `okx-api/src/{clock,error,instrument,lib,market_data,public_data,rate,transport}.rs` | Подписанные часы, limits и ошибки исполнения принадлежат Engine. `research_catalog_page`, `history_*_captured`, `market_data_history_captured` и их receipts — Research. `instrument`, price limit, ticker/mark и funding делятся по применению: допуск и сверка — Engine, исторический анализ и каталог — Research. У каждого корня собственные ошибки, deadline и бюджет запросов. |
| `okx-ws/src/{lib,protocol,public,wire}.rs` | Приватная аутентификация, account subscriptions и reconnect — Engine. Публичные подписки для live-аналитики — Research. Публичные подписки для контроля позиции — Engine. Фреймы и транспорт определяются в адаптере владельца. |
| `okx-market-runtime/src/{lib,market_bootstrap}.rs` и Engine `src/market_bootstrap.rs` | `snapshot` для торговли, reference refresh и состояние соединения Engine остаются в Engine. `research_source`, `research_*_page`, `analytics_*` и хранение исторических receipts переходят в Research. Engine re-export `MarketBootstrapper` выводится вместе со старым маршрутом. |
| `okx-ipc-contracts/src/lib.rs`, `account_catalog_proto.rs` | `NativeChildRequest/Reply`, торговые варианты `AgentOperation` и подтверждённая account identity — Engine. `Market` и `Research` — Research. `HostControlRequest/Operation/Result` — Control. `DirectTransportFrame` и доставка request ID — Edge. Каталог, прочитанный CONTROL, становится версионированным readback от Engine. `MailboxEnvelope` сохраняется только в переходном Edge-адаптере. |
| `okx-ipc-contracts/src/research_ipc.rs` и `okx-research-service/{Cargo.toml,src/main.rs,src/ipc.rs,src/source_api.rs}` | `ResearchCommand`, jobs и публичный вход — Research. `PublicSourceRequest/Reply` и разбор захваченных ответов — его adapter. `HostFrame/WorkerFrame` остаются у переходного stdio-входа до вывода. `SignalRequest/EvaluateSignal` снимутся при замене вызова из Engine. Новый сигнал описывается по сценарию Observe в межкомпонентной проверке. Manifest и transport тесты локальны Research. |
| Engine `src/{main,lib,runtime,config,query,cloudflare_transport}.rs` | Trade/finance, runtime account owner и приватная конфигурация остаются в Engine. `execute_market`, `execute_market_microstructure` и расчёты публичной аналитики — Research. Маршрутизация/обёртка ответа и WSS-доставка — Edge. Действия установки и запуска компонента — Control. В каждом корне собственный entrypoint и конфигурация. |
| Engine `src/research_client.rs` и `src/trade_service/strategies.rs` | `ResearchClient::installed` и выбор бинарника — Control. `research/market/capabilities/diagnostic_job` переходят в прямой Research API. `SourceHandler` выводится после запуска адаптера Research. `strategy_turn`: расчёт по закрытым барам — Research, право активации и проверка сделки — Engine. Синхронный `evaluate_signal` выводится по проверенному переходу. |
| `okx-github/src/lib.rs` | Загрузка release/artifact и provenance — Control. Issue polling, курсоры и публикация комментариев — Edge compatibility до снятия зависимости на mailbox. |
| Worker `runtime_session.ts`, `risk_authority.ts`, `account_governance*.ts` | `webSocketMessage`, очередь, ACK и маршрутизация — Edge. `initializeGovernance`, `setAccountLimits`, `setAccountPause`, `stopNewRealRisk` и хранение policy — Control после миграции. Engine проверяет применимую версию policy и финансовый допуск. Файлы `vnext/edge/src/account_governance*.ts` и соответствующие ветви `RuntimeSession` временно в Edge. Их рабочее поведение защищено тестами до переключения. |
| `contracts/trade-semantic-golden.json` | Канонический торговый контракт — Engine. Edge проверяет собственную доставку по неизменяемому snapshot. Межкомпонентная проверка сравнивает отдельно выпущенные версии. |

В Edge извлечённый `src/integration/runtime_entry.ts` служит его собственному
тесту транспорта. Исторические RiskBook и cutover-файлы остаются в исходном
`cloudflare/okx-mcp/` с пометкой `archive`. Старые Native crash/load entrypoints
также остаются в исходном дереве. Их критерии относятся к межкомпонентным
проверкам, которые будут перенесены после появления внешних контрактов.

## Автоматическая проверка

```sh
python3 -B vnext/check_boundaries.py
python3 -B vnext/check_boundaries.py --list
```

Проверка сверяет строки legacy-карты с Git index, применяет владельцев к
активным файлам и отклоняет локальные ссылки, выходящие за пределы корня.
Проверяются Cargo path-зависимости, включая dev, build и target, локальные
npm ссылки и статические пути в исходниках. Скрипт не пытается выводить
владение из смысла кода. Его дополняет сборка каждого корня из архива своего
каталога, которая обнаруживает скрытые зависимости от соседних исходников.
Новый legacy-файл требует явной строки с владельцем. При удалении legacy
файла строка удаляется вместе с ним.
