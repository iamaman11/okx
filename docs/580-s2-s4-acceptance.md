# #580 · S2–S4: единая приёмка автономных компонентов

Этот документ — исполнимый чек-лист к [issue #580](https://github.com/iamaman11/okx/issues/580),
не отдельное ТЗ. Источник истины — канонический удалённый репозиторий и
точные SHA/tree/CI/Windows-свидетельства. Приёмку **кода**, **CI**,
**публикации** и **установленного приложения** нельзя подменять друг другом.

## Контрольная точка и владельцы

Принятый `main` на 25.11.2026 —
[`ef9060bd`](https://github.com/iamaman11/okx/commit/ef9060bd30a00225ea78050103a1768aa98217c5),
tree `ed0884d1402565378a7c61265700eeeb42be1ad0`.
Последний полный Ready CI:
[#45684845788](https://github.com/iamaman11/okx/actions/runs/45684845788),
SUCCESS за **5:55**, не соответствует целевым **≤120 с**.
Последующий `main` CI:
[#45685939326](https://github.com/iamaman11/okx/actions/runs/45685939326),
SUCCESS за **1:25**, использовал проверенный артефакт без новой сборки
Engine. Эти две длительности означают разные операции.

| Владелец | Что делает | Чего не делает |
| --- | --- | --- |
| Engine + SQLite (`okx-trading-engine`, `okx-trade-execution`) | Решает и исполняет **утверждённую живую стратегию** на свежих закрытых барах; единолично владеет финансовым SQLite/RiskBook, риском, ledger, at-most-once POST и восстановлением | Не тестирует гипотезы, не перебирает параметры, не вызывает Research за торговым сигналом |
| Research | Исследования, backtest/grid, публичные market-операции, jobs/result/replay, собственный каталог и WSS | Не читает финансовые БД/секреты, не осуществляет venue POST, не утверждает торговые операции |
| CONTROL/Launcher | Проверяет образ/SHA, устанавливает, запускает, останавливает, контролирует Job/PID и rollback компонента | Не принимает торговых решений и не рассчитывает исследовательские гипотезы |
| Edge/MCP | Авторизует и доставляет запрос нужному владельцу, различает offline и UNKNOWN | Не является владельцем финансовых данных или Research-истории; не подменяет недоступного владельца другим |

Строгая Windows Cargo normal/build/dev граница между Engine/CONTROL/Launcher/Bridge и
Research protocol/domain принята в #595; Engine `ResearchClient` и
`legacy-research-test` удалены. Десять Windows Worker process/IPC тестов
перемещены к Research. Две финансовые T10 нагрузки сохранены с реальным
non-shipping Research fixture. Edge маршрутизация четырёх публичных market
операций и отдельный `RESEARCH_RUNTIME_OFFLINE` уже присутствуют в `main`
через `d0703b6a`, хотя исходный PR #583 ещё открыт: повторное применение
его патча не требуется. Состояние CONTROL-кода также следует сверять
по дереву, а не считать каждый открытый PR неперенесённым.

## S2 · Физическое автономное исполнение

| Конфигурация установки | Проверка через пользовательский вход | Обязательный итог |
| --- | --- | --- |
| Только Engine, Research отсутствует | Фиксированные закрытые бары, стратегия/риск/финансовый SQL, lost-ACK/reopen | То же решение/экономика, одна допустимая отправка; ни Research process, ни callback/файл/сокет не требуются |
| Только Research, Engine отсутствует | `backtest`, `grid`, `status`, `cancel`, `result`, replay старого job, четыре market-команды | Детерминированные результаты и typed offline/error; нет Engine process/credentials/finance ACL |
| Оба | Реальные тяжёлые Research jobs + Engine finance read/protection/stop | Нет второго Research; нет нарушения финансовой задержки, CPU/RAM budget и at-most-once POST |
| Оба остановлены | Read-only status/diagnostics и запрос к отсутствующему владельцу | Ни новый процесс, ни POST, ни скрытый Engine fallback; причина `UNKNOWN`/`OFFLINE` по фактам |

Защитная часть S2 выполняется **только на одноразовой Windows VM** с
синтетическими файлами и CredMan, не на установленном `C:\okx`:

1. CONTROL создаёт Research suspended и до Resume доказывает AppContainer
   Package SID, LPAC, Low Integrity, единственный `internetClient`, точный
   набор наследуемых дескрипторов и успешное назначение ограниченного Job.
2. Хешированные image/manifest и защищённые DACL для Research EXE/version
   проверяются с защитой от reparse/hardlink и подмены пути; пакетная identity
   имеет только RX для образа. Research catalog/SQLite sidecars имеют
   изолированное RW/delete и правильное наследование Low IL. Engine/finance
   файлы не открываются и не меняются при provision Research.
3. Положительный тест родителя и отказ в Research ребёнке на синтетических
   CredMan/finance SQLite, WAL, SHM и lock, включая другие исторические
   account/risk пути. Отсутствие доступа проверено фактической попыткой ОС,
   а не только статическим анализом ACL.
4. Проверены nonce-bound READY → однократный bearer stdin, общий mutex,
   отсутствие bearer в argv/env/logs, пределы дочерних CPU **50%** и памяти
   **4 GiB** и их системный readback, отказ до Resume при любой неясности.
5. Restart/reboot/crash, параллельный Start, Stop по SHA+generation, clean
   rollback, сохранность jobs/cursor и отсутствие повторной отправки торговли.

Текущий статус этой физической защиты:
**`S2 PHYSICAL_DENY=NOT_PROVEN`**. Драфты #588/#592 содержат реализацию,
но успешная сборка/синтетические tests не являются свидетельством физического
запуска LPAC. Пока этот пункт открыт, нельзя делать production cutover,
Ready/merge security PR или включать отдельный Research token в production.
Старый каталог с обычными ACL не мигрировать молча: несовместимость —
типизированный отказ до переключения; допустимая virgin установка или
отдельно согласованное контролируемое восстановление.

## S3 · Установка, обновление, диагностика

| Действие | Наблюдаемое доказательство |
| --- | --- |
| Установить и обновить Research при работающем Engine | Старый/новый Research image SHA, PID, generation и результат job; Engine SHA/PID, финансовая БД и поведение не меняются |
| Отказать в activation и откатить | Версия до/после, причина первой недоказанной стадии, восстановление старого job/result, отсутствие параллельного запуска двух Research |
| Обновить CONTROL/Launcher отдельно | Точные host-artifact hashes, корректное управление установленными компонентами, без Engine release и finance matrix в owner-only изменении |
| Проверить Edge Research offline | Действительный `RESEARCH_RUNTIME_OFFLINE`, никакого Engine fallback, никаких финансовых POST и новых процессов при read-only диагностике |
| `explain` после restart/offline | Владелец, reference, наблюдаемое состояние, стадия отказа и допустимый следующий шаг; при нехватке фактов `UNVERIFIED`/`UNKNOWN` |

Собирать свидетельства через установленный штатный CONTROL, адресно и без
ручного копирования бинарников. Для тестовой установки Windows использовать
только `C:\okx`, подтверждённый GitHub artifact SHA и штатный установщик;
локально Engine не пересобирать. Выпуск GitHub и Windows installation —
разные события. Финансовые операции запрещены; тесты используют fixtures.

## S4 · Один CI и происхождение выпуска

Единственный автоматический workflow — `.github/workflows/ci.yml`.
Одна `classify` выбирает component impact; `linux-core` и `windows-native`
агрегируют доказательства, а не выполняют повторную компиляцию.

| Изменение только владельца | Требуемый маршрут Ready | Что не должно выполняться |
| --- | --- | --- |
| Engine/его финансовый SQL | Полные 62 финансовых контракта / 107 свидетелей, 2 T10, 4 Windows suites и exact-SHA Windows release | Ничего из обязательной финансовой матрицы не исключать |
| Research | Research Windows IPC/Edge + переносимые проверки, component artifact | `rustc`/link Engine, финансовая матрица, повторная сборка четырёх EXE |
| CONTROL/Launcher | `host-controller-launcher-checks` с реальными Windows native тестами | Engine compile/finance, Research build без необходимости |
| Edge | TypeScript/MCP/DO/security contracts, no Engine fallback | Engine и финансовые Windows jobs |
| Shared, CI, неизвестный diff, неполный resolver | Full fail-closed путь | Нельзя разрешать skip по названию папки |

Смешанные два owner-only изменения проходят оба затронутых маршрута
лишь после доказанной неизменности protected Windows Cargo closures; иначе
`full`. Нужны четыре настоящих owner-only PR/runs и отрицательные контроли:
неполный diff, подменённый artifact digest, пропущенный обязательный job,
пустой/нулевой test run и чужой SHA/tree должны блокировать публикацию.
Ни skipped job, ни workflow conclusion не заменяют точный первый Ready attempt.

Публикация `main` использует *те же bytes* из успешно принятого Ready tree,
не компилирует Engine повторно и не устанавливает EXE. Релиз с именем
`native-base-ef9060bd...` существует, но не доказывает установку.

**Никакого скрытого переименования статусов:** protected `classify`,
`linux-core`, `windows-native` сохраняются. Текущий установленный CONTROL
и Research manifest `okx.windows.research-component/v1` требуют семь
первых Ready имен: `classify`, `architecture-contracts`,
`dependency-audit`, `cloudflare-mcp-contracts`,
`research-service-tests-windows`, `linux-core`, `windows-native`.
Типизированный manifest v2 и policy
`okx.github.actions.research-ready/v2` в исходниках предусмотрены, но
**producer и установленная миграция не активированы**. Новые отображаемые
имена нельзя использовать без совместимого CONTROL, publisher, проверки
доказательств, branch protection и отрицательных тестов.

## Что засчитывает этап

Для каждого PASS — точный git HEAD/tree, входные данные, ожидаемый и
фактический результат, ссылка на CI или SHA-verified Windows observation,
тайминги и артефакты. Если нет реального исполнения — `NOT_RUN`, доказательство
невозможно — `BLOCKED`, найдено расхождение — `FAIL`. Отсутствие LPAC VM
оставляет S2 `NOT_PROVEN` независимо от успешного обычного CI.

Для S5/A7 нужны **12 полных замеров** (три изменения каждого из четырёх
владельцев), на прогретом cache, каждый ≤120 секунд включая обязательные
checks и готовый artifact. Ускоренный `main` merge/release run **1:25**
не засчитывается за полный Ready run **5:55**. Никаких дополнительных
расписаний или тестовых оркестраторов ради таблицы не требуется.
