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
`legacy-research-test` удалены. Десять исходных Windows Worker process/IPC тестов
перемещены к Research; кандидат #596 добавляет два защитных теста и строгий
контроль **12/12**, не заменяя исходную десятку. Две финансовые T10 нагрузки сохранены с реальным
non-shipping Research fixture. Edge маршрутизация четырёх публичных market
операций и отдельный `RESEARCH_RUNTIME_OFFLINE` уже присутствуют в `main`
через `d0703b6a`. Исходный PR #583 закрыт как дубликат без повторного merge.
Состояние CONTROL-кода также следует сверять
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
**`S2 PHYSICAL_DENY=NOT_PROVEN`**. Интеграционный Draft #596 включает код из
#588/#592 и проходит Linux owner checks,
но успешная сборка/синтетические tests не являются свидетельством физического
запуска LPAC. Пока этот пункт открыт, нельзя делать production cutover,
Ready/merge security PR или включать отдельный Research token в production.
Старый каталог с обычными ACL не мигрировать молча: несовместимость —
типизированный отказ до переключения; допустимая virgin установка или
отдельно согласованное контролируемое восстановление.

### Наблюдаемый Windows-прогресс PR #596 (25.11.2026)

Все пять opt-in запусков `research-lpac-disposable-windows` завершились
**FAIL**, не S2 PASS. В каждом выполнены **5/5** отдельных ACL-тестов с
именованными witness lines, но шестой физический suspended-LPAC → Job → READY
тест не прошёл:

| Windows run / точный PR HEAD | Что реально наблюдалось | Итог |
| --- | --- | --- |
| [45700697746](https://github.com/iamaman11/okx/actions/runs/45700697746), `3159f17d` | ACL **5/5**; image preflight отказал `research_installed_image_mutable` | `FAIL`, запуск ребёнка до READY не доказан |
| [45701532922](https://github.com/iamaman11/okx/actions/runs/45701532922), `1da1c6ff` | ACL **5/5**; диагностика показала `FILE_READ_EA=0x8`, ошибочно считавшийся mutable; настоящий `FILE_WRITE_ATTRIBUTES=0x100` остаётся write right | `FAIL`; после уточнения маски image gate требует повторного прогона |
| [45702285358](https://github.com/iamaman11/okx/actions/runs/45702285358), `ef151fca` | После исправления read-EA image gate пройден; физический тест остановился на `ResearchSandboxDenied("worker_ready_pipe")`, ACL **5/5** | `FAIL`, READY не доказан |
| [45703166591](https://github.com/iamaman11/okx/actions/runs/45703166591), `9b87029b` | ACL **5/5**; typed отказ `worker_startup_auth_or_sandbox_denied_before_ready` (код дочернего процесса **76**) | `FAIL`, ребёнок завершился до READY |
| [45704093417](https://github.com/iamaman11/okx/actions/runs/45704093417), `7117fe84` | Draft owner-feedback **SUCCESS**; ACL **5/5**; LPAC дочерний процесс завершился с `worker_credman_read_probe_unexpected_before_ready` (код **83**) | `FAIL`, Win32 причина `CredReadW` требует числовой диагностики |

В текущем кандидате добавлены отдельные typed child exit **75–87**
для локализации первой отказавшей стадии. Их новый разбор пока **не проверен
полностью физическим прогоном**: код 83 достигнут, но ещё не классифицирован.
Debug-only числовой канал `GetLastError()` при `CredReadW` также должен получить
свой отдельный Windows witness; неизвестный код не является доказательством deny.
Наличие ACL witness не доказывает успешный LPAC token/Job,
READY/bearer, parent-positive/child-negative CredMan/finance canary,
restart/rollback или установленный CONTROL. `S2 PHYSICAL_DENY=NOT_PROVEN`;
отдельные 62 финансовых контракта / 107 свидетелей этим smoke не заменяются.

### Контракт одноразового Windows-свидетельства S2

GitHub-hosted `windows-latest` создаёт отдельную Windows VM для стандартного
job, поэтому подходит для **первой физической проверки механизмов ОС**.
Однако GitHub запускает Windows runner от администратора с отключённым UAC:
это не доказательство прав уже установленного CONTROL под пользовательской
учётной записью и не заменяет S3. Зафиксировать фактический Windows build,
runner image/version и token ребёнка, а не делать вывод по имени runner.
См. [GitHub-hosted runners](https://docs.github.com/en/actions/reference/runners/github-hosted-runners)
и [Microsoft AppContainer](https://learn.microsoft.com/en-us/windows/win32/secauthz/implementing-an-appcontainer).

Опциональная задача **`research-lpac-disposable-windows`** в том же
`.github/workflows/ci.yml` допускается только при явной метке Draft PR
`s2-disposable-windows`, установленной через GitHub. Ни один обычный Draft
не должен запускать её случайно. Она запускает шесть точных, явно
игнорируемых `--lib` тестов `research_sandbox::native::tests::disposable_real_research_lpac_ready_job_and_bearer`
и `research_acl::native::tests::disposable_windows_acl_*`, использует
`OKX_DISPOSABLE_WINDOWS_RUNNER=1`, создаёт все canary и каталог исключительно
под `${RUNNER_TEMP}` и публикует протокол даже при отказе. Никакого
`C:\okx`, рабочего finance DB, боевых Credential Manager targets,
Exchange POST, изменения protected status или публикации релиза.
Пропущенная цель, ноль исполненных тестов, отсутствие LPAC или отсутствие
доказательства отказа — **FAIL/NOT_RUN**, никогда PASS.

Машинный протокол `okx.s2.disposable-windows/v1` — это **свидетельство
запуска, а не сертификат S2 PASS**. Первый opt-in smoke обязан сохранить
SHA/tree, runner identity, test names, действительное ненулевое число
проверок и SHA256 логов. Полная матрица S2 PHYSICAL_DENY требует также
следующих OS-level свидетелей; если тест пока не создаёт их, соответствующая
строка остаётся `NOT_RUN/NOT_PROVEN`, даже при зелёном CI.

| Поле | Доказательство / допустимое значение | Когда обязательно |
| --- | --- | --- |
| `source_head_sha`, `source_tree`, `run_id`, `attempt` | Точные GitHub SHA/tree, PR number, workflow run/attempt и URL одного запуска; сверить с событием PR и receipt, а не с принятым `main` | Smoke |
| `runner_image`, `windows_build`, `test_root`, `disposable_marker` | Версия ОС и подтверждение изолированного `${RUNNER_TEMP}`, marker присутствует; запрещённый установленный путь не открывался | Smoke: image и root; полный S2: Windows build и marker |
| `time` | Реальные UTC start/end, queue/build/test/total по отдельности, test duration из runner/log; не выдавать предыдущие 1:25/5:55 за время этого запуска | Smoke: доступные фактические метки; полный S2: все интервалы |
| `parent`, `child`, `image` | PID и generation, SHA256 и путь образа; источник CONTROL, не вспомогательный подменный процесс | Полный S2 |
| `token` | Реальные `TokenIsAppContainer`, LPAC, Package SID, Low IL, capability `internetClient`, фактические группы AAP/ARAP, не только расчёт ACL | Полный S2 |
| `job` | Результат `AssignProcessToJobObject` **до** `ResumeThread`, nested Job, CPU 50%/RAM 4 GiB и readback ограничений | Полный S2 |
| `image_acl`, `catalog_acl` | Реальные child-попытки RX образа, отказ rewrite/`WRITE_DAC`/`WRITE_OWNER`; catalog create/write/delete и SQLite/WAL/SHM с проверкой Low IL наследования | Полный S2 |
| `finance_canaries`, `credman_canary` | Parent-positive **до и после READY**, child-negative для синтетических SQLite/WAL/SHM/lock/account/risk и CredMan, наблюдаемый Windows error code | Полный S2 |
| `startup` | Suspended → Job → token/canary → nonce-bound READY → одноразовый bearer; отсутствие bearer в argv/env/logs | Полный S2 |
| `test_cases`, `summary`, `receipt_sha256` | Для каждой проверки ID, `PASS/FAIL/NOT_RUN/BLOCKED`, первая недоказанная стадия, expected/observed; число реально выполненных случаев больше нуля; SHA логов и отчёта | Smoke; полный S2 расширяет поля свидетелей |

Research намеренно не содержит `CredWriteW`/`CredDeleteW`: это запрещённые
привилегированные API по его действующему owner-contract. Текущий дочерний
canary проверяет только `CredReadW`/`CredEnumerateW` и повторную сверку
содержимого родителем. Физическое доказательство отказа child-write/delete
Credential Manager пока `NOT_RUN`, не подразумевается булевым READY.

Одноразовый порядок: сверить source HEAD/tree → создать чистый корень
в `${RUNNER_TEMP}` → создать уникальные synthetic finance/CredMan canary с
положительным чтением родителем → запускать **именно** нужные тестовые EXE
и сохранить child OS-deny + фактические Windows error codes → повторить
parent-positive → остановить процессы и проверить отсутствие новых/осиротевших
Job/PID → собрать receipt и SHA256 логов/артефактов → уничтожить VM по
завершении job. Нулевой exit code Cargo без имён действительно выполненных
тестов и наблюдений — только `NOT_RUN`. Не создавать synthetic target с
именем рабочего account/CredMan target; не открывать установленный
`C:\okx` даже для проверки чистоты. Изолированная temp-root область —
единственное разрешённое место для синтетических canary/каталога (обычные
Cargo build/log artefacts допускаются в sandbox runner).

Внешние `BLOCKED` (`CreateAppContainerProfile`/LPAC недоступен, запрещённый
API, отсутствие admin privilege или OS feature) отличать от `FAIL`
(child неожиданно прочитал canary, не назначился Job, нарушен DACL/IL,
выполнен неверный SHA, исчезли старые данные). Непроведённый reboot,
неразвёрнутая настоящая пара Engine/Research и непротестированный S3 rollback
суть `NOT_RUN`, не `PASS`. Любая неясная/неполная запись сохраняет
`S2 PHYSICAL_DENY=NOT_PROVEN` и исключает production activation.

Для полного S2 PASS дополнительно необходимы два отдельно работающих
компонента, аварийный restart, конкурентный Start, сохранность Research jobs,
совместная нагрузка и отсутствие влияния на финансовый Engine. Успешный
**одноразовый LPAC smoke** подтверждает только указанные физические механизмы,
а не всю S2/S3 приёмку. Ненайденная capability или неподдерживаемый Win32
вызов классифицируется `BLOCKED` с точной стадией, не выдаётся за успешный
отказ доступа.

## S3 · Установка, обновление, диагностика

| Действие | Наблюдаемое доказательство |
| --- | --- |
| Установить и обновить Research при работающем Engine | Старый/новый Research image SHA, PID, generation и результат job; Engine SHA/PID, финансовая БД и поведение не меняются |
| Отказать в activation и откатить | Версия до/после, причина первой недоказанной стадии, восстановление старого job/result, отсутствие параллельного запуска двух Research |
| Обновить CONTROL/Launcher отдельно | Точные host-artifact hashes, корректное управление установленными компонентами, без Engine release и finance matrix в owner-only изменении |
| Проверить Edge Research offline | Действительный `RESEARCH_RUNTIME_OFFLINE`, никакого Engine fallback, никаких финансовых POST и новых процессов при read-only диагностике |
| `explain` после restart/offline | Владелец, reference, наблюдаемое состояние, стадия отказа и допустимый следующий шаг; при нехватке фактов `UNVERIFIED`/`UNKNOWN` |

Собирать свидетельства через установленный штатный CONTROL, адресно и без
ручного копирования бинарников. При **последующей приёмке штатной установки**
Windows использовать только `C:\okx`, подтверждённый GitHub artifact SHA и
штатный установщик; одноразовый предустановочный S2 smoke выше использует
исключительно `${RUNNER_TEMP}` и не является установленным приложением.
Virgin-host bootstrap отдельно `NOT_PROVEN`: Research-only installer требует
уже доверенный CONTROL/Launcher; Native bootstrap восстанавливает CONTROL
из полного bundle, а начальная Scheduler policy на чистом Windows может
отсутствовать. Не обходить этот отказ ручным task/ACL/EXE; документировать
точный preflight и сценарий корректного отказа/rollback до активации.
При успешном S3 пути проверить старый/новый SHA, единственный PID/generation,
сохранность catalog/jobs/SQLite и отсутствие рестарта Engine после rollback.
Локально Engine не пересобирать. Выпуск GitHub и Windows installation —
разные события. Финансовые операции запрещены; тесты используют fixtures.

## S4 · Один CI и происхождение выпуска

Единственный автоматический workflow — `.github/workflows/ci.yml`.
Одна `classify` выбирает component impact; `linux-core` и `windows-native`
агрегируют доказательства, а не выполняют повторную компиляцию.
Опциональный Draft S2 disposable Windows job в этом же файле не является
отдельным workflow, новым protected status или частью release-гарантий.

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
