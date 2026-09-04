# easybd

`easybd` — намеренно минималистичный протокол доступа к блочному
устройству поверх TCP, сервер и клиент к нему, сделанные как стенд для
сравнения I/O-бэкендов (обычные libc-вызовы vs io_uring, обычный io_uring
recv vs io_uring multishot recv) при сетевом round-trip — а не как боевое
удалённое блочное устройство.

Wire-протокол (`include/easybd/protocol.h`) намеренно голый: ни
magic-числа, ни версии, native byte order, никакой аутентификации/TLS. Он
рассчитан на доверенную LAN (или один хост, или два контейнера в одной
docker-сети) между специально написанными клиентом и сервером, которые
всегда обновляются вместе — обычные причины платить за более надёжный
wire-формат тут неприменимы, это инструмент для бенчмарков.

## Структура

- `easyio/` — асинхронное ядро ввода-вывода (`easyio::Queue`): одна
  реализация на обычных libc-вызовах, другая на io_uring (с опциональным
  режимом multishot recv). Оба бэкенда предоставляют одинаковый
  интерфейс, поэтому сервер и клиент написаны один раз и выбирают бэкенд
  в рантайме.
- `server/` — `easybd-server`: слушает TCP-порт, обслуживает
  чтения/записи в backing-файл или блочное устройство (по умолчанию
  открывается с `O_DIRECT`).
- `client/` — `libeasybd`: C API (`include/easybd/client.h`),
  оборачивающий одно TCP-соединение + один `easyio::Queue`; используется
  форком [easybd_fio](https://github.com/VasilyStepanov/easybd_fio) (fio
  с ioengine `easybd`) для реальной генерации нагрузки.
- `bench/` — скрипты, которые собирают весь стек и прогоняют/оформляют
  через него стандартную fio job-матрицу (см. [Бенчмаркинг](#бенчмаркинг)
  ниже).
- `docker/`, `docker-compose.yml` — топология из двух контейнеров
  (настоящее разделение клиент/сервер вместо обоих на localhost) для тех
  же скриптов бенчмаркинга.

## Сборка

Нужен компилятор C++20, autotools, pkg-config и
[`liburing`](https://github.com/axboe/liburing) >= 2.3 (если не собирать
с `--without-liburing`, что ограничивает вас только libc-бэкендом).

```
./autogen.sh
./configure --prefix=/path/to/install   # при необходимости добавьте --without-liburing или --disable-tests
make
make install
```

Это соберёт `server/easybd-server` и `libeasybd` (+ его pkg-config файл
`easybd.pc`), но не клиента, чтобы им нагружать сервер — см. ниже.

## Запуск сервера вручную

```
server/easybd-server --bind 0.0.0.0:39900 --file /path/to/backing-file-or-device \
  --queue-type io_uring --feature-multishot
```

Запустите `server/easybd-server --help` за полным списком флагов
(`--queue-type libc|io_uring`, `--threads`, `--queue-depth`,
`--direct 0|1`, ...).

## Бенчмаркинг

Для бенчмаркинга нужна сборка fio со встроенным ioengine easybd —
отдельный репозиторий,
[easybd_fio](https://github.com/VasilyStepanov/easybd_fio) — поверх
установленного easybd. `bench/build.sh` делает и то, и другое:

```
eval "$(bench/build.sh)"
# теперь заданы EASYBD_SERVER_BIN, EASYBD_LIB_DIR, EASYBD_FIO_BIN
```

Затем `bench/run.sh` прогоняет стандартную 12-профильную job-матрицу
(случайные 4k чтение/запись при qd1 и qd16, каждый ×1 и ×8 параллельных
соединений, последовательные 4M чтение/запись; см. `bench/lib.sh`) и
сохраняет по одному JSON-результату на профиль:

```
bench/run.sh --mode easybd --queue-type io_uring --multishot --sync 0 \
  --file /path/to/backing-file-or-device --out results/io_uring_ms_sync0 \
  --server-bin "$EASYBD_SERVER_BIN" --lib-dir "$EASYBD_LIB_DIR" --fio-bin "$EASYBD_FIO_BIN"
```

В этом режиме скрипт сам запускает/останавливает `easybd-server` на
loopback для заданной комбинации `--queue-type`/`--multishot`. Есть также
`--mode raw` (fio напрямую против `--file`, без easybd-сервера — базовая
линия "сырого" устройства) и, для сравнения настроек durability, просто
перезапустите с `--sync 1`.

Превратите один или несколько каталогов `--out` в единую сравнительную
таблицу через `bench/report.sh`:

```
bench/report.sh --title "libc vs io_uring vs io_uring-ms, sync=0" \
  libc=results/libc_sync0 io_uring=results/io_uring_sync0 io_uring-ms=results/io_uring_ms_sync0
```

Запустите любой из этих скриптов с `--help` за полным списком опций
(привязка к CPU через `--client-cpu`/`--server-cpu`, `--runtime`/`--ramp`,
поведение ретраев при сбое отдельного прогона и т.д.).

### Бенчмаркинг в двух контейнерах (docker-compose)

Всё выше гоняет клиента и сервер на одном хосте. `docker-compose.yml`
вместо этого разносит их по двум контейнерам в сети compose — сервис
`server` (backing-файл в именованном volume) и сервис `client` (fio +
ioengine easybd + скрипты `bench/`), так что клиент реально общается с
сервером по TCP/IP, а не через loopback:

```
docker compose build
docker compose up -d server
docker compose run --rm client \
  bench/run.sh --mode easybd --no-server --host server \
    --queue-type io_uring --multishot --sync 0 \
    --out results/io_uring_ms_sync0
docker compose run --rm client bench/report.sh mine=results/io_uring_ms_sync0
```

`--no-server --host server` говорит `bench/run.sh` не управлять сервером
самому, а просто прогнать клиентскую матрицу против уже слушающего
сервиса `server` (см. `bench/run.sh --help`). Результаты оказываются в
`./results` на хосте (примонтировано в клиентский контейнер).

Не нужно передавать `--fio-bin`/`--lib-dir`: `bench/run.sh` берёт их из
`$EASYBD_FIO_BIN`/`$EASYBD_LIB_DIR`, если флаги не заданы явно, а
`docker/client.Dockerfile` уже выставляет эти переменные внутри образа. И
не пытайтесь подставлять их сами в командной строке `docker compose run`
(например, `--fio-bin "$EASYBD_FIO_BIN"`) — это окружение вашего
хостового шелла, а не контейнера, так что оно развернётся в пустую строку
ещё до того, как docker вообще это увидит.

Чтобы протестировать другую комбинацию queue-type/бэкенда на сервере,
пересоздайте `server` с другими переменными `EASYBD_*` (полный список см.
в `docker/server-entrypoint.sh`), например:

```
EASYBD_QUEUE_TYPE=libc docker compose up -d --force-recreate server
```

Обоим сервисам нужен `security_opt: seccomp:unconfined` (уже задан в
`docker-compose.yml`) — без него системный вызов инициализации io_uring
блокируется дефолтным seccomp-профилем Docker, и тогда ни сервер, ни
клиент (который тоже использует io_uring для своего соединения,
независимо от `--queue-type` сервера) вообще не смогут подключиться.

#### Запуск полной матрицы

`bench/matrix.sh` автоматизирует всё вышеперечисленное по всем
комбинациям бэкенд×durability (libc / io_uring / io_uring-multishot,
каждый при `--sync 0` и `--sync 1`) и рендерит обе сравнительные таблицы.
Он выполняется *внутри* клиентского контейнера и не имеет собственного
доступа к Docker, поэтому вместо пересоздания одного сервера
`docker-compose.yml` держит поднятыми одновременно три фиксированных
сервера — `server-libc`/`server-io-uring`/`server-io-uring-ms` (каждый
простаивает, пока не тестируется именно его бэкенд), а `matrix.sh` просто
обращается к тому из них, который сейчас проверяет:

```
docker compose build
docker compose run --rm client bench/matrix.sh
```

Вот и всё — `depends_on` у `client` поднимет все три сервера
автоматически, так что эта одна команда (после однократного
`docker compose build`) прогоняет клиентскую матрицу при обоих значениях
sync против каждого бэкенда и пишет
`results/matrix-<timestamp>/sync{0,1}.md` (плюс сырые `<job>.json`/`.err`
каждой комбинации рядом). На дефолтных `--runtime 60 --ramp 20` ожидайте
порядка 96 минут (6 полных прогонов матрицы); передайте `--runtime`/
`--ramp`, чтобы сократить это для быстрой проверки, или `--out DIR`, чтобы
выбрать, куда класть результаты. См. `bench/matrix.sh --help` за
остальным.

`matrix.sh` не останавливает три сервера по завершении (опять же, нет
доступа к Docker изнутри контейнера) — сделайте `docker compose down`
сами, когда закончите бенчмаркинг.

## Известные ограничения

- Wire-протокол не поддерживает IPv6 (и клиент, и сервер работают только
  по IPv4).
