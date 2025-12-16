# erid-parser (public search + growth viewer)

Минимальный вынос сканера `searchPublicPosts` и визуализатора роста просмотров.

## Структура
- `config/paths.js` — читает `.env`, задаёт дефолтные пути (по умолчанию в `data/`), прокидывает env переменные.
- `data/` — runtime данные: SQLite (`data/db/public-search.sqlite`), `growth-sets.json`, TDLib `data/tdlib/*`, выгрузки ссылок, конфиг аккаунтов. В гите игнорируется.
- `tdlib-helpers.js` — обёртка вокруг `tdl` (login, создание клиента, delay, ensureDirectories).
- `scripts/`:
  - `search-public-posts.js` — сканер `searchPublicPosts` с мультиаккаунтами, SQLite (`public_search`, `channel_messages`, `runs`, `message_metrics`), сохранением ссылок.
  - `metrics-visualize.js` — HTTP API + статика growth viewer/builder, хранит сохранённые подборки в `data/growth-sets.json`, использует ту же SQLite.
  - `init-accounts.js` — создаёт конфиг аккаунтов для публичного поиска.
- `visualizer/` — фронтенд growth builder/viewer (`growth-builder/viewer.html|js|css`, редирект `growth.html`).
- `tmp/` — резерв под временные файлы (если понадобится).

## Команды
```bash
npm install
npm run public-search       # запуск сканера (см. ENV ниже)
npm run visualize-growth    # сервер growth viewer на PORT=3100 (по умолчанию)
npm run init-accounts       # создать/перезаписать конфиг аккаунтов
```

## Основные переменные окружения
- Базовый каталог: `DATA_DIR` (по умолчанию `./data`).
- TDLib: `TELEGRAM_API_ID`, `TELEGRAM_API_HASH`, `TDLIB_PATH` (опциональный путь к tdjson), `TDLIB_DATABASE_DIR`, `TDLIB_FILES_DIR`.
- Публичный поиск: `PUBLIC_SEARCH_QUERY`, `PUBLIC_SEARCH_LIMIT`, `PUBLIC_SEARCH_DELAY_MS`, `PUBLIC_SEARCH_STAR_SPEND`, `PUBLIC_SEARCH_OUTPUT`, `PUBLIC_SEARCH_DB_PATH`, `PUBLIC_SEARCH_ACCOUNTS_CONFIG`.
- Визуализатор: `PORT` (по умолчанию 3100), `PUBLIC_SEARCH_DB_PATH`, `GROWTH_SETS_PATH`.

## Мини-setup
0) Скопировать `.env.example` в `.env`, вписать свои `TELEGRAM_API_ID` и `TELEGRAM_API_HASH` (остальное можно оставить по умолчанию).
1) Создать `data/public-search-accounts.json`: `npm run init-accounts -- --name acc --count 3`. Скрипт добавит аккаунты (существующий файл не перезаписывается без `--force`), создаст отдельные каталоги для каждого (`data/tdlib/<name>/database|files`) и проведёт интерактивный логин (телефон/код/2FA), после логина проставит `username` в JSON. Флаг `--skip-login` — если нужно только сгенерировать конфиг.
2) Положить tdlib файлы в каталоги из `config/paths.js` или переопределить `TDLIB_*` (по умолчанию `data/tdlib/<name>/...` для новых аккаунтов).
3) `npm run public-search` — данные в SQLite `data/db/public-search.sqlite` (файл ссылок не пишется по умолчанию, включить можно env `PUBLIC_SEARCH_OUTPUT`).
4) `npm run visualize-growth` и открыть `http://localhost:3100/growth-builder.html`.

## Автозапуск (systemd)
Пример юнита для сканера (`/etc/systemd/system/erid-public-search.service`):
```
[Unit]
Description=erid-parser public search
After=network-online.target

[Service]
Type=simple
WorkingDirectory=/opt/erid-parser
EnvironmentFile=/opt/erid-parser/.env
ExecStart=/usr/bin/node scripts/search-public-posts.js
Restart=always
RestartSec=5
StandardOutput=append:/opt/erid-parser/logs/search.log
StandardError=append:/opt/erid-parser/logs/search.err.log

[Install]
WantedBy=multi-user.target
```
Визуализатор аналогично (`ExecStart=/usr/bin/node scripts/metrics-visualize.js`, можно добавить `Environment=PORT=3100`). Перед запуском `sudo systemctl daemon-reload && sudo systemctl enable --now erid-public-search.service`.
