# erid-parser (public search + growth viewer)

Минимальный вынос сканера `searchPublicPosts` и визуализатора роста просмотров.

## Структура
- `config/paths.js` — пути/окружение (TDLib dirs, пути БД, файлы ссылок, конфиг аккаунтов, tmp).
- `tdlib-helpers.js` — обёртка вокруг `tdl` (login, создание клиента, delay, ensureDirectories).
- `scripts/`:
  - `search-public-posts.js` — сканер `searchPublicPosts` с мультиаккаунтами, SQLite (`public_search`, `channel_messages`, `runs`, `message_metrics`), сохранением ссылок.
  - `metrics-visualize.js` — HTTP API + статика growth viewer/builder, хранит сохранённые подборки в `tmp/growth-sets.json`, использует ту же SQLite.
  - `init-accounts.js` — создаёт конфиг аккаунтов для публичного поиска.
- `visualizer/` — фронтенд growth builder/viewer (`growth-builder/viewer.html|js|css`, редирект `growth.html`).
- `tmp/` — создаётся автоматически для БД/конфигов/подборок.

## Команды
```bash
npm install
npm run public-search       # запуск сканера (см. ENV ниже)
npm run visualize-growth    # сервер growth viewer на PORT=3100 (по умолчанию)
npm run init-accounts       # создать/перезаписать конфиг аккаунтов
```

## Основные переменные окружения
- TDLib: `TELEGRAM_API_ID`, `TELEGRAM_API_HASH`, `TDLIB_PATH` (опциональный путь к tdjson), `TDLIB_DATABASE_DIR`, `TDLIB_FILES_DIR`.
- Публичный поиск: `PUBLIC_SEARCH_QUERY`, `PUBLIC_SEARCH_LIMIT`, `PUBLIC_SEARCH_DELAY_MS`, `PUBLIC_SEARCH_MAX_PAGES`, `PUBLIC_SEARCH_STAR_SPEND`, `PUBLIC_SEARCH_OUTPUT`, `PUBLIC_SEARCH_DB_PATH`, `PUBLIC_SEARCH_ACCOUNTS_CONFIG`.
- Визуализатор: `PORT` (по умолчанию 3100), `PUBLIC_SEARCH_DB_PATH`, `GROWTH_SETS_PATH`.

## Мини-setup
0) Скопировать `.env.example` в `.env`, вписать свои `TELEGRAM_API_ID` и `TELEGRAM_API_HASH` (остальное можно оставить по умолчанию).
1) Создать `tmp/public-search-accounts.json`: `npm run init-accounts -- --name acc --count 3` (пути db/files берутся из `config/paths.js` или override `--db/--files`, для перезаписи файла добавьте `--force`).
2) Положить tdlib файлы в каталоги из `config/paths.js` или переопределить `TDLIB_*`.
3) `npm run public-search` — ссылки в `tmp/public-search-links.txt`, данные в SQLite.
4) `npm run visualize-growth` и открыть `http://localhost:3100/growth-builder.html`.
