"use strict";

/**
 * Быстро создаёт/перезаписывает конфиг аккаунтов для public search.
 *
 * Примеры:
 *   node scripts/init-accounts.js                   # один аккаунт default с путями из config/paths.js
 *   node scripts/init-accounts.js --name acc1       # имя acc1
 *   node scripts/init-accounts.js --count 3         # acc1, acc2, acc3
 *   node scripts/init-accounts.js --db ./td/db1 --files ./td/files1 --name acc1
 *   node scripts/init-accounts.js --force           # перезаписать существующий файл
 */

const fs = require("node:fs/promises");
const path = require("node:path");
const { PUBLIC_SEARCH_ACCOUNTS_CONFIG, TDLIB_DATABASE_DIR, TDLIB_FILES_DIR } = require("../config/paths");
const { createClientWithDirs, login } = require("../tdlib-helpers");

function parseArgs() {
  const args = process.argv.slice(2);
  const res = {};
  for (let i = 0; i < args.length; i += 1) {
    const arg = args[i];
    if (!arg.startsWith("--")) continue;
    const key = arg.slice(2);
    const next = args[i + 1];
    if (next && !next.startsWith("--")) {
      res[key] = next;
      i += 1;
    } else {
      res[key] = true;
    }
  }
  return res;
}

async function main() {
  const argv = parseArgs();
  const baseDbRoot = argv.db ? path.resolve(argv.db) : path.resolve(TDLIB_DATABASE_DIR, "..");
  const baseFilesRoot = argv.files ? path.resolve(argv.files) : path.resolve(TDLIB_FILES_DIR, "..");
  const name = argv.name || "default";
  const count = Math.max(1, Number(argv.count) || 1);
  const force = Boolean(argv.force);
  const skipLogin = Boolean(argv["skip-login"]);

  let accounts = [];
  if (!force) {
    try {
      const raw = await fs.readFile(PUBLIC_SEARCH_ACCOUNTS_CONFIG, "utf8");
      const parsed = JSON.parse(raw);
      accounts = Array.isArray(parsed) ? parsed : Array.isArray(parsed.accounts) ? parsed.accounts : [];
      console.log(`Найдены существующие аккаунты: ${accounts.map((a) => a.name).join(", ") || "нет"}`);
    } catch (_) {
      // файла нет — начнём с пустого списка
    }
  }
  if (force) {
    accounts = [];
  }

  const newAccounts = [];
  for (let i = 0; i < count; i += 1) {
    const accName = count === 1 ? name : `${name}${i + 1}`;
    const dbDir = path.join(baseDbRoot, accName, "database");
    const filesDir = path.join(baseFilesRoot, accName, "files");
    accounts.push({
      name: accName,
      database_directory: dbDir,
      files_directory: filesDir,
      last_checked_at: null,
      daily_free: null,
      remaining_free: null,
      next_free_in: null,
      free_at: null,
      star_cost_per_query: null,
      star_balance: null,
      skip_stars: false
    });
    newAccounts.push(accName);
  }

  await fs.mkdir(path.dirname(PUBLIC_SEARCH_ACCOUNTS_CONFIG), { recursive: true });
  await fs.writeFile(PUBLIC_SEARCH_ACCOUNTS_CONFIG, JSON.stringify(accounts, null, 2), "utf8");
  console.log(`Создан конфиг ${PUBLIC_SEARCH_ACCOUNTS_CONFIG}`);
  accounts.forEach((a) => {
    console.log(`- ${a.name}: db=${a.database_directory}, files=${a.files_directory}`);
  });

  if (skipLogin) {
    console.log("Пропущен логин (--skip-login).");
    return;
  }

  for (const acc of accounts.slice(-newAccounts.length)) {
    console.log(`\n[login] ${acc.name}: db=${acc.database_directory}, files=${acc.files_directory}`);
    await fs.mkdir(acc.database_directory, { recursive: true });
    await fs.mkdir(acc.files_directory, { recursive: true });
    const client = createClientWithDirs({
      databaseDirectory: acc.database_directory,
      filesDirectory: acc.files_directory
    });
    try {
      await login(client);
      console.log(`[login] ${acc.name} ok`);
      try {
        const me = await client.invoke({ _: "getMe" });
        if (me?.usernames?.active_usernames?.length) {
          const username = me.usernames.active_usernames[0];
          acc.username = username;
          console.log(`[login] ${acc.name} username=@${username}`);
        }
      } catch (err) {
        console.warn(`[login] ${acc.name} getMe failed: ${err.message || err}`);
      }
    } catch (err) {
      console.error(`[login] ${acc.name} failed: ${err.message || err}`);
    } finally {
      try {
        await client.close();
        client.destroy();
      } catch (_) {
        // ignore
      }
    }
  }

  try {
    await fs.writeFile(PUBLIC_SEARCH_ACCOUNTS_CONFIG, JSON.stringify(accounts, null, 2), "utf8");
    console.log(`\nОбновлён конфиг с username: ${PUBLIC_SEARCH_ACCOUNTS_CONFIG}`);
  } catch (err) {
    console.warn(`Не удалось обновить конфиг после логина: ${err.message || err}`);
  }
}

if (require.main === module) {
  main().catch((err) => {
    console.error(err);
    process.exitCode = 1;
  });
}
