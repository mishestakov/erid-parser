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
  const baseDb = argv.db || TDLIB_DATABASE_DIR;
  const baseFiles = argv.files || TDLIB_FILES_DIR;
  const name = argv.name || "default";
  const count = Math.max(1, Number(argv.count) || 1);
  const force = Boolean(argv.force);

  try {
    await fs.access(PUBLIC_SEARCH_ACCOUNTS_CONFIG);
    if (!force) {
      console.error(`Файл уже существует: ${PUBLIC_SEARCH_ACCOUNTS_CONFIG} (use --force для перезаписи)`);
      process.exit(1);
    }
  } catch (_) {
    // ok, файла нет
  }

  const accounts = [];
  for (let i = 0; i < count; i += 1) {
    const accName = count === 1 ? name : `${name}${i + 1}`;
    accounts.push({
      name: accName,
      database_directory: path.resolve(baseDb),
      files_directory: path.resolve(baseFiles),
      last_checked_at: null,
      daily_free: null,
      remaining_free: null,
      next_free_in: null,
      free_at: null,
      star_cost_per_query: null,
      star_balance: null,
      skip_stars: false
    });
  }

  await fs.mkdir(path.dirname(PUBLIC_SEARCH_ACCOUNTS_CONFIG), { recursive: true });
  await fs.writeFile(PUBLIC_SEARCH_ACCOUNTS_CONFIG, JSON.stringify(accounts, null, 2), "utf8");
  console.log(`Создан конфиг ${PUBLIC_SEARCH_ACCOUNTS_CONFIG}`);
  accounts.forEach((a) => {
    console.log(`- ${a.name}: db=${a.database_directory}, files=${a.files_directory}`);
  });
}

if (require.main === module) {
  main().catch((err) => {
    console.error(err);
    process.exitCode = 1;
  });
}
