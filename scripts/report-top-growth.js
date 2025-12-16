"use strict";

/**
 * Выводит топ-10 постов по максимуму просмотров среди сообщений,
 * опубликованных не позже чем за 15 минут до первого run.
 *
 * Формат вывода:
 * link
 * runs: run1|run2|...
 * views: v1|v2|...
 * reactions: r1|r2|...
 *
 * Запуск:
 *   node scripts/report-top-growth.js [dbPath]
 * По умолчанию db = tmp/public-search-new.sqlite или PUBLIC_SEARCH_DB_PATH.
 */

const path = require("node:path");
const { DatabaseSync } = require("node:sqlite");
const { PUBLIC_SEARCH_DB_PATH, TMP_DIR } = require("../config/paths");

const DEFAULT_DB = PUBLIC_SEARCH_DB_PATH || path.join(TMP_DIR, "public-search.sqlite");

function toSeconds(ts) {
  return Math.floor(new Date(ts).getTime() / 1000);
}

function main() {
  const dbPath = process.argv[2] || DEFAULT_DB;
  const db = new DatabaseSync(dbPath);

  const refRun = db
    .prepare(
      `
      SELECT m.run_id, r.started_at
      FROM message_metrics m
      JOIN runs r ON r.run_id = m.run_id
      ORDER BY m.run_id DESC
      LIMIT 1
    `
    )
    .get();
  if (!refRun) {
    console.error("No runs with message data found");
    process.exit(1);
  }
  const refRunSec = toSeconds(refRun.started_at);
  const thresholdSec = refRunSec - 3 * 60;

  const runs = db
    .prepare("SELECT run_id FROM runs WHERE run_id >= ? ORDER BY run_id ASC")
    .all(refRun.run_id)
    .map((r) => r.run_id);

  const messages = db
    .prepare(
      `
      SELECT chat_id, message_id, message_link, message_date
      FROM channel_messages
      WHERE message_date BETWEEN ? AND ?
        AND message_link IS NOT NULL
    `
    )
    .all(thresholdSec, refRunSec);

  const scored = messages.map((m) => {
    const maxViewsRow = db
      .prepare("SELECT MAX(view_count) AS max_views FROM message_metrics WHERE chat_id = ? AND message_id = ?")
      .get(m.chat_id, m.message_id);
    return { ...m, max_views: maxViewsRow?.max_views ?? 0 };
  });

  scored.sort((a, b) => (b.max_views || 0) - (a.max_views || 0));
  const top = scored.slice(0, 10);

  for (const msg of top) {
    const metrics = db
      .prepare(
        `
        SELECT run_id, view_count, reactions_total
        FROM message_metrics
        WHERE chat_id = ? AND message_id = ?
        ORDER BY run_id ASC
      `
      )
      .all(msg.chat_id, msg.message_id);
    const byRun = new Map();
    for (const row of metrics) {
      byRun.set(row.run_id, row);
    }
    const viewsRow = runs.map((r) => {
      const v = byRun.get(r)?.view_count;
      return Number.isFinite(v) ? v : "";
    });
    const reactionsRow = runs.map((r) => {
      const v = byRun.get(r)?.reactions_total;
      return Number.isFinite(v) ? v : "";
    });
    console.log(msg.message_link || `${msg.chat_id}/${msg.message_id}`);
    console.log(`runs: ${runs.join("|")}`);
    console.log(`views: ${viewsRow.join("|")}`);
    console.log(`reactions: ${reactionsRow.join("|")}`);
    console.log("");
  }
}

if (require.main === module) {
  main();
}
