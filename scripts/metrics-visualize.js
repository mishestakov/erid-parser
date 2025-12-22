"use strict";

const http = require("node:http");
const fs = require("node:fs/promises");
const fsSync = require("node:fs");
const path = require("node:path");
const url = require("node:url");
const { DatabaseSync } = require("node:sqlite");
const { createClickhouseClient } = require("../clickhouse");
const { PUBLIC_SEARCH_DB_PATH, GROWTH_SETS_PATH } = require("../config/paths");

const STATIC_DIR = path.join(__dirname, "..", "visualizer");
const PORT = Number(process.env.PORT || 3100);
const DB_PATH = PUBLIC_SEARCH_DB_PATH;
const SETS_PATH = GROWTH_SETS_PATH;
const CLICKHOUSE_METRICS_TABLE = process.env.CLICKHOUSE_METRICS_TABLE || "message_metrics";
const CLICKHOUSE_READ_CHUNK = Number(process.env.CLICKHOUSE_READ_CHUNK || 1000);
const MINPOINTS_SCAN_LIMIT = Number.isFinite(Number(process.env.CLICKHOUSE_MINPOINTS_SCAN_LIMIT))
  ? Number(process.env.CLICKHOUSE_MINPOINTS_SCAN_LIMIT)
  : 50000;
const DEFAULT_PATTERN_TAGS = [
  "zero_start_late_spike",
  "linear_growth",
  "steps",
  "plateau_after_round",
  "plus30_pattern",
  "micro_steps"
];
const PATTERN_KEYS = [...DEFAULT_PATTERN_TAGS, "other"];

fsSync.mkdirSync(path.dirname(DB_PATH), { recursive: true });

const db = new DatabaseSync(DB_PATH);
db.exec(`
  CREATE TABLE IF NOT EXISTS message_flags_manual (
    chat_id INTEGER NOT NULL,
    message_id INTEGER NOT NULL,
    ok INTEGER,
    zero_start_late_spike INTEGER,
    linear_growth INTEGER,
    steps INTEGER,
    plateau_after_round INTEGER,
    plus30_pattern INTEGER,
    micro_steps INTEGER,
    other INTEGER,
    note TEXT,
    updated_at TEXT NOT NULL DEFAULT (datetime('now')),
    updated_by TEXT,
    PRIMARY KEY (chat_id, message_id)
  );
`);

db.exec(`
  CREATE TABLE IF NOT EXISTS message_flags_auto (
    chat_id INTEGER NOT NULL,
    message_id INTEGER NOT NULL,
    ok REAL,
    zero_start_late_spike REAL,
    linear_growth REAL,
    steps REAL,
    plateau_after_round REAL,
    plus30_pattern REAL,
    micro_steps REAL,
    other REAL,
    scores_json TEXT,
    meta_json TEXT,
    updated_at TEXT NOT NULL DEFAULT (datetime('now')),
    PRIMARY KEY (chat_id, message_id)
  );
`);

const HAS_MESSAGE_LABELS = Boolean(
  db.prepare(`SELECT name FROM sqlite_master WHERE type='table' AND name='message_labels';`).get()
);
const clickhouse = createClickhouseClient();
const SAFE_METRICS_TABLE = (() => {
  if (!/^[A-Za-z0-9_]+(\.[A-Za-z0-9_]+)?$/.test(CLICKHOUSE_METRICS_TABLE)) {
    throw new Error(`Invalid CLICKHOUSE_METRICS_TABLE: ${CLICKHOUSE_METRICS_TABLE}`);
  }
  return CLICKHOUSE_METRICS_TABLE;
})();

function metricsKey(chatId, messageId) {
  return `${chatId}:${messageId}`;
}

async function fetchMetricsSeries(chatId, messageId) {
  const sql = `
    SELECT
      run_id,
      toUnixTimestamp(ts) AS ts,
      view_count,
      forward_count,
      reply_count,
      reactions_total,
      reactions_paid,
      reactions_free
    FROM ${SAFE_METRICS_TABLE}
    WHERE chat_id = ${Math.trunc(chatId)} AND message_id = ${Math.trunc(messageId)}
    ORDER BY ts ASC, run_id ASC
  `;
  return clickhouse.queryJsonEachRow(sql);
}

async function fetchMetricsCounts(ids) {
  const map = new Map();
  if (!Array.isArray(ids) || ids.length === 0) return map;
  const chunkSize = Number.isFinite(CLICKHOUSE_READ_CHUNK) && CLICKHOUSE_READ_CHUNK > 0 ? CLICKHOUSE_READ_CHUNK : 1000;
  for (let i = 0; i < ids.length; i += chunkSize) {
    const chunk = ids.slice(i, i + chunkSize);
    const tuples = chunk
      .map((row) => `(${Math.trunc(row.chat_id)},${Math.trunc(row.message_id)})`)
      .join(",");
    const sql = `
      SELECT chat_id, message_id, count() AS cnt
      FROM ${SAFE_METRICS_TABLE}
      WHERE (chat_id, message_id) IN (${tuples})
      GROUP BY chat_id, message_id
    `;
    const rows = await clickhouse.queryJsonEachRow(sql);
    for (const r of rows) {
      const key = metricsKey(r.chat_id, r.message_id);
      map.set(key, Number(r.cnt) || 0);
    }
  }
  return map;
}

async function collectDbStats() {
  const disk =
    typeof fs.statfs === "function"
      ? await fs
          .statfs(path.dirname(DB_PATH))
          .then((stat) => {
            const block = Number(stat.bsize || stat.f_bsize || stat.blockSize || 4096);
            const total = Number(stat.blocks);
            const free = Number(stat.bavail ?? stat.bfree);
            return Number.isFinite(total) && Number.isFinite(free)
              ? { total_bytes: total * block, free_bytes: free * block }
              : null;
          })
          .catch(() => null)
      : null;

  const dbFile = await fs
    .stat(DB_PATH)
    .then((st) => ({ path: DB_PATH, size_bytes: Number(st.size) || 0 }))
    .catch(() => ({ path: DB_PATH, size_bytes: null }));

  let tables = [];
  try {
    tables = db
      .prepare(
        `
        SELECT name, SUM(pgsize) AS size_bytes
        FROM dbstat
        WHERE name NOT LIKE 'sqlite_%'
        GROUP BY name
        ORDER BY size_bytes DESC
        LIMIT 3
      `
      )
      .all()
      .map((r) => ({ name: r.name, size_bytes: Number(r.size_bytes) || 0 }));
  } catch (_) {
    tables = [];
  }

  return { disk, db: dbFile, tables };
}

function sendJson(res, status, payload) {
  res.writeHead(status, { "Content-Type": "application/json" });
  res.end(JSON.stringify(payload));
}

async function readJsonSafe(filePath) {
  try {
    const data = await fs.readFile(filePath, "utf8");
    return JSON.parse(data);
  } catch (_) {
    return null;
  }
}

async function writeJsonSafe(filePath, data) {
  await fs.mkdir(path.dirname(filePath), { recursive: true });
  await fs.writeFile(filePath, JSON.stringify(data, null, 2), "utf8");
}

async function serveStatic(req, res, pathname) {
  const filePath = path.join(STATIC_DIR, pathname === "/" ? "growth.html" : pathname.slice(1));
  try {
    const data = await fs.readFile(filePath);
    const ext = path.extname(filePath).toLowerCase();
    const type =
      ext === ".html"
        ? "text/html"
        : ext === ".js"
          ? "application/javascript"
          : ext === ".css"
            ? "text/css"
            : "application/octet-stream";
    res.writeHead(200, { "Content-Type": type });
    res.end(data);
  } catch (err) {
    res.writeHead(404);
    res.end("Not found");
  }
}

function numberOrNull(val) {
  const num = Number(val);
  return Number.isFinite(num) ? num : null;
}

function parseScoresJson(jsonStr) {
  if (!jsonStr) return {};
  try {
    const parsed = JSON.parse(jsonStr);
    if (parsed && typeof parsed === "object") return parsed;
  } catch (_) {
    /* ignore parse errors */
  }
  return {};
}

function extractAutoScores(row) {
  const scores = { ...parseScoresJson(row?.scores_json) };
  for (const key of PATTERN_KEYS) {
    const val = numberOrNull(row?.[key]);
    if (val !== null) scores[key] = val;
  }
  return scores;
}

function extractManualFlags(row) {
  if (!row) return {};
  const flags = {};
  for (const key of [...PATTERN_KEYS, "ok"]) {
    const val = numberOrNull(row[key]);
    if (val !== null) flags[key] = val;
  }
  if (row.note !== undefined) flags.note = row.note;
  return flags;
}

async function handleMetrics(req, res, query) {
  const chatId = Number(query.chat_id);
  const messageId = Number(query.message_id);
  if (!Number.isFinite(chatId) || !Number.isFinite(messageId)) {
    return sendJson(res, 400, { error: "chat_id and message_id are required" });
  }

  const msgRow = db
    .prepare(
      `
      SELECT message_link, message_date, view_count, forward_count, reply_count, reactions_total, reactions_paid, reactions_free
      FROM channel_messages
      WHERE chat_id = ? AND message_id = ?
      LIMIT 1
    `
    )
    .get(chatId, messageId);

  let metrics = [];
  try {
    metrics = await fetchMetricsSeries(chatId, messageId);
  } catch (err) {
    console.error("ClickHouse metrics fetch failed", err);
    return sendJson(res, 502, { error: "ClickHouse unavailable" });
  }

  const runs = db
    .prepare("SELECT run_id, started_at, account_name, limits_exceeded FROM runs ORDER BY run_id ASC")
    .all();

  return sendJson(res, 200, {
    chat_id: chatId,
    message_id: messageId,
    message: msgRow || null,
    metrics,
    runs
  });
}

async function readJsonBody(req) {
  return new Promise((resolve, reject) => {
    let data = "";
    req.on("data", (chunk) => {
      data += chunk;
      if (data.length > 2_000_000) {
        reject(new Error("Body too large"));
        req.destroy();
      }
    });
    req.on("end", () => {
      if (!data) return resolve({});
      try {
        resolve(JSON.parse(data));
      } catch (err) {
        reject(err);
      }
    });
    req.on("error", reject);
  });
}

function handleLabels(req, res, query) {
  const chatId = Number(query.chat_id);
  const messageId = Number(query.message_id);
  if (!Number.isFinite(chatId) || !Number.isFinite(messageId)) {
    return sendJson(res, 400, { error: "chat_id and message_id are required" });
  }
  const manual =
    db
      .prepare("SELECT * FROM message_flags_manual WHERE chat_id = ? AND message_id = ? LIMIT 1")
      .get(chatId, messageId) || null;
  const auto =
    db
      .prepare("SELECT * FROM message_flags_auto WHERE chat_id = ? AND message_id = ? LIMIT 1")
      .get(chatId, messageId) || null;
  return sendJson(res, 200, { manual, auto });
}

async function handleLabelPost(req, res) {
  const body = await readJsonBody(req);
  const chatId = Number(body.chat_id);
  const messageId = Number(body.message_id);
  if (!Number.isFinite(chatId) || !Number.isFinite(messageId)) {
    return sendJson(res, 400, { error: "chat_id and message_id are required" });
  }

  const manual = body.manual || body;
  const fields = {
    ok: manual.ok,
    zero_start_late_spike: manual.zero_start_late_spike ?? manual.zeroStartLateSpike,
    linear_growth: manual.linear_growth ?? manual.linearGrowth,
    steps: manual.steps,
    plateau_after_round: manual.plateau_after_round ?? manual.plateauAfterRound,
    plus30_pattern: manual.plus30_pattern ?? manual.plus30Pattern,
    micro_steps: manual.micro_steps ?? manual.microSteps,
    other: manual.other,
    note: manual.note
  };
  const columns = [];
  const values = [];
  const updates = [];
  for (const [key, val] of Object.entries(fields)) {
    if (val === undefined) continue;
    const norm =
      val === null ? null : typeof val === "boolean" ? (val ? 1 : 0) : Number.isFinite(Number(val)) ? Number(val) : null;
    columns.push(key);
    values.push(norm);
    updates.push(`${key}=excluded.${key}`);
  }
  if (!columns.length) {
    return sendJson(res, 400, { error: "no fields to update" });
  }
  const placeholders = columns.map(() => "?").join(",");
  const updateSql = updates.join(", ");

  db.prepare(
    `
    INSERT INTO message_flags_manual (chat_id, message_id, ${columns.join(", ")}, updated_at)
    VALUES (${["?", "?"].concat(new Array(columns.length).fill("?")).join(", ")}, datetime('now'))
    ON CONFLICT(chat_id, message_id) DO UPDATE SET
      ${updateSql},
      updated_at=datetime('now')
  `
  ).run(chatId, messageId, ...values);

  const row =
    db
      .prepare("SELECT * FROM message_flags_manual WHERE chat_id = ? AND message_id = ? LIMIT 1")
      .get(chatId, messageId) || null;

  return sendJson(res, 200, { ok: true, manual: row });
}

function handlePatternTags(req, res) {
  const tags = new Set([...DEFAULT_PATTERN_TAGS, "other"]);
  if (HAS_MESSAGE_LABELS) {
    try {
      const rows = db
        .prepare(
          `
          SELECT DISTINCT pattern_tag
          FROM message_labels
          WHERE pattern_tag IS NOT NULL AND pattern_tag != ''
        `
        )
        .all();
      for (const row of rows) {
        if (row?.pattern_tag) tags.add(row.pattern_tag);
      }
    } catch (_) {
      // ignore lookup issues, fall back to defaults
    }
  }
  return sendJson(res, 200, { tags: Array.from(tags) });
}

function handleBloggers(req, res) {
  const rows =
    db
      .prepare(
        `
        SELECT ps.chat_id AS id,
               ps.active_username AS u,
               ps.member_count AS s,
               COUNT(cm.message_id) AS p,
               AVG(cm.view_count) AS v
        FROM public_search ps
        LEFT JOIN channel_messages cm ON cm.chat_id = ps.chat_id
        WHERE ps.active_username IS NOT NULL AND ps.active_username != ''
        GROUP BY ps.chat_id, ps.active_username, ps.member_count
        ORDER BY p DESC
      `
      )
      .all() || [];

  const items = rows.map((r) => ({
    id: r.id,
    u: r.u,
    s: Number.isFinite(r.s) ? r.s : null,
    p: Number.isFinite(r.p) ? r.p : 0,
    v: Number.isFinite(r.v) ? Math.round(r.v) : null
  }));

  return sendJson(res, 200, { items });
}

function handleChannelItems(req, res, query) {
  const chatId = Number(query.chat_id);
  if (!Number.isFinite(chatId)) return sendJson(res, 400, { error: "chat_id required" });

  const meta =
    db
      .prepare("SELECT active_username AS u, title FROM public_search WHERE chat_id = ? LIMIT 1")
      .get(chatId) || null;

  const rows =
    db
      .prepare(
        `
        SELECT message_id, message_date, view_count
        FROM channel_messages
        WHERE chat_id = ?
        ORDER BY message_date ASC
      `
      )
      .all(chatId) || [];

  const items = rows.map((r) => ({
    chat_id: chatId,
    message_id: r.message_id,
    message_date: r.message_date,
    view_count: r.view_count
  }));

  const name = meta?.u ? `@${meta.u}` : meta?.title || `channel ${chatId}`;
  return sendJson(res, 200, {
    set: {
      id: `channel-${chatId}`,
      name,
      total: items.length,
      items
    }
  });
}

async function handleDbStats(req, res) {
  try {
    const stats = await collectDbStats();
    return sendJson(res, 200, stats);
  } catch (err) {
    console.error("db stats failed", err);
    return sendJson(res, 500, { error: err.message || "db stats failed" });
  }
}

async function loadSavedSets() {
  const data = await readJsonSafe(SETS_PATH);
  if (!Array.isArray(data)) return [];
  return data;
}

async function saveSavedSets(list) {
  await writeJsonSafe(SETS_PATH, list);
}

async function handleSets(req, res, pathname, method) {
  if (pathname === "/api/sets" && method === "GET") {
    const sets = await loadSavedSets();
    const meta = sets.map((s) => ({
      id: s.id,
      name: s.name,
      total: s.total,
      filters: s.filters,
      created_at: s.created_at
    }));
    return sendJson(res, 200, { sets: meta });
  }

  if (pathname === "/api/sets" && method === "POST") {
    const body = await readJsonBody(req);
    const name = (body.name || "").trim();
    if (!name) return sendJson(res, 400, { error: "name is required" });
    const listResult = buildListResult(body.filters || body);
    const items = Array.isArray(listResult.items) ? listResult.items : [];
    const total = Number(listResult.total) || items.length;
    const set = {
      id: `set-${Date.now().toString(36)}-${Math.random().toString(36).slice(2, 6)}`,
      name,
      filters: body.filters || body,
      total,
      items,
      created_at: new Date().toISOString()
    };
    const sets = await loadSavedSets();
    sets.unshift(set);
    await saveSavedSets(sets);
    return sendJson(res, 200, { ok: true, set: { ...set, items: undefined }, total });
  }

  if (pathname.startsWith("/api/sets/")) {
    const parts = pathname.split("/").filter(Boolean);
    const id = parts[2] || parts[1]; // tolerate both /api/sets/:id and mis-split
    if (!id) return sendJson(res, 400, { error: "id is required" });
    const sets = await loadSavedSets();
    const idx = sets.findIndex((s) => s.id === id);
    if (idx === -1) return sendJson(res, 404, { error: "set not found" });
    if (method === "GET") {
      const set = sets[idx];
      return sendJson(res, 200, { set });
    }
    if (method === "DELETE") {
      sets.splice(idx, 1);
      await saveSavedSets(sets);
      return sendJson(res, 200, { ok: true });
    }
  }

  return false;
}

function parseIdsList(ids) {
  const parsed = [];
  if (!Array.isArray(ids)) return parsed;
  for (const item of ids) {
    const chatId = Number(item.chat_id);
    const messageId = Number(item.message_id);
    if (Number.isFinite(chatId) && Number.isFinite(messageId)) {
      parsed.push({ chat_id: chatId, message_id: messageId });
    }
  }
  return parsed;
}

function withTempIds(ids, fn) {
  const table = `tmp_ids_${Date.now().toString(36)}_${Math.random().toString(36).slice(2, 6)}`;
  db.exec(`CREATE TEMP TABLE ${table} (chat_id INTEGER NOT NULL, message_id INTEGER NOT NULL);`);
  const insert = db.prepare(`INSERT INTO ${table} (chat_id, message_id) VALUES (?, ?);`);
  db.exec("BEGIN IMMEDIATE;");
  try {
    for (const row of ids) insert.run(row.chat_id, row.message_id);
    db.exec("COMMIT;");
  } catch (err) {
    db.exec("ROLLBACK;");
    throw err;
  }
  try {
    return fn(table);
  } finally {
    db.exec(`DROP TABLE IF EXISTS ${table};`);
  }
}

function collectLabelsFromMessageLabels(rows, labelerFilter) {
  if (!rows.length) {
    return { patternScoresMap: new Map(), autoLabelMap: new Map(), manualLabelMap: new Map(), manualFlagsMap: new Map() };
  }
  const labels = withTempIds(
    rows.map((r) => ({ chat_id: r.chat_id, message_id: r.message_id })),
    (table) =>
      db
        .prepare(
          `
        SELECT ml.*
        FROM message_labels ml
        JOIN ${table} t ON t.chat_id = ml.chat_id AND t.message_id = ml.message_id
        ${labelerFilter ? "WHERE ml.labeler = ?" : ""}
      `
        )
        .all(...(labelerFilter ? [labelerFilter] : []))
  );

  const patternScoresMap = new Map();
  const autoLabelMap = new Map();
  const manualLabelMap = new Map();

  for (const l of labels) {
    const key = `${l.chat_id}:${l.message_id}`;
    if (labelerFilter && l.labeler !== labelerFilter) continue;
    if (l.labeler === "" && !manualLabelMap.has(key)) {
      manualLabelMap.set(key, l);
    }
    if (l.labeler && l.labeler.startsWith("auto")) {
      const prev = autoLabelMap.get(key);
      if (!prev || (prev.updated_at || "") < (l.updated_at || "")) {
        autoLabelMap.set(key, l);
      }
    }
    if (l.pattern_tag) {
      const conf = numberOrNull(l.confidence);
      if (conf !== null) {
        const existing = patternScoresMap.get(key) || {};
        if (!existing[l.pattern_tag] || existing[l.pattern_tag] < conf) {
          existing[l.pattern_tag] = conf;
        }
        patternScoresMap.set(key, existing);
      }
    }
  }

  return { patternScoresMap, autoLabelMap, manualLabelMap, manualFlagsMap: new Map() };
}

function collectLabelsFromFlags(rows) {
  if (!rows.length) {
    return { patternScoresMap: new Map(), autoLabelMap: new Map(), manualLabelMap: new Map(), manualFlagsMap: new Map() };
  }
  return withTempIds(
    rows.map((r) => ({ chat_id: r.chat_id, message_id: r.message_id })),
    (table) => {
      const manualRows = db
        .prepare(
          `
          SELECT m.*
          FROM message_flags_manual m
          JOIN ${table} t ON t.chat_id = m.chat_id AND t.message_id = m.message_id
        `
        )
        .all();
      const autoRows = db
        .prepare(
          `
          SELECT a.*
          FROM message_flags_auto a
          JOIN ${table} t ON t.chat_id = a.chat_id AND t.message_id = a.message_id
        `
        )
        .all();

      const patternScoresMap = new Map();
      const autoLabelMap = new Map();
      const manualLabelMap = new Map();
      const manualFlagsMap = new Map();

      for (const row of autoRows) {
        const key = `${row.chat_id}:${row.message_id}`;
        autoLabelMap.set(key, row);
        patternScoresMap.set(key, extractAutoScores(row));
      }
      for (const row of manualRows) {
        const key = `${row.chat_id}:${row.message_id}`;
        manualLabelMap.set(key, row);
        manualFlagsMap.set(key, extractManualFlags(row));
      }

      return { patternScoresMap, autoLabelMap, manualLabelMap, manualFlagsMap };
    }
  );
}

function collectLabelData(rows, labelerFilter) {
  return HAS_MESSAGE_LABELS ? collectLabelsFromMessageLabels(rows, labelerFilter) : collectLabelsFromFlags(rows);
}

async function fetchMessagesForSet(options) {
  const { ids, minViews, maxViews, fromDate, toDate, limit, minPoints } = options;
  const useMinPoints = Number.isFinite(minPoints) && minPoints > 0;
  let rows = [];
  let total = 0;
  if (ids.length > 0) {
    const result = withTempIds(ids, (table) => {
      const conditions = [];
      const params = [];
      if (Number.isFinite(minViews)) {
        conditions.push("cm.view_count >= ?");
        params.push(minViews);
      }
      if (Number.isFinite(maxViews)) {
        conditions.push("cm.view_count <= ?");
        params.push(maxViews);
      }
      if (Number.isFinite(fromDate)) {
        conditions.push("cm.message_date >= ?");
        params.push(fromDate);
      }
      if (Number.isFinite(toDate)) {
        conditions.push("cm.message_date <= ?");
        params.push(toDate);
      }
      // minPoints handled via ClickHouse
      const where = conditions.length ? `WHERE ${conditions.join(" AND ")}` : "";
      const limitClause = Number.isFinite(limit) && limit > 0 ? `LIMIT ${limit}` : "";
      const baseSql = `
        FROM channel_messages cm
        JOIN ${table} t ON t.chat_id = cm.chat_id AND t.message_id = cm.message_id
        ${where}
      `;
      rows = db
        .prepare(
          `
          SELECT cm.chat_id, cm.message_id, cm.message_link, cm.message_date, cm.view_count
          ${baseSql}
          ORDER BY cm.message_date ASC
          ${limitClause}
        `
        )
        .all(...params);
      const totalRow = db.prepare(`SELECT COUNT(1) as cnt ${baseSql}`).get(...params);
      return { rows, total: totalRow?.cnt || 0 };
    });
    rows = result.rows;
    total = result.total;
    if (!useMinPoints) return { rows, total };
    let counts;
    try {
      counts = await fetchMetricsCounts(ids);
    } catch (err) {
      console.error("ClickHouse points fetch failed", err);
      return { rows: [], total: 0 };
    }
    rows = rows.filter((row) => (counts.get(metricsKey(row.chat_id, row.message_id)) || 0) >= minPoints);
    total = ids.reduce((acc, row) => {
      const key = metricsKey(row.chat_id, row.message_id);
      return acc + ((counts.get(key) || 0) >= minPoints ? 1 : 0);
    }, 0);
    return { rows, total };
  }

  const conditions = [];
  const params = [];
  if (Number.isFinite(minViews)) {
    conditions.push("cm.view_count >= ?");
    params.push(minViews);
  }
  if (Number.isFinite(maxViews)) {
    conditions.push("cm.view_count <= ?");
    params.push(maxViews);
  }
  if (Number.isFinite(fromDate)) {
    conditions.push("cm.message_date >= ?");
    params.push(fromDate);
  }
  if (Number.isFinite(toDate)) {
    conditions.push("cm.message_date <= ?");
    params.push(toDate);
  }
  if (Number.isFinite(minPoints)) {
    // minPoints now handled via ClickHouse
  }
  const where = conditions.length ? `WHERE ${conditions.join(" AND ")}` : "";
  const limitClause = Number.isFinite(limit) && limit > 0 ? "LIMIT ?" : "";
  if (limitClause) params.push(limit);

  rows = db
    .prepare(
      `
      SELECT cm.chat_id, cm.message_id, cm.message_link, cm.message_date, cm.view_count
      FROM channel_messages cm
      ${where}
      ORDER BY cm.message_date ASC
      ${limitClause}
    `
    )
    .all(...params);

  const totalRow = db
    .prepare(
      `
      SELECT COUNT(1) as cnt
      FROM channel_messages cm
      ${where}
    `
    )
    .get(...(conditions.length ? params.slice(0, conditions.length) : []));

  total = totalRow?.cnt || 0;
  if (!useMinPoints) return { rows, total };

  let countsForRows;
  try {
    countsForRows = await fetchMetricsCounts(rows);
  } catch (err) {
    console.error("ClickHouse points fetch failed", err);
    return { rows: [], total: 0 };
  }
  rows = rows.filter((row) => (countsForRows.get(metricsKey(row.chat_id, row.message_id)) || 0) >= minPoints);

  if (total > MINPOINTS_SCAN_LIMIT) {
    console.warn(`minPoints scan capped at ${MINPOINTS_SCAN_LIMIT} rows`);
    return { rows, total: rows.length };
  }

  const allIds = db
    .prepare(
      `
      SELECT cm.chat_id, cm.message_id
      FROM channel_messages cm
      ${where}
    `
    )
    .all(...(conditions.length ? params.slice(0, conditions.length) : []));
  let countsAll;
  try {
    countsAll = await fetchMetricsCounts(allIds);
  } catch (err) {
    console.error("ClickHouse points fetch failed", err);
    return { rows, total: rows.length };
  }
  total = allIds.reduce((acc, row) => {
    const key = metricsKey(row.chat_id, row.message_id);
    return acc + ((countsAll.get(key) || 0) >= minPoints ? 1 : 0);
  }, 0);
  return { rows, total };
}

async function buildListResult(body) {
  const ids = parseIdsList(body.ids || []);
  const minViews = Number(body.min_views);
  const maxViews = Number(body.max_views);
  const fromDate = Number(body.from_date); // seconds
  const toDate = Number(body.to_date);
  const limitRaw = Number(body.limit);
  const limit = Number.isFinite(limitRaw) && limitRaw > 0 ? Math.min(limitRaw, 5000) : 5000;
  const sortPattern = (body.sort_pattern || "").trim() || null;
  const labelerFilter = (body.labeler || "").trim() || null;
  const minPoints = Number(body.min_points);

  if (ids.length > 5000) {
    throw new Error("ids length must be <= 5000");
  }

  const { rows, total } = await fetchMessagesForSet({ ids, minViews, maxViews, fromDate, toDate, limit, minPoints });
  if (!rows || rows.length === 0) {
    return { items: [], total: 0 };
  }

  const { patternScoresMap, autoLabelMap, manualLabelMap, manualFlagsMap } = collectLabelData(rows, labelerFilter);

  const items = rows.map((row) => {
    const key = `${row.chat_id}:${row.message_id}`;
    const patternScores = patternScoresMap.get(key) || {};
    const manualFlags = manualFlagsMap.get(key) || {};
    const autoLabel = autoLabelMap.get(key) || null;
    const manualLabel = manualLabelMap.get(key) || null;
    return {
      ...row,
      pattern_scores: patternScores,
      auto: patternScores,
      manual: manualFlags,
      auto_label: autoLabel || null,
      manual_label: manualLabel || null
    };
  });

  if (sortPattern) {
    items.sort((a, b) => {
      const sa = Number.isFinite(a.pattern_scores?.[sortPattern]) ? a.pattern_scores[sortPattern] : -Infinity;
      const sb = Number.isFinite(b.pattern_scores?.[sortPattern]) ? b.pattern_scores[sortPattern] : -Infinity;
      if (sb !== sa) return sb - sa;
      return (b.view_count || 0) - (a.view_count || 0);
    });
  }

  return { items, total };
}

async function handleList(req, res, body) {
  try {
    const result = await buildListResult(body);
    return sendJson(res, 200, result);
  } catch (err) {
    console.error("handleList error", err);
    return sendJson(res, 500, { error: err.message || "List failed" });
  }
}
const server = http.createServer(async (req, res) => {
  const { pathname, query } = url.parse(req.url, true);
  try {
    if (pathname === "/api/message" && req.method === "GET") {
      return await handleMetrics(req, res, query);
    }
    if (pathname === "/api/labels" && req.method === "GET") {
      return handleLabels(req, res, query);
    }
    if (pathname === "/api/label" && req.method === "POST") {
      return handleLabelPost(req, res);
    }
    if (pathname === "/api/pattern-tags" && req.method === "GET") {
      return handlePatternTags(req, res);
    }
    if (pathname === "/api/db-stats" && req.method === "GET") {
      return handleDbStats(req, res);
    }
    if (pathname === "/api/bloggers" && req.method === "GET") {
      return handleBloggers(req, res);
    }
    if (pathname === "/api/channel-items" && req.method === "GET") {
      return handleChannelItems(req, res, query);
    }
    if (pathname === "/api/list" && req.method === "POST") {
      const body = await readJsonBody(req);
      return await handleList(req, res, body);
    }
    if (pathname.startsWith("/api/sets")) {
      const handled = await handleSets(req, res, pathname, req.method);
      if (handled !== false) return;
    }
    return serveStatic(req, res, pathname || "/");
  } catch (err) {
    console.error("Request error", err);
    res.writeHead(500);
    res.end("Server error");
  }
});

server.listen(PORT, () => {
  console.log(`Metrics visualize server at http://localhost:${PORT}`);
  console.log(`DB: ${DB_PATH}`);
});
