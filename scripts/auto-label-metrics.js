"use strict";

/**
 * Авторазметка паттернов накруток по message_metrics.
 * Записывает в message_flags_auto:
 *  - колонки паттернов и ok (как score 0..1, NULL если не сработало)
 *  - scores_json (map паттерн -> score)
 *  - meta_json (min_points, version)
 *
 * Паттерны независимы: считаем score по каждому, не выбираем "лучший".
 */

const fs = require("node:fs");
const path = require("node:path");
const { DatabaseSync } = require("node:sqlite");
const { PUBLIC_SEARCH_DB_PATH, TMP_DIR } = require("../config/paths");

function parseArgs(argv) {
  const args = {};
  for (let i = 0; i < argv.length; i += 1) {
    const arg = argv[i];
    if (arg.startsWith("--")) {
      const key = arg.slice(2);
      const next = argv[i + 1];
      if (next && !next.startsWith("--")) {
        args[key] = next;
        i += 1;
      } else {
        args[key] = true;
      }
    }
  }
  return args;
}

const argv = parseArgs(process.argv.slice(2));
const DB_PATH =
  argv.db || PUBLIC_SEARCH_DB_PATH;
const SETS_PATH = argv["sets-path"] || process.env.GROWTH_SETS_PATH || path.join(TMP_DIR, "growth-sets.json");
const SET_FILTER = argv.set || argv["set-name"] || null;
const MIN_POINTS = Number(argv["min-points"] || process.env.AUTO_MIN_POINTS || 50);
const LIMIT = Number(argv.limit) || null;
const DRY_RUN = Boolean(argv["dry-run"]);
const PROGRESS_EVERY = Number.isFinite(Number(argv["progress-every"])) ? Number(argv["progress-every"]) : 1000;
const MAX_GAP_MS = 10 * 60 * 1000;

const VERSION = "auto-label-v2";

function toMs(value) {
  if (value === null || value === undefined) return null;
  if (typeof value === "number") {
    if (!Number.isFinite(value)) return null;
    return value > 1e12 ? value : value * 1000;
  }
  if (typeof value === "string") {
    const iso = value.includes("T") ? value : value.replace(" ", "T") + "Z";
    const ms = Date.parse(iso);
    return Number.isNaN(ms) ? null : ms;
  }
  return null;
}

function fetchCandidatesFromSet(db, setFilter) {
  let sets;
  try {
    const raw = fs.readFileSync(SETS_PATH, "utf8");
    sets = JSON.parse(raw);
  } catch (err) {
    throw new Error(`Не удалось прочитать ${SETS_PATH}: ${err.message || err}`);
  }
  if (!Array.isArray(sets) || sets.length === 0) {
    throw new Error(`Файл ${SETS_PATH} пуст или не массив`);
  }
  const needle = setFilter.toLowerCase();
  const found = sets.find(
    (s) =>
      String(s.id || "").toLowerCase() === needle ||
      (s.name && s.name.toLowerCase() === needle)
  );
  if (!found) throw new Error(`Подборка "${setFilter}" не найдена в ${SETS_PATH}`);
  const items = Array.isArray(found.items) ? found.items : [];
  if (!items.length) return [];
  const table = `tmp_set_${Date.now().toString(36)}`;
  db.exec(`CREATE TEMP TABLE ${table} (chat_id INTEGER NOT NULL, message_id INTEGER NOT NULL);`);
  const insert = db.prepare(`INSERT INTO ${table} (chat_id, message_id) VALUES (?, ?);`);
  db.exec("BEGIN;");
  try {
    for (const it of items) {
      const chatId = Number(it.chat_id);
      const msgId = Number(it.message_id);
      if (Number.isFinite(chatId) && Number.isFinite(msgId)) insert.run(chatId, msgId);
    }
    db.exec("COMMIT;");
  } catch (err) {
    db.exec("ROLLBACK;");
    db.exec(`DROP TABLE IF EXISTS ${table};`);
    throw err;
  }
  try {
    const rows = db
      .prepare(
        `
        SELECT cm.chat_id, cm.message_id, cm.message_link, cm.message_date, cm.view_count, agg.points
        FROM channel_messages cm
        JOIN ${table} t ON t.chat_id = cm.chat_id AND t.message_id = cm.message_id
        JOIN (
          SELECT chat_id, message_id, COUNT(*) AS points
          FROM message_metrics
          GROUP BY chat_id, message_id
        ) agg ON agg.chat_id = cm.chat_id AND agg.message_id = cm.message_id
        WHERE agg.points >= ?
        ORDER BY cm.message_date ASC, cm.chat_id ASC, cm.message_id ASC
      `
      )
      .all(MIN_POINTS);
    return rows;
  } finally {
    db.exec(`DROP TABLE IF EXISTS ${table};`);
  }
}

function fetchCandidates(db) {
  if (SET_FILTER) return fetchCandidatesFromSet(db, SET_FILTER);
  const limitClause = Number.isFinite(LIMIT) && LIMIT > 0 ? "LIMIT ?" : "";
  const params = Number.isFinite(LIMIT) && LIMIT > 0 ? [MIN_POINTS, LIMIT] : [MIN_POINTS];
  return db
    .prepare(
      `
      SELECT cm.chat_id, cm.message_id, cm.message_link, cm.message_date, cm.view_count, agg.points
      FROM channel_messages cm
      JOIN (
        SELECT chat_id, message_id, COUNT(*) AS points
        FROM message_metrics
        GROUP BY chat_id, message_id
        HAVING COUNT(*) >= ?
      ) agg ON agg.chat_id = cm.chat_id AND agg.message_id = cm.message_id
      WHERE cm.view_count IS NOT NULL
      ORDER BY cm.message_date ASC, cm.chat_id ASC, cm.message_id ASC
      ${limitClause}
    `
    )
    .all(...params);
}

function fetchSeries(db, chatId, messageId) {
  return db
    .prepare(
      `
      SELECT mm.ts, mm.view_count, r.started_at
      FROM message_metrics mm
      JOIN runs r ON r.run_id = mm.run_id
      WHERE mm.chat_id = ? AND mm.message_id = ?
      ORDER BY mm.ts ASC
    `
    )
    .all(chatId, messageId);
}

function buildSeries(metrics, messageDateSec) {
  const baseMs = Number.isFinite(messageDateSec) ? messageDateSec * 1000 : null;
  const points = [];
  for (const m of metrics) {
    const tsMs = toMs(m.ts || m.started_at);
    if (!Number.isFinite(tsMs)) continue;
    const views = Number.isFinite(m.view_count) ? m.view_count : null;
    if (!Number.isFinite(views)) continue;
    const t = baseMs !== null ? Math.max((tsMs - baseMs) / 60000, 0) : 0;
    points.push({ t, views, tsMs });
  }
  points.sort((a, b) => a.t - b.t);
  let maxViews = 0;
  for (const p of points) {
    if (!Number.isFinite(p.views)) continue;
    if (p.views < maxViews) {
      p.views = maxViews;
    } else {
      maxViews = p.views;
    }
  }
  return points;
}

function computeDeltas(series) {
  const deltas = [];
  for (let i = 0; i < series.length - 1; i += 1) {
    const curr = series[i + 1];
    const prev = series[i];
    if (!Number.isFinite(curr.tsMs) || !Number.isFinite(prev.tsMs)) continue;
    if (curr.tsMs - prev.tsMs > MAX_GAP_MS) continue;
    const delta = curr.views - prev.views;
    deltas.push({ delta, i1: i, i2: i + 1 });
  }
  return deltas;
}

function detectDelayedStart(series, deltas) {
  const { minFlatPoints, flatShareMax, spikeShareMin, minDelayMinutes, postSpikeShare } = {
    minFlatPoints: 3,
    flatShareMax: 0.02,
    spikeShareMin: 0.15,
    minDelayMinutes: 10,
    postSpikeShare: 0.7
  };
  if (series.length < minFlatPoints + 2) return null;
  const total = series[series.length - 1].views || 0;
  if (total <= 0) return null;
  const flat = [];
  for (let i = 0; i < series.length && i < minFlatPoints + 3; i += 1) {
    if ((series[i].views || 0) / total <= flatShareMax) flat.push(series[i]);
  }
  if (flat.length < minFlatPoints) return null;
  const firstBig = deltas.find((d) => d.delta / total >= spikeShareMin);
  if (!firstBig) return null;
  const spikeTime = series[firstBig.i2]?.t || 0;
  const delayOk = spikeTime >= minDelayMinutes;
  if (!delayOk) return null;
  const tail = series.slice(firstBig.i2);
  const tailGain = tail.length ? (tail[tail.length - 1].views - tail[0].views) / total : 0;
  if (tailGain < postSpikeShare) return null;
  const conf = Math.min(1, (spikeTime - minDelayMinutes) / minDelayMinutes + tailGain);
  return { tag: "zero_start_late_spike", confidence: Number(conf.toFixed(3)), note: `spike at ${spikeTime}m` };
}

function detectPlateau(series, deltas) {
  const { reachShare, tailShareMax, tailPointsMin, tailMaxDeltaShare, preJumpShareMin } = {
    reachShare: 0.8,
    tailShareMax: 0.05,
    tailPointsMin: 10,
    tailMaxDeltaShare: 0.02,
    preJumpShareMin: 0.05
  };
  if (series.length < tailPointsMin) return null;
  const total = series[series.length - 1].views || 0;
  if (total <= 0) return null;
  let reachIdx = -1;
  for (let i = 0; i < series.length; i += 1) {
    if ((series[i].views || 0) / total >= reachShare) {
      reachIdx = i;
      break;
    }
  }
  if (reachIdx === -1) return null;
  const tail = series.slice(reachIdx);
  const tailGain = tail[tail.length - 1].views - tail[0].views;
  const tailShare = tailGain / total;
  if (tailShare > tailShareMax) return null;
  const maxDelta = Math.max(...deltas.map((d) => d.delta / total));
  if (maxDelta > tailMaxDeltaShare) return null;
  const preGain = reachIdx > 0 ? (series[reachIdx].views - series[0].views) / total : 0;
  if (preGain < preJumpShareMin) return null;
  const conf = Math.min(1, preGain + (tailShareMax - tailShare));
  return {
    tag: "plateau_after_round",
    confidence: Number(conf.toFixed(3)),
    note: `reach_idx=${reachIdx}, tail_share=${tailShare.toFixed(3)}`
  };
}

function detectSteps(series, deltasInput) {
  const { coverShare, maxFraction, minJumpShare, minBigJumps, maxSmallAvgShare, minPlateauShare } = {
    coverShare: 0.9,
    maxFraction: 0.15,
    minJumpShare: 0.1,
    minBigJumps: 3,
    maxSmallAvgShare: 0.02,
    minPlateauShare: 0.5
  };
  const total = series[series.length - 1].views || 0;
  if (total <= 0) return null;
  const deltas = deltasInput.filter((d) => d.delta > 0).map((d) => d.delta / total);
  if (deltas.length < minBigJumps) return null;
  const sorted = deltas.slice().sort((a, b) => b - a);
  const cover = sorted.reduce((acc, v) => acc + v, 0);
  if (cover < coverShare) return null;
  const big = sorted.filter((v) => v >= minJumpShare);
  if (big.length < minBigJumps) return null;
  if (Math.max(...sorted) > maxFraction) return null;
  const rest = sorted.slice(minBigJumps);
  const smallAvg = rest.length ? rest.reduce((a, b) => a + b, 0) / rest.length : 0;
  if (smallAvg > maxSmallAvgShare) return null;
  const plateau = series[Math.floor(series.length * 0.7)].views / total;
  if (plateau < minPlateauShare) return null;
  const conf = Math.min(1, cover + plateau - maxFraction);
  return { tag: "steps", confidence: Number(conf.toFixed(3)), note: `cover=${cover.toFixed(3)}, plateau=${plateau.toFixed(3)}` };
}

function detectPlus30(series, deltasInput) {
  const { base, tol, minAccepted, minShareSum, minShareCount, minModeShare, minRunLength, minRunCount } = {
    base: 30,
    tol: 5,
    minAccepted: 5,
    minShareSum: 0.55,
    minShareCount: 0.55,
    minModeShare: 0.5,
    minRunLength: 3,
    minRunCount: 1
  };
  if (series.length < minAccepted) return null;
  const total = series[series.length - 1].views || 0;
  if (total <= 0) return null;
  const deltas = deltasInput.filter((d) => d.delta > 0);
  const buckets = new Map();
  for (const d of deltas) {
    const val = d.delta / total;
    const rounded = Math.round(val * 100);
    const key = Math.round((rounded - base) / tol);
    buckets.set(key, (buckets.get(key) || 0) + 1);
  }
  if (!buckets.size) return null;
  const totalCnt = deltas.length;
  let best = { key: null, count: 0 };
  for (const [k, v] of buckets.entries()) {
    if (v > best.count) best = { key: k, count: v };
  }
  const bestShare = best.count / totalCnt;
  const runKeys = Array.from(buckets.entries())
    .filter(([_, v]) => v >= minRunLength)
    .map(([k]) => k)
    .sort((a, b) => a - b);
  const runCount = runKeys.length;
  const sumShare = Array.from(buckets.values()).reduce((a, b) => a + b, 0) / totalCnt;
  if (bestShare < minModeShare || sumShare < minShareSum || runCount < minRunCount) return null;
  const conf = Math.min(1, bestShare + sumShare - (Math.abs(best.key) * tol) / 100);
  return { tag: "plus30_pattern", confidence: Number(conf.toFixed(3)), note: `mode_share=${bestShare.toFixed(3)}` };
}

function detectMicroSteps(series, deltasInput) {
  const { flatDeltaAbs, minZeroShare, minPulses, minPulseShare, minPulseTotalShare } = {
    flatDeltaAbs: 2,
    minZeroShare: 0.35,
    minPulses: 6,
    minPulseShare: 0.005,
    minPulseTotalShare: 0.35
  };
  const total = series[series.length - 1].views || 0;
  if (total <= 0) return null;
  const deltas = deltasInput.map((d) => d.delta);
  const zeroShare = deltas.filter((d) => d <= flatDeltaAbs).length / deltas.length;
  const pulses = deltas.filter((d) => d / total >= minPulseShare);
  const pulseShare = pulses.reduce((a, b) => a + b, 0) / total;
  if (zeroShare < minZeroShare) return null;
  if (pulses.length < minPulses) return null;
  if (pulseShare < minPulseTotalShare) return null;
  const conf = Math.min(1, zeroShare + pulseShare);
  return { tag: "micro_steps", confidence: Number(conf.toFixed(3)), note: `zero=${zeroShare.toFixed(3)}, pulse=${pulseShare.toFixed(3)}` };
}

function detectLinear(series, deltasInput) {
  const { r2Min, maxResidualShare, maxDeltaShare, cvMax } = {
    r2Min: 0.9,
    maxResidualShare: 0.12,
    maxDeltaShare: 0.15,
    cvMax: 0.6
  };
  const n = series.length;
  if (n < 5) return null;
  const xs = series.map((p) => p.t);
  const ys = series.map((p) => p.views);
  const meanX = xs.reduce((a, b) => a + b, 0) / n;
  const meanY = ys.reduce((a, b) => a + b, 0) / n;
  let num = 0;
  let den = 0;
  for (let i = 0; i < n; i += 1) {
    num += (xs[i] - meanX) * (ys[i] - meanY);
    den += (xs[i] - meanX) ** 2;
  }
  const slope = den === 0 ? 0 : num / den;
  const intercept = meanY - slope * meanX;
  let rss = 0;
  let tss = 0;
  let maxResidual = 0;
  let maxDelta = 0;
  for (let i = 0; i < n; i += 1) {
    const pred = slope * xs[i] + intercept;
    const resid = ys[i] - pred;
    rss += resid ** 2;
    tss += (ys[i] - meanY) ** 2;
    if (Math.abs(resid) > maxResidual) maxResidual = Math.abs(resid);
    if (i > 0) {
      const delta = ys[i] - ys[i - 1];
      if (delta > maxDelta) maxDelta = delta;
    }
  }
  const r2 = tss === 0 ? 0 : 1 - rss / tss;

  const deltas = [];
  for (const d of deltasInput) {
    if (d.delta > 0) deltas.push(d.delta);
  }
  if (deltas.length === 0) return null;
  const meanDelta = deltas.reduce((a, b) => a + b, 0) / deltas.length;
  const varDelta = deltas.reduce((a, b) => a + (b - meanDelta) ** 2, 0) / deltas.length;
  const stdDelta = Math.sqrt(varDelta);
  const cv = meanDelta > 0 ? stdDelta / meanDelta : Infinity;
  const maxD = Math.max(...deltas);
  const maxJumpShare = maxD / (series[series.length - 1].views || 1);
  const maxResidShare = maxResidual / (series[series.length - 1].views || 1);

  if (r2 < r2Min) return null;
  if (maxResidShare > maxResidualShare) return null;
  if (maxJumpShare > maxDeltaShare) return null;
  if (cv > cvMax) return null;

  const conf = Math.min(1, (r2 - r2Min + 0.1) + (1 - cv / cvMax));
  const note = `linear: r2=${r2.toFixed(3)}, max_resid_share=${maxResidShare.toFixed(
    3
  )}, max_jump_share=${maxJumpShare.toFixed(3)}, cv=${cv.toFixed(3)}`;
  return { tag: "linear_growth", confidence: Number(conf.toFixed(3)), note };
}

function detectAll(series) {
  const deltas = computeDeltas(series);
  const detectors = [
    detectDelayedStart,
    detectPlus30,
    detectLinear,
    detectMicroSteps,
    detectSteps,
    detectPlateau
  ];
  const scores = {};
  const notes = [];
  for (const fn of detectors) {
    const res = fn(series, deltas);
    if (res && res.tag) {
      scores[res.tag] = res.confidence ?? 0;
      if (res.note) notes.push(`${res.tag}:${res.note}`);
    }
  }
  return { scores, notes };
}

function upsertAuto(db, row, scores) {
  const meta = {
    version: VERSION,
    min_points: MIN_POINTS
  };
  db.prepare(
    `
    INSERT INTO message_flags_auto (chat_id, message_id, ok, zero_start_late_spike, linear_growth, steps, plateau_after_round, plus30_pattern, micro_steps, other, scores_json, meta_json, updated_at)
    VALUES (@chat_id, @message_id, NULL, @zero_start_late_spike, @linear_growth, @steps, @plateau_after_round, @plus30_pattern, @micro_steps, @other, @scores_json, @meta_json, datetime('now'))
    ON CONFLICT(chat_id, message_id) DO UPDATE SET
      ok=excluded.ok,
      zero_start_late_spike=excluded.zero_start_late_spike,
      linear_growth=excluded.linear_growth,
      steps=excluded.steps,
      plateau_after_round=excluded.plateau_after_round,
      plus30_pattern=excluded.plus30_pattern,
      micro_steps=excluded.micro_steps,
      other=excluded.other,
      scores_json=excluded.scores_json,
      meta_json=excluded.meta_json,
      updated_at=datetime('now')
  `
  ).run({
    chat_id: row.chat_id,
    message_id: row.message_id,
    zero_start_late_spike: scores.zero_start_late_spike ?? null,
    linear_growth: scores.linear_growth ?? null,
    steps: scores.steps ?? null,
    plateau_after_round: scores.plateau_after_round ?? null,
    plus30_pattern: scores.plus30_pattern ?? null,
    micro_steps: scores.micro_steps ?? null,
    other: scores.other ?? null,
    scores_json: JSON.stringify(scores),
    meta_json: JSON.stringify(meta)
  });
}

function main() {
  const db = new DatabaseSync(DB_PATH);
  db.exec("PRAGMA journal_mode=WAL;");
  db.exec("PRAGMA synchronous=OFF;");
  const candidates = fetchCandidates(db);
  console.log(
    `[auto-label] DB=${DB_PATH} min_points=${MIN_POINTS} limit=${LIMIT || "∞"} dry_run=${DRY_RUN} candidates=${candidates.length}`
  );
  const stats = new Map();
  let processed = 0;
  let visited = 0;
  for (const row of candidates) {
    visited += 1;
    const metrics = fetchSeries(db, row.chat_id, row.message_id);
    const series = buildSeries(metrics, row.message_date);
    if (series.length < MIN_POINTS) continue;
    const { scores } = detectAll(series);
    if (!scores || Object.keys(scores).length === 0) continue;
    processed += 1;
    for (const [k, v] of Object.entries(scores)) {
      if (v !== null && v !== undefined) stats.set(k, (stats.get(k) || 0) + 1);
    }
    if (!DRY_RUN) upsertAuto(db, row, scores);
    if (PROGRESS_EVERY > 0 && visited % PROGRESS_EVERY === 0) {
      const parts = [];
      for (const [tag, count] of stats.entries()) parts.push(`${tag}:${count}`);
      console.log(
        `[auto-label] processed=${visited}/${candidates.length} saved=${processed}${parts.length ? " [" + parts.join(", ") + "]" : ""}`
      );
    }
  }
  console.log("[auto-label] done, saved entries:", processed);
  for (const [tag, count] of stats.entries()) {
    console.log(`  - ${tag}: ${count}`);
  }
}

main();
