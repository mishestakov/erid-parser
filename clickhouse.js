"use strict";

const http = require("node:http");
const https = require("node:https");
const { URL } = require("node:url");

const DEFAULT_URL = process.env.CLICKHOUSE_URL || "http://localhost:8123";
const DEFAULT_DATABASE = process.env.CLICKHOUSE_DATABASE || "default";
const DEFAULT_USER = process.env.CLICKHOUSE_USER || "default";
const DEFAULT_PASSWORD = process.env.CLICKHOUSE_PASSWORD || "";
const DEFAULT_TIMEOUT_MS = Number(process.env.CLICKHOUSE_TIMEOUT_MS || 10000);

function sanitizeIdentifier(value, label) {
  if (typeof value !== "string" || value.length === 0) {
    throw new Error(`ClickHouse ${label} is required`);
  }
  if (!/^[A-Za-z0-9_]+(\.[A-Za-z0-9_]+)?$/.test(value)) {
    throw new Error(`Invalid ClickHouse ${label}: ${value}`);
  }
  return value;
}

function createClickhouseClient(options = {}) {
  const baseUrl = options.url || DEFAULT_URL;
  const database = options.database || DEFAULT_DATABASE;
  const user = options.user || DEFAULT_USER;
  const password = options.password || DEFAULT_PASSWORD;
  const timeoutMs = Number.isFinite(Number(options.timeoutMs))
    ? Number(options.timeoutMs)
    : DEFAULT_TIMEOUT_MS;

  const request = (query, body = "") =>
    new Promise((resolve, reject) => {
      const target = new URL(baseUrl);
      if (database) target.searchParams.set("database", database);
      if (user) target.searchParams.set("user", user);
      if (password) target.searchParams.set("password", password);

      const payload = body ? `${query}\n${body}` : query;
      const transport = target.protocol === "https:" ? https : http;
      const req = transport.request(
        target,
        {
          method: "POST",
          headers: {
            "Content-Type": "text/plain; charset=utf-8",
            "Content-Length": Buffer.byteLength(payload)
          },
          timeout: timeoutMs
        },
        (res) => {
          const chunks = [];
          res.on("data", (chunk) => chunks.push(chunk));
          res.on("end", () => {
            const text = Buffer.concat(chunks).toString("utf8");
            if (res.statusCode && res.statusCode >= 200 && res.statusCode < 300) {
              resolve(text);
              return;
            }
            reject(new Error(`ClickHouse ${res.statusCode || 0}: ${text}`));
          });
        }
      );

      req.on("error", reject);
      req.on("timeout", () => {
        req.destroy(new Error("ClickHouse request timeout"));
      });
      req.write(payload);
      req.end();
    });

  const exec = (sql) => request(sql);

  const queryJsonEachRow = async (sql) => {
    const text = await request(`${sql}\nFORMAT JSONEachRow`);
    const trimmed = text.trim();
    if (!trimmed) return [];
    return trimmed
      .split("\n")
      .filter(Boolean)
      .map((line) => JSON.parse(line));
  };

  const insertJsonEachRow = async (table, rows, columns = null) => {
    if (!Array.isArray(rows) || rows.length === 0) return;
    const safeTable = sanitizeIdentifier(table, "table");
    const cols = Array.isArray(columns) && columns.length > 0 ? columns.map((col) => sanitizeIdentifier(col, "column")) : null;
    const columnSql = cols ? ` (${cols.join(",")})` : "";
    const body = `${rows.map((row) => JSON.stringify(row)).join("\n")}\n`;
    const query = `INSERT INTO ${safeTable}${columnSql} FORMAT JSONEachRow`;
    await request(query, body);
  };

  const ensureMessageMetricsTable = async (table) => {
    const safeTable = sanitizeIdentifier(table, "table");
    const ddl = `
      CREATE TABLE IF NOT EXISTS ${safeTable} (
        run_id UInt64,
        chat_id Int64,
        message_id Int64,
        ts DateTime('UTC'),
        view_count Nullable(UInt32),
        forward_count Nullable(UInt32),
        reply_count Nullable(UInt32),
        reactions_total Nullable(UInt32),
        reactions_paid Nullable(UInt32),
        reactions_free Nullable(UInt32)
      )
      ENGINE = MergeTree
      PARTITION BY toDate(ts)
      ORDER BY (chat_id, message_id, ts, run_id)
    `;
    await exec(ddl);
  };

  return {
    exec,
    queryJsonEachRow,
    insertJsonEachRow,
    ensureMessageMetricsTable
  };
}

module.exports = {
  createClickhouseClient
};
