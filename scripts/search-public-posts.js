"use strict";

/**
 * Выполняет searchPublicPosts с заданным запросом:
 * - собирает все страницы и пишет ссылки на посты в текстовый файл;
 * - складывает все прилетевшие чатовые апдейты (chat/supergroup/full info) и сообщения в одну SQLite-таблицу.
 */

const fs = require("node:fs/promises");
const path = require("node:path");
const { DatabaseSync } = require("node:sqlite");
const {
  createClientWithDirs,
  login,
  ensureDirectories,
  delay
} = require("../tdlib-helpers");
const {
  PUBLIC_SEARCH_DB_PATH,
  PUBLIC_SEARCH_ACCOUNTS_CONFIG,
  TDLIB_DATABASE_DIR,
  TDLIB_FILES_DIR
} = require("../config/paths");

const MAX_LIMIT = 100;
const DEFAULT_LIMIT = Number(process.env.PUBLIC_SEARCH_LIMIT || MAX_LIMIT);
const DEFAULT_DELAY_MS = Number(process.env.PUBLIC_SEARCH_DELAY_MS || 500);
const DEFAULT_PERIOD_MS = Number(process.env.PUBLIC_SEARCH_PERIOD_MS || 1 * 1000);
const MESSAGE_ID_SHIFT = 20;
const MESSAGE_ID_MULTIPLIER = 1 << MESSAGE_ID_SHIFT;
const DEFAULT_DB_PATH = PUBLIC_SEARCH_DB_PATH;
const DEFAULT_QUERY =
  process.env.PUBLIC_SEARCH_QUERY || "erid";
const ACCOUNTS_CONFIG_PATH = PUBLIC_SEARCH_ACCOUNTS_CONFIG;
const STAR_SPEND = Number.isFinite(Number(process.env.PUBLIC_SEARCH_STAR_SPEND))
  ? Number(process.env.PUBLIC_SEARCH_STAR_SPEND)
  : 10;

const chatUsernames = new Map(); // chat_id -> username|null
const chatFetches = new Map(); // chat_id -> Promise<void>
let shuttingDown = false;
let runNumber = 0;
let currentAccount = null;
let cancelSleep = null;
let currentRunId = null;
const allowedMessages = new Map(); // chat_id -> Set(message_id)
const albumKeeper = new Map(); // `${chat_id}:${album_id}` -> message_id
const lastMetrics = new Map(); // `${chat_id}:${message_id}` -> last snapshot to skip duplicates

function boolToInt(value) {
  return typeof value === "boolean" ? (value ? 1 : 0) : null;
}

function extractUsername(usernames) {
  if (!usernames || !Array.isArray(usernames.active_usernames) || usernames.active_usernames.length === 0) return null;
  return usernames.active_usernames[0];
}

function chatIdToChannelId(chatId) {
  if (!Number.isFinite(chatId)) return null;
  const abs = Math.abs(Number(chatId));
  if (abs < 1000000000000) return null;
  return abs - 1000000000000;
}

function toChatIdFromSupergroup(supergroupId) {
  if (!Number.isFinite(supergroupId)) return null;
  return -1000000000000 - Number(supergroupId);
}

function computeFreeAt(limits) {
  const nextFree = Number(limits?.next_free_query_in);
  if (!Number.isFinite(nextFree) || nextFree <= 0) return null;
  return new Date(Date.now() + nextFree * 1000).toISOString();
}

function getRemainingFree(limits) {
  const val = limits?.remaining_free_query_count;
  return Number.isFinite(val) ? val : 0;
}

function isFreeReady(account) {
  if (!account) return false;
  if (Number.isFinite(account.remaining_free) && account.remaining_free > 0) return true;
  const freeAt = account.free_at ? Date.parse(account.free_at) : NaN;
  return Number.isFinite(freeAt) && freeAt <= Date.now();
}

function freeAtMs(account) {
  const freeAt = account?.free_at ? Date.parse(account.free_at) : NaN;
  return Number.isFinite(freeAt) ? freeAt : null;
}

function findNextFreeAccountIndex(accounts, fromIndex) {
  const total = accounts.length;
  if (total === 0) return null;
  for (let step = 0; step < total; step += 1) {
    const idx = (fromIndex + step) % total;
    if (isFreeReady(accounts[idx])) {
      return idx;
    }
  }
  return null;
}

function findStarAccountIndex(accounts, minCost) {
  for (let i = 0; i < accounts.length; i += 1) {
    const acc = accounts[i];
    if (acc?.skip_stars) continue;
    const cost = Number.isFinite(acc?.star_cost_per_query) ? acc.star_cost_per_query : null;
    const balance = Number.isFinite(acc?.star_balance) ? acc.star_balance : null;
    if (cost !== null && cost >= minCost && balance !== null && balance > 0) {
      return i;
    }
  }
  return null;
}

function nextFreeTimestamp(accounts) {
  const now = Date.now();
  let ts = null;
  for (const acc of accounts) {
    const freeTs = freeAtMs(acc);
    if (freeTs && freeTs > now && (ts === null || freeTs < ts)) {
      ts = freeTs;
    }
  }
  return ts;
}

function sanitizeAccount(acc) {
  if (!acc) return null;
  const starCost =
    Number.isFinite(acc.star_cost_per_query)
      ? acc.star_cost_per_query
      : Number.isFinite(acc.last_star_count)
        ? acc.last_star_count
        : typeof acc.last_limits?.star_count === "string"
          ? Number(acc.last_limits.star_count)
          : Number(acc.last_limits?.star_count);
  const remainingFree = Number.isFinite(acc.remaining_free)
    ? acc.remaining_free
    : Number.isFinite(acc.last_remaining_free)
      ? acc.last_remaining_free
      : getRemainingFree(acc.last_limits);
  const nextFreeIn = Number.isFinite(acc.next_free_in)
    ? acc.next_free_in
    : Number.isFinite(acc.next_free_query_in)
      ? acc.next_free_query_in
      : Number(acc.last_limits?.next_free_query_in);
  const freeAt =
    acc.free_at ||
    (Number.isFinite(nextFreeIn) && nextFreeIn > 0 ? new Date(Date.now() + nextFreeIn * 1000).toISOString() : null);

  return {
    name: acc.name || "default",
    database_directory: acc.database_directory,
    files_directory: acc.files_directory,
    last_checked_at: acc.last_checked_at || null,
    daily_free: Number.isFinite(acc.daily_free)
      ? acc.daily_free
      : acc.last_limits?.daily_free_query_count ?? null,
    remaining_free: Number.isFinite(remainingFree) ? remainingFree : null,
    next_free_in: Number.isFinite(nextFreeIn) ? nextFreeIn : null,
    free_at: freeAt,
    star_cost_per_query: Number.isFinite(starCost) ? starCost : null,
    skip_stars: Boolean(acc.skip_stars),
    star_balance: Number.isFinite(acc.star_balance) ? acc.star_balance : null
  };
}

function aggregateReactions(interactionInfo) {
  const reactions = interactionInfo?.reactions?.reactions;
  if (!Array.isArray(reactions)) {
    return { total: null, paid: null, free: null };
  }
  let hasData = false;
  let paid = 0;
  let free = 0;
  for (const reaction of reactions) {
    const count = Number(reaction?.total_count);
    if (!Number.isFinite(count)) continue;
    hasData = true;
    if (reaction?.type?._ === "reactionTypePaid") {
      paid += count;
    } else {
      free += count;
    }
  }
  if (!hasData || paid + free <= 0) {
    return { total: null, paid: null, free: null };
  }
  return { total: paid + free, paid, free };
}

function extractPlainText(message) {
  const content = message?.content;
  if (!content) return null;
  if (content.text?.text) return content.text.text;
  if (content.caption?.text) return content.caption.text;
  return null;
}

function mapMessageToJson(message) {
  if (!message || typeof message.id !== "number") return null;
  const reactions = aggregateReactions(message.interaction_info);
  const row = {
    message_id: message.id,
    chat_id: message.chat_id ?? null,
    message_date: message.date ?? null,
    content_type: message.content?._ || null,
    text_markdown: extractPlainText(message)
  };

  const forwardCount = message.interaction_info?.forward_count;
  row.forward_count = Number.isFinite(forwardCount) && forwardCount > 0 ? forwardCount : null;

  const replyCount = message.interaction_info?.reply_info?.reply_count;
  row.reply_count = Number.isFinite(replyCount) && replyCount > 0 ? replyCount : null;

  const viewCount = message.interaction_info?.view_count;
  row.view_count = Number.isFinite(viewCount) && viewCount > 0 ? viewCount : null;

  if (Number.isFinite(reactions.total)) {
    row.reactions = {
      total: reactions.total,
      paid: reactions.paid,
      free: reactions.free
    };
  }

  return row;
}

function initDb(dbPath) {
  const db = new DatabaseSync(dbPath);
  db.exec("PRAGMA journal_mode=WAL;");
  db.exec("PRAGMA synchronous=NORMAL;");
  db.exec(`
    CREATE TABLE IF NOT EXISTS public_search (
      chat_id INTEGER PRIMARY KEY,
      title TEXT,
      supergroup_id INTEGER,
      boost_level INTEGER,
      date INTEGER,
      has_direct_messages_group INTEGER,
      has_linked_chat INTEGER,
      member_count INTEGER,
      active_username TEXT,
      description TEXT,
      direct_messages_chat_id INTEGER,
      gift_count INTEGER,
      linked_chat_id INTEGER,
      outgoing_paid_message_star_count INTEGER,
      found_messages_count INTEGER DEFAULT 0,
      updated_at TEXT DEFAULT (datetime('now'))
    );

    CREATE TABLE IF NOT EXISTS channel_messages (
      chat_id INTEGER NOT NULL,
      message_id INTEGER NOT NULL,
      link_message_id INTEGER,
      message_link TEXT,
      message_date INTEGER,
      content_type TEXT,
      text_markdown TEXT,
      view_count INTEGER,
      forward_count INTEGER,
      reply_count INTEGER,
      reactions_total INTEGER,
      reactions_paid INTEGER,
      reactions_free INTEGER,
      inserted_at TEXT DEFAULT (datetime('now')),
      PRIMARY KEY(chat_id, message_id)
    );

    CREATE INDEX IF NOT EXISTS idx_channel_messages_chat ON channel_messages(chat_id);

    CREATE TABLE IF NOT EXISTS runs (
      run_id INTEGER PRIMARY KEY AUTOINCREMENT,
      started_at TEXT DEFAULT (datetime('now')),
      account_name TEXT,
      limits_json TEXT,
      limits_exceeded INTEGER DEFAULT 0
    );

    CREATE TABLE IF NOT EXISTS message_metrics (
      run_id INTEGER NOT NULL,
      chat_id INTEGER NOT NULL,
      message_id INTEGER NOT NULL,
      ts INTEGER NOT NULL,
      view_count INTEGER,
      forward_count INTEGER,
      reply_count INTEGER,
      reactions_total INTEGER,
      reactions_paid INTEGER,
      reactions_free INTEGER,
      PRIMARY KEY(run_id, chat_id, message_id)
    );

    CREATE INDEX IF NOT EXISTS idx_message_metrics_chat ON message_metrics(chat_id, message_id, ts);
  `);

  const insertChannel = db.prepare(`
    INSERT INTO public_search (
      chat_id, title, supergroup_id, boost_level, date, has_direct_messages_group, has_linked_chat,
      member_count, active_username, description, direct_messages_chat_id, gift_count, linked_chat_id,
      outgoing_paid_message_star_count, found_messages_count, updated_at
    ) VALUES (
      @chat_id, @title, @supergroup_id, @boost_level, @date, @has_direct_messages_group, @has_linked_chat,
      @member_count, @active_username, @description, @direct_messages_chat_id, @gift_count, @linked_chat_id,
      @outgoing_paid_message_star_count, @found_messages_count, datetime('now')
    )
    ON CONFLICT(chat_id) DO UPDATE SET
      title = COALESCE(excluded.title, public_search.title),
      supergroup_id = COALESCE(excluded.supergroup_id, public_search.supergroup_id),
      boost_level = CASE
        WHEN public_search.boost_level IS NULL THEN excluded.boost_level
        WHEN excluded.boost_level > COALESCE(public_search.boost_level, -1) THEN excluded.boost_level
        ELSE public_search.boost_level
      END,
      date = CASE
        WHEN public_search.date IS NULL THEN excluded.date
        WHEN excluded.date > COALESCE(public_search.date, -1) THEN excluded.date
        ELSE public_search.date
      END,
      has_direct_messages_group = COALESCE(excluded.has_direct_messages_group, public_search.has_direct_messages_group),
      has_linked_chat = COALESCE(excluded.has_linked_chat, public_search.has_linked_chat),
      member_count = CASE
        WHEN public_search.member_count IS NULL THEN excluded.member_count
        WHEN excluded.member_count > COALESCE(public_search.member_count, -1) THEN excluded.member_count
        ELSE public_search.member_count
      END,
      description = COALESCE(excluded.description, public_search.description),
      direct_messages_chat_id = COALESCE(excluded.direct_messages_chat_id, public_search.direct_messages_chat_id),
      gift_count = CASE
        WHEN public_search.gift_count IS NULL THEN excluded.gift_count
        WHEN excluded.gift_count > COALESCE(public_search.gift_count, -1) THEN excluded.gift_count
        ELSE public_search.gift_count
      END,
      linked_chat_id = COALESCE(excluded.linked_chat_id, public_search.linked_chat_id),
      outgoing_paid_message_star_count = CASE
        WHEN public_search.outgoing_paid_message_star_count IS NULL THEN excluded.outgoing_paid_message_star_count
        WHEN excluded.outgoing_paid_message_star_count > COALESCE(public_search.outgoing_paid_message_star_count, -1)
          THEN excluded.outgoing_paid_message_star_count
        ELSE public_search.outgoing_paid_message_star_count
      END,
      active_username = COALESCE(excluded.active_username, public_search.active_username),
      updated_at = datetime('now');
  `);

  const upsertMessage = db.prepare(`
    INSERT INTO channel_messages (
      chat_id, message_id, link_message_id, message_link, message_date, content_type, text_markdown,
      view_count, forward_count, reply_count, reactions_total, reactions_paid, reactions_free
    ) VALUES (
      @chat_id, @message_id, @link_message_id, @message_link, @message_date, @content_type, @text_markdown,
      @view_count, @forward_count, @reply_count, @reactions_total, @reactions_paid, @reactions_free
    )
    ON CONFLICT(chat_id, message_id) DO UPDATE SET
      link_message_id = COALESCE(excluded.link_message_id, channel_messages.link_message_id),
      message_link = COALESCE(excluded.message_link, channel_messages.message_link),
      message_date = CASE
        WHEN channel_messages.message_date IS NULL THEN excluded.message_date
        WHEN excluded.message_date > COALESCE(channel_messages.message_date, -1) THEN excluded.message_date
        ELSE channel_messages.message_date
      END,
      content_type = COALESCE(excluded.content_type, channel_messages.content_type),
      text_markdown = COALESCE(excluded.text_markdown, channel_messages.text_markdown),
      view_count = CASE
        WHEN channel_messages.view_count IS NULL THEN excluded.view_count
        WHEN excluded.view_count > COALESCE(channel_messages.view_count, -1) THEN excluded.view_count
        ELSE channel_messages.view_count
      END,
      forward_count = CASE
        WHEN channel_messages.forward_count IS NULL THEN excluded.forward_count
        WHEN excluded.forward_count > COALESCE(channel_messages.forward_count, -1) THEN excluded.forward_count
        ELSE channel_messages.forward_count
      END,
      reply_count = CASE
        WHEN channel_messages.reply_count IS NULL THEN excluded.reply_count
        WHEN excluded.reply_count > COALESCE(channel_messages.reply_count, -1) THEN excluded.reply_count
        ELSE channel_messages.reply_count
      END,
      reactions_total = CASE
        WHEN channel_messages.reactions_total IS NULL THEN excluded.reactions_total
        WHEN excluded.reactions_total > COALESCE(channel_messages.reactions_total, -1) THEN excluded.reactions_total
        ELSE channel_messages.reactions_total
      END,
      reactions_paid = CASE
        WHEN channel_messages.reactions_paid IS NULL THEN excluded.reactions_paid
        WHEN excluded.reactions_paid > COALESCE(channel_messages.reactions_paid, -1) THEN excluded.reactions_paid
        ELSE channel_messages.reactions_paid
      END,
      reactions_free = CASE
        WHEN channel_messages.reactions_free IS NULL THEN excluded.reactions_free
        WHEN excluded.reactions_free > COALESCE(channel_messages.reactions_free, -1) THEN excluded.reactions_free
        ELSE channel_messages.reactions_free
      END;
  `);

  const insertRun = db.prepare(`
    INSERT INTO runs (account_name, limits_json, limits_exceeded, started_at)
    VALUES (@account_name, @limits_json, @limits_exceeded, datetime('now'))
  `);

  const updateRun = db.prepare(`
    UPDATE runs
    SET limits_json = @limits_json, limits_exceeded = @limits_exceeded
    WHERE run_id = @run_id
  `);

  const upsertMessageMetric = db.prepare(`
    INSERT INTO message_metrics (
      run_id, chat_id, message_id, ts, view_count, forward_count, reply_count, reactions_total, reactions_paid, reactions_free
    ) VALUES (
      @run_id, @chat_id, @message_id, @ts, @view_count, @forward_count, @reply_count, @reactions_total, @reactions_paid, @reactions_free
    )
    ON CONFLICT(run_id, chat_id, message_id) DO UPDATE SET
      view_count = CASE
        WHEN message_metrics.view_count IS NULL THEN excluded.view_count
        WHEN excluded.view_count > COALESCE(message_metrics.view_count, -1) THEN excluded.view_count
        ELSE message_metrics.view_count
      END,
      forward_count = CASE
        WHEN message_metrics.forward_count IS NULL THEN excluded.forward_count
        WHEN excluded.forward_count > COALESCE(message_metrics.forward_count, -1) THEN excluded.forward_count
        ELSE message_metrics.forward_count
      END,
      reply_count = CASE
        WHEN message_metrics.reply_count IS NULL THEN excluded.reply_count
        WHEN excluded.reply_count > COALESCE(message_metrics.reply_count, -1) THEN excluded.reply_count
        ELSE message_metrics.reply_count
      END,
      reactions_total = CASE
        WHEN message_metrics.reactions_total IS NULL THEN excluded.reactions_total
        WHEN excluded.reactions_total > COALESCE(message_metrics.reactions_total, -1) THEN excluded.reactions_total
        ELSE message_metrics.reactions_total
      END,
      reactions_paid = CASE
        WHEN message_metrics.reactions_paid IS NULL THEN excluded.reactions_paid
        WHEN excluded.reactions_paid > COALESCE(message_metrics.reactions_paid, -1) THEN excluded.reactions_paid
        ELSE message_metrics.reactions_paid
      END,
      reactions_free = CASE
        WHEN message_metrics.reactions_free IS NULL THEN excluded.reactions_free
        WHEN excluded.reactions_free > COALESCE(message_metrics.reactions_free, -1) THEN excluded.reactions_free
        ELSE message_metrics.reactions_free
      END;
  `);

  const updateFoundCount = db.prepare(`
    UPDATE public_search
    SET
      found_messages_count = (SELECT COUNT(*) FROM channel_messages WHERE chat_id = @chat_id),
      updated_at = datetime('now')
    WHERE chat_id = @chat_id;
  `);

  return {
    db,
    upsertChannel: (patch) => insertChannel.run(normalizeChannelPatch(patch)),
    upsertMessage: (row) => upsertMessage.run(normalizeMessageRow(row)),
    bumpMessageCount: (chatId) => updateFoundCount.run({ chat_id: chatId }),
    insertRun: (accountName) => insertRun.run({ account_name: accountName, limits_json: null, limits_exceeded: 0 }),
    updateRun: (runId, limitsJson, limitsExceeded) =>
      updateRun.run({ run_id: runId, limits_json: limitsJson, limits_exceeded: limitsExceeded ? 1 : 0 }),
    upsertMessageMetric: (row) => upsertMessageMetric.run(row)
  };
}

function applyChatToDb(dbOps, chat) {
  if (!chat || typeof chat.id !== "number") return;
  const username = extractUsername(chat.usernames);
  if (username) {
    chatUsernames.set(chat.id, username);
  }
  const params = {
    chat_id: chat.id,
    title: chat.title || null,
    supergroup_id: chat.type?._ === "chatTypeSupergroup" ? chat.type.supergroup_id : null,
    member_count: Number.isFinite(chat.member_count) ? chat.member_count : null,
    active_username: username,
    description: chat.description || null,
    direct_messages_chat_id: Number.isFinite(chat.direct_messages_chat_id) ? chat.direct_messages_chat_id : null,
    linked_chat_id: Number.isFinite(chat.linked_chat_id) ? chat.linked_chat_id : null
  };
  const norm = normalizeChannelPatch(params);
  dbOps.upsertChannel(norm);
}

function applySupergroupToDb(dbOps, supergroup, chatIdOverride = null) {
  if (!supergroup || typeof supergroup.id !== "number") return;
  const chatId = chatIdOverride || toChatIdFromSupergroup(supergroup.id);
  if (chatId === null) return;
  const username = extractUsername(supergroup.usernames);
  if (username) {
    chatUsernames.set(chatId, username);
  }
  const params = {
    chat_id: chatId,
    supergroup_id: supergroup.id,
    boost_level: Number.isFinite(supergroup.boost_level) ? supergroup.boost_level : null,
    date: Number.isFinite(supergroup.date) ? supergroup.date : null,
    has_direct_messages_group: boolToInt(supergroup.has_direct_messages_group),
    has_linked_chat: boolToInt(supergroup.has_linked_chat),
    member_count: Number.isFinite(supergroup.member_count) ? supergroup.member_count : null,
    active_username: username,
    description: supergroup.description || null,
    gift_count: Number.isFinite(supergroup.gift_count) ? supergroup.gift_count : null,
    linked_chat_id: Number.isFinite(supergroup.linked_chat_id) ? supergroup.linked_chat_id : null,
    outgoing_paid_message_star_count: Number.isFinite(supergroup.outgoing_paid_message_star_count)
      ? supergroup.outgoing_paid_message_star_count
      : null
  };
  const norm = normalizeChannelPatch(params);
  dbOps.upsertChannel(norm);
}

function applyFullInfoToDb(dbOps, supergroupId, fullInfo, chatIdOverride = null) {
  if (!fullInfo || !Number.isFinite(supergroupId)) return;
  const chatId = chatIdOverride || toChatIdFromSupergroup(supergroupId);
  if (chatId === null) return;
  const params = {
    chat_id: chatId,
    supergroup_id: supergroupId,
    member_count: Number.isFinite(fullInfo.member_count) ? fullInfo.member_count : null,
    description: fullInfo.description || null,
    direct_messages_chat_id: Number.isFinite(fullInfo.direct_messages_chat_id) ? fullInfo.direct_messages_chat_id : null,
    gift_count: Number.isFinite(fullInfo.gift_count) ? fullInfo.gift_count : null,
    linked_chat_id: Number.isFinite(fullInfo.linked_chat_id) ? fullInfo.linked_chat_id : null,
    outgoing_paid_message_star_count: Number.isFinite(fullInfo.outgoing_paid_message_star_count)
      ? fullInfo.outgoing_paid_message_star_count
      : null
  };
  const norm = normalizeChannelPatch(params);
  dbOps.upsertChannel(norm);
}

function decodeMessageId(rawMessageId) {
  if (!Number.isFinite(rawMessageId)) return null;
  const decoded = Math.floor(Number(rawMessageId) / MESSAGE_ID_MULTIPLIER);
  return decoded > 0 ? decoded : null;
}

function normalizeMessage(message) {
  const mapped = mapMessageToJson(message);
  if (!mapped || !Number.isFinite(mapped.chat_id)) return null;
  const linkMessageId = decodeMessageId(message.id);
  mapped.link_message_id = Number.isFinite(linkMessageId) ? linkMessageId : null;
  const linkTarget = mapped.link_message_id ?? mapped.message_id;
  mapped.message_link = buildPostLink(mapped.chat_id, linkTarget) || null;
  return mapped;
}

async function delayWithStop(ms, stopSignal) {
  if (stopSignal && stopSignal()) return;
  return new Promise((resolve) => {
    const timer = setTimeout(() => {
      cancelSleep = null;
      resolve();
    }, ms);
    cancelSleep = () => {
      clearTimeout(timer);
      cancelSleep = null;
      resolve();
    };
  });
}

async function readAccounts() {
  try {
    const raw = await fs.readFile(ACCOUNTS_CONFIG_PATH, "utf8");
    const parsed = JSON.parse(raw);
    const arr = Array.isArray(parsed) ? parsed : Array.isArray(parsed.accounts) ? parsed.accounts : [];
    return arr
      .map(sanitizeAccount)
      .filter(Boolean);
  } catch (_) {
    // ignore
  }
  return [];
}

async function writeAccounts(accounts) {
  await fs.mkdir(path.dirname(ACCOUNTS_CONFIG_PATH), { recursive: true });
  const compact = accounts
    .map(sanitizeAccount)
    .filter(Boolean);
  await fs.writeFile(ACCOUNTS_CONFIG_PATH, JSON.stringify(compact, null, 2), "utf8");
}

function ensureDefaultAccount(accounts) {
  if (accounts.length > 0) return accounts;
  return [
    sanitizeAccount({
      name: "default",
      database_directory: TDLIB_DATABASE_DIR,
      files_directory: TDLIB_FILES_DIR
    })
  ];
}

async function ensureAccountDirs(account) {
  if (account?.database_directory) {
    await fs.mkdir(account.database_directory, { recursive: true });
  }
  if (account?.files_directory) {
    await fs.mkdir(account.files_directory, { recursive: true });
  }
}

function normalizeChannelPatch(patch) {
  return {
    chat_id: patch.chat_id,
    title: patch.title ?? null,
    supergroup_id: patch.supergroup_id ?? null,
    boost_level: Number.isFinite(patch.boost_level) ? patch.boost_level : null,
    date: Number.isFinite(patch.date) ? patch.date : null,
    has_direct_messages_group: patch.has_direct_messages_group ?? null,
    has_linked_chat: patch.has_linked_chat ?? null,
    member_count: Number.isFinite(patch.member_count) ? patch.member_count : null,
    active_username: patch.active_username ?? null,
    description: patch.description ?? null,
    direct_messages_chat_id: Number.isFinite(patch.direct_messages_chat_id) ? patch.direct_messages_chat_id : null,
    gift_count: Number.isFinite(patch.gift_count) ? patch.gift_count : null,
    linked_chat_id: Number.isFinite(patch.linked_chat_id) ? patch.linked_chat_id : null,
    outgoing_paid_message_star_count: Number.isFinite(patch.outgoing_paid_message_star_count)
      ? patch.outgoing_paid_message_star_count
      : null,
    found_messages_count: patch.found_messages_count ?? 0
  };
}

function normalizeMessageRow(row) {
  return {
    chat_id: row.chat_id,
    message_id: row.message_id,
    link_message_id: Number.isFinite(row.link_message_id) ? row.link_message_id : null,
    message_link: row.message_link || null,
    message_date: Number.isFinite(row.message_date) ? row.message_date : null,
    content_type: row.content_type || null,
    text_markdown: row.text_markdown || null,
    view_count: Number.isFinite(row.view_count) ? row.view_count : null,
    forward_count: Number.isFinite(row.forward_count) ? row.forward_count : null,
    reply_count: Number.isFinite(row.reply_count) ? row.reply_count : null,
    reactions_total: Number.isFinite(row.reactions_total) ? row.reactions_total : null,
    reactions_paid: Number.isFinite(row.reactions_paid) ? row.reactions_paid : null,
    reactions_free: Number.isFinite(row.reactions_free) ? row.reactions_free : null
  };
}

function isAlbumDuplicate(message) {
  const albumId = message?.media_album_id;
  const chatId = message?.chat_id;
  if (!Number.isFinite(albumId) || !Number.isFinite(chatId) || !Number.isFinite(message?.id)) return false;
  const key = `${chatId}:${albumId}`;
  const existing = albumKeeper.get(key);
  if (existing === undefined) {
    albumKeeper.set(key, message.id);
    return false;
  }
  return existing !== message.id;
}

function allowMessages(messages) {
  if (!Array.isArray(messages)) return;
  for (const msg of messages) {
    const chatId = msg?.chat_id;
    const messageId = msg?.id;
    if (!Number.isFinite(chatId) || !Number.isFinite(messageId)) continue;
    let set = allowedMessages.get(chatId);
    if (!set) {
      set = new Set();
      allowedMessages.set(chatId, set);
    }
    set.add(messageId);
  }
}

function isMessageAllowed(chatId, messageId) {
  const set = allowedMessages.get(chatId);
  if (!set) return false;
  return set.has(messageId);
}

function upsertMessages(dbOps, messages) {
  if (!Array.isArray(messages) || messages.length === 0) return;
  const chatIds = new Set();
  for (const message of messages) {
    if (isAlbumDuplicate(message)) continue;
    const normalized = normalizeMessage(message);
    if (!normalized) continue;
    const normalizedRow = normalizeMessageRow({
      chat_id: normalized.chat_id,
      message_id: normalized.message_id,
      link_message_id: normalized.link_message_id ?? null,
      message_link: normalized.message_link || null,
      message_date: normalized.message_date ?? null,
      content_type: normalized.content_type || null,
      text_markdown: normalized.text_markdown || null,
      view_count: normalized.view_count,
      forward_count: normalized.forward_count,
      reply_count: normalized.reply_count,
      reactions_total: normalized.reactions?.total,
      reactions_paid: normalized.reactions?.paid,
      reactions_free: normalized.reactions?.free
    });
    dbOps.upsertMessage(normalizedRow);
    if (currentRunId !== null) {
      const last = lastMetrics.get(`${normalizedRow.chat_id}:${normalizedRow.message_id}`) || null;
      const snapshot = {
        run_id: currentRunId,
        chat_id: normalizedRow.chat_id,
        message_id: normalizedRow.message_id,
        ts: Math.floor(Date.now() / 1000),
        view_count: normalizedRow.view_count,
        forward_count: normalizedRow.forward_count,
        reply_count: normalizedRow.reply_count,
        reactions_total: normalizedRow.reactions_total,
        reactions_paid: normalizedRow.reactions_paid,
        reactions_free: normalizedRow.reactions_free
      };
      if (
        !last ||
        last.view_count !== snapshot.view_count ||
        last.forward_count !== snapshot.forward_count ||
        last.reply_count !== snapshot.reply_count ||
        last.reactions_total !== snapshot.reactions_total ||
        last.reactions_paid !== snapshot.reactions_paid ||
        last.reactions_free !== snapshot.reactions_free
      ) {
        dbOps.upsertMessageMetric(snapshot);
        lastMetrics.set(`${normalizedRow.chat_id}:${normalizedRow.message_id}`, snapshot);
      }
    }
    chatIds.add(normalized.chat_id);
  }
  for (const chatId of chatIds) {
    dbOps.bumpMessageCount(chatId);
  }
}

function extractChatIdFromUpdate(update) {
  if (!update || typeof update !== "object") return null;
  if (Number.isFinite(update.chat_id)) return update.chat_id;
  if (Number.isFinite(update.message?.chat_id)) return update.message.chat_id;
  if (Number.isFinite(update.supergroup_id)) return toChatIdFromSupergroup(update.supergroup_id);
  if (Number.isFinite(update.supergroup?.id)) return toChatIdFromSupergroup(update.supergroup.id);
  if (Number.isFinite(update.chat?.id)) return update.chat.id;
  return null;
}

function attachUpdateProcessor(client, dbOps, targets) {
  const handler = (update) => {
    if (shuttingDown) return;
    const relatedChatId = extractChatIdFromUpdate(update);
    if (relatedChatId === null || !targets.has(relatedChatId)) return;

    try {
      if (update.chat) {
        applyChatToDb(dbOps, update.chat);
      }
      if (update.supergroup) {
        applySupergroupToDb(dbOps, update.supergroup);
      }
      if (update.supergroup_full_info) {
        applyFullInfoToDb(dbOps, update.supergroup_id, update.supergroup_full_info);
      }
      if (update.message_interaction_info && Number.isFinite(update.message_interaction_info.message_id)) {
        if (!isMessageAllowed(update.message_interaction_info.chat_id, update.message_interaction_info.message_id)) {
          return;
        }
        const fakeMessage = {
          id: update.message_interaction_info.message_id,
          chat_id: update.message_interaction_info.chat_id,
          interaction_info: update.message_interaction_info.interaction_info
        };
        upsertMessages(dbOps, [fakeMessage]);
      }
      if (update.message) {
        if (!isMessageAllowed(update.message.chat_id, update.message.id)) {
          return;
        }
        upsertMessages(dbOps, [update.message]);
      }
    } catch (err) {
      console.warn(`Не удалось применить апдейт: ${err.message || err}`);
    }
  };
  client.on("update", handler);
  return () => client.off("update", handler);
}

async function logLimits(client) {
  try {
    const limits = await client.invoke({ _: "getPublicPostSearchLimits" });
    console.log(`[run ${runNumber}] limits: ${JSON.stringify(limits)}`);
    return limits;
  } catch (err) {
    console.warn(`[run ${runNumber}] getPublicPostSearchLimits failed: ${err.message || err}`);
    return null;
  }
}

async function ensureChatMeta(client, dbOps, chatId) {
  if (!Number.isFinite(chatId)) return;
  if (chatUsernames.has(chatId)) return;
  if (chatFetches.has(chatId)) {
    await chatFetches.get(chatId);
    return;
  }

  const task = (async () => {
    const channelId = chatIdToChannelId(chatId);
    try {
      const chat = await client.invoke({ _: "getChat", chat_id: chatId });
      applyChatToDb(dbOps, chat);
      let username = extractUsername(chat?.usernames) || null;

      if (!username && channelId !== null) {
        try {
          const supergroup = await client.invoke({ _: "getSupergroup", supergroup_id: channelId });
          applySupergroupToDb(dbOps, supergroup, chatId);
          username = extractUsername(supergroup?.usernames) || null;
        } catch (err) {
          // ignore supergroup fetch errors
        }
      }

      chatUsernames.set(chatId, username);
    } catch (err) {
      console.warn(`Не удалось получить chat ${chatId}: ${err.message || err}`);
      chatUsernames.set(chatId, null);
    } finally {
      chatFetches.delete(chatId);
    }
  })();

  chatFetches.set(chatId, task);
  await task;
}

function buildPostLink(chatId, messageId) {
  if (!Number.isFinite(chatId) || !Number.isFinite(messageId)) return null;
  const username = chatUsernames.get(chatId);
  if (username) return `https://t.me/${username}/${messageId}`;
  const channelId = chatIdToChannelId(chatId);
  if (channelId !== null) return `https://t.me/c/${channelId}/${messageId}`;
  return null;
}

async function runSearchLoop(client, options) {
  const { query, limit, starCount, delayMs, dbOps, targets, stopSignal } = options;
  let offset = "";
  let page = 0;
  let total = 0;
  let limitsExceeded = false;
  let balanceLow = false;

  while (!stopSignal()) {
    if (page > 0) {
      await delayWithStop(delayMs, stopSignal);
    }
    console.log(`[search] page=${page} offset="${offset}" limit=${limit} star_count=${starCount}`);

    let res;
    try {
      res = await client.invoke({
        _: "searchPublicPosts",
        query,
        offset,
        limit,
        star_count: starCount
      });
    } catch (err) {
      const msg = err?.message || String(err);
      if (msg && msg.includes("BALANCE_TOO_LOW")) {
        console.warn(`[search] баланс звёзд закончился, прерываем поиск`);
        balanceLow = true;
        break;
      }
      throw err;
    }

    const messages = Array.isArray(res?.messages) ? res.messages : [];
    const chatIdsToFetch = new Set();
    for (const message of messages) {
      const chatId = message?.chat_id;
      if (Number.isFinite(chatId)) {
        targets.add(chatId);
        const supergroupId = chatIdToChannelId(chatId);
        if (Number.isFinite(supergroupId)) {
          const sgChatId = toChatIdFromSupergroup(supergroupId);
          if (Number.isFinite(sgChatId)) targets.add(sgChatId);
        }
        if (!chatUsernames.has(chatId)) {
          chatIdsToFetch.add(chatId);
        }
      }
    }
    for (const chatId of chatIdsToFetch) {
      await ensureChatMeta(client, dbOps, chatId);
    }

    let oldestTs = null;
    for (const message of messages) {
      if (Number.isFinite(message?.date)) {
        if (oldestTs === null || message.date < oldestTs) {
          oldestTs = message.date;
        }
      }
    }

    allowMessages(messages);
    upsertMessages(dbOps, messages);

    total += messages.length;
    const oldestIso = oldestTs ? new Date(oldestTs * 1000).toISOString() : "n/a";
    console.log(`[page ${page}] messages=${messages.length} total=${total} oldest=${oldestIso}`);

    if (res?.are_limits_exceeded) {
      console.warn("Search limits exceeded; stopping.");
      limitsExceeded = true;
      break;
    }
    if (!res?.next_offset || typeof res.next_offset !== "string" || res.next_offset.length === 0) {
      break;
    }
    offset = res.next_offset;
    page += 1;
  }

  return { total, limitsExceeded, balanceLow };
}

async function main() {
  const query = DEFAULT_QUERY;
  const limit = Math.min(MAX_LIMIT, Math.max(1, DEFAULT_LIMIT));
  const delayMs = DEFAULT_DELAY_MS;
  const dbPath = DEFAULT_DB_PATH;
  const accounts = ensureDefaultAccount(await readAccounts());
  let accountIndex = 0;

  await ensureDirectories();
  await fs.mkdir(path.dirname(dbPath), { recursive: true });
  const dbOps = initDb(dbPath);
  const targets = new Set();
  let stopRequested = false;
  let detachUpdates = null;
  const stopSignal = () => stopRequested;
  const stopHandler = () => {
    stopRequested = true;
    shuttingDown = true;
    console.log("\nОстановка по сигналу...");
    if (typeof cancelSleep === "function") {
      cancelSleep();
    }
  };
  process.on("SIGINT", stopHandler);
  process.on("SIGTERM", stopHandler);

  currentAccount = accounts[accountIndex];
  await ensureAccountDirs(currentAccount);
  let client = createClientWithDirs({
    databaseDirectory: currentAccount.database_directory,
    filesDirectory: currentAccount.files_directory
  });
  detachUpdates = attachUpdateProcessor(client, dbOps, targets);

  try {
    await login(client);
    console.log(`[init] account -> ${currentAccount.name || "default"}`);
    console.log(`[init] query   -> ${query}`);
    console.log(`[init] db      -> ${dbPath}`);
    console.log(`[init] accounts config -> ${ACCOUNTS_CONFIG_PATH}`);
    while (!stopRequested) {
      // предварительно узнаем лимиты текущего аккаунта и фиксируем free_at/звёзды
      const preLimits = await logLimits(client);
      let starBalance = null;
      try {
        const me = await client.invoke({ _: "getMe" });
        const incoming = await client.invoke({
          _: "getStarTransactions",
          owner_id: { _: "messageSenderUser", user_id: me.id },
          subscription_id: "",
          direction: { _: "transactionDirectionIncoming" },
          offset: "",
          limit: 1
        });
        starBalance = Number.isFinite(incoming?.star_amount?.star_count) ? incoming.star_amount.star_count : null;
      } catch (err) {
        console.warn(`[run ${runNumber + 1}] не удалось получить баланс звёзд: ${err.message || err}`);
      }
      // обновляем плоские поля
      const nextFree = Number(preLimits?.next_free_query_in);
      const starCost = typeof preLimits?.star_count === "string" ? Number(preLimits.star_count) : Number(preLimits?.star_count);
      currentAccount.last_checked_at = new Date().toISOString();
      currentAccount.daily_free = preLimits?.daily_free_query_count ?? null;
      currentAccount.remaining_free = Number.isFinite(preLimits?.remaining_free_query_count)
        ? preLimits.remaining_free_query_count
        : currentAccount.remaining_free ?? null;
      if (Number.isFinite(nextFree) && nextFree > 0) {
        currentAccount.next_free_in = nextFree;
        currentAccount.free_at = computeFreeAt(preLimits);
      }
      currentAccount.star_cost_per_query = Number.isFinite(starCost) ? starCost : null;
      currentAccount.star_balance = Number.isFinite(starBalance) ? starBalance : currentAccount.star_balance ?? null;

      await writeAccounts(accounts);

      // выбор режима: free/звёзды/переключение/сон
      const now = Date.now();
      const currentFree = Number.isFinite(currentAccount.remaining_free)
        ? currentAccount.remaining_free
        : getRemainingFree(preLimits);
      const freeReadyIdx = currentFree > 0 ? accountIndex : findNextFreeAccountIndex(accounts, accountIndex + 1);
      if (freeReadyIdx !== null && freeReadyIdx !== accountIndex) {
        console.log(
          `[run ${runNumber + 1}] switching to account ${accounts[freeReadyIdx].name || "default"} (free quota ready)`
        );
        if (detachUpdates) {
          try {
            detachUpdates();
          } catch (_) {
            /* ignore */
          }
        }
        try {
          await client.close();
          client.destroy();
        } catch (_) {
          /* ignore */
        }
        accountIndex = freeReadyIdx;
        currentAccount = accounts[accountIndex];
        await ensureAccountDirs(currentAccount);
        client = createClientWithDirs({
          databaseDirectory: currentAccount.database_directory,
          filesDirectory: currentAccount.files_directory
        });
        detachUpdates = attachUpdateProcessor(client, dbOps, targets);
        await login(client);
        continue;
      }

      const freeReady = freeReadyIdx !== null && freeReadyIdx === accountIndex && currentFree > 0;
      let useStars = false;
      if (!freeReady) {
        const starIdx = findStarAccountIndex(accounts, STAR_SPEND);
        if (starIdx === null) {
          const nextTs = nextFreeTimestamp(accounts);
          const sleepMs = nextTs ? Math.max(1000, nextTs - now) : DEFAULT_PERIOD_MS;
          console.log(`[run ${runNumber + 1}] free=0 у всех и звёзд нет, спим ${sleepMs} мс до ближайшего free`);
          await delayWithStop(sleepMs, stopSignal);
          continue;
        }
        if (starIdx !== accountIndex) {
          console.log(
            `[run ${runNumber + 1}] switching to account ${accounts[starIdx].name || "default"} (use stars)`
          );
          if (detachUpdates) {
            try {
              detachUpdates();
            } catch (_) {
              /* ignore */
            }
          }
          try {
            await client.close();
            client.destroy();
          } catch (_) {
            /* ignore */
          }
          accountIndex = starIdx;
          currentAccount = accounts[accountIndex];
          await ensureAccountDirs(currentAccount);
          client = createClientWithDirs({
            databaseDirectory: currentAccount.database_directory,
            filesDirectory: currentAccount.files_directory
          });
          detachUpdates = attachUpdateProcessor(client, dbOps, targets);
          await login(client);
          continue;
        }
        const starCost = currentAccount.star_cost_per_query ?? STAR_SPEND;
        if (!Number.isFinite(starCost) || starCost <= 0) {
          const nextTs = nextFreeTimestamp(accounts);
          const sleepMs = nextTs ? Math.max(1000, nextTs - now) : DEFAULT_PERIOD_MS;
          console.log(`[run ${runNumber + 1}] на звёздном аккаунте звёзд нет, спим ${sleepMs} мс до free`);
          await delayWithStop(sleepMs, stopSignal);
          continue;
        }
        useStars = true;
      }

      const starCountForRun = useStars ? (currentAccount.star_cost_per_query || STAR_SPEND) : 0;

      runNumber += 1;
      currentRunId = null;
      console.log(`[run ${runNumber}] start (account=${currentAccount.name || "default"}${useStars ? ", stars=1" : ""})`);
      try {
        const info = dbOps.insertRun(currentAccount.name || "default");
        currentRunId = info?.lastInsertRowid ?? null;
      } catch (err) {
        console.warn(`[run ${runNumber}] не удалось создать запись run: ${err.message || err}`);
      }
      await delayWithStop(delayMs, stopSignal);

      const { total, limitsExceeded, balanceLow } = await runSearchLoop(client, {
        query,
        limit,
        starCount: starCountForRun,
        delayMs,
        dbOps,
        targets,
        stopSignal
      });
      console.log(`[run ${runNumber}] Готово. Всего сообщений: ${total}`);
      if (balanceLow) {
        console.warn(`[run ${runNumber}] остановка поиска: на аккаунте нет звёзд, ждём бесплатные лимиты`);
        currentAccount.skip_stars = true;
      }
      const limits = limitsExceeded || balanceLow || useStars ? await logLimits(client) : preLimits;
      const limitsZero =
        limitsExceeded ||
        (limits && typeof limits.remaining_free_query_count === "number" && limits.remaining_free_query_count <= 0);
      if (currentRunId !== null) {
        try {
          dbOps.updateRun(currentRunId, limits ? JSON.stringify(limits) : null, limitsZero);
        } catch (err) {
          console.warn(`[run ${runNumber}] не удалось обновить run: ${err.message || err}`);
        }
      }
      const nextFreeAfter = Number(limits?.next_free_query_in);
      const starCostAfter =
        typeof limits?.star_count === "string" ? Number(limits.star_count) : Number(limits?.star_count);
      currentAccount.last_checked_at = new Date().toISOString();
      currentAccount.daily_free =
        limits?.daily_free_query_count ?? (Number.isFinite(currentAccount.daily_free) ? currentAccount.daily_free : null);
      currentAccount.remaining_free = Number.isFinite(limits?.remaining_free_query_count)
        ? limits.remaining_free_query_count
        : Number.isFinite(currentAccount.remaining_free)
          ? currentAccount.remaining_free
          : null;
      if (Number.isFinite(nextFreeAfter) && nextFreeAfter > 0) {
        currentAccount.next_free_in = nextFreeAfter;
        currentAccount.free_at = computeFreeAt(limits);
      }
      currentAccount.star_cost_per_query = Number.isFinite(starCostAfter)
        ? starCostAfter
        : Number.isFinite(currentAccount.star_cost_per_query)
          ? currentAccount.star_cost_per_query
          : null;
      currentAccount.star_balance = Number.isFinite(starBalance)
        ? starBalance
        : Number.isFinite(currentAccount.star_balance)
          ? currentAccount.star_balance
          : null;
      await writeAccounts(accounts);
      if (stopRequested) break;

      // если жгли звёзды, но free где-то уже готов — переключимся сразу
      if (useStars && accounts.length > 1) {
        const freeIdx = findNextFreeAccountIndex(accounts, 0);
        if (freeIdx !== null && freeIdx !== accountIndex) {
          console.log(
            `[run ${runNumber}] free восстановился на аккаунте ${accounts[freeIdx].name || "default"}, переключаемся`
          );
          if (detachUpdates) {
            try {
              detachUpdates();
            } catch (_) {
              // ignore
            }
          }
          try {
            await client.close();
            client.destroy();
          } catch (_) {
            // ignore
          }
          accountIndex = freeIdx;
          currentAccount = accounts[accountIndex];
          await ensureAccountDirs(currentAccount);
          client = createClientWithDirs({
            databaseDirectory: currentAccount.database_directory,
            filesDirectory: currentAccount.files_directory
          });
          detachUpdates = attachUpdateProcessor(client, dbOps, targets);
          await login(client);
          continue; // сразу следующий run без сна
        }
      }

      // если free закончился на текущем и мы не на звёздах — переключаемся на следующий free
      if (!useStars && limitsZero && accounts.length > 1) {
        const nextFreeIdx = findNextFreeAccountIndex(accounts, accountIndex + 1);
        if (nextFreeIdx !== null && nextFreeIdx !== accountIndex) {
          console.log(`[run ${runNumber}] limits zero, switching to ${accounts[nextFreeIdx].name || "default"}`);
          if (detachUpdates) {
            try {
              detachUpdates();
            } catch (_) {
              // ignore
            }
          }
          try {
            await client.close();
            client.destroy();
          } catch (_) {
            // ignore
          }
          accountIndex = nextFreeIdx;
          currentAccount = accounts[accountIndex];
          await ensureAccountDirs(currentAccount);
          client = createClientWithDirs({
            databaseDirectory: currentAccount.database_directory,
            filesDirectory: currentAccount.files_directory
          });
          detachUpdates = attachUpdateProcessor(client, dbOps, targets);
          await login(client);
          continue; // сразу следующий run без сна
        }
      }

      let sleepMs = DEFAULT_PERIOD_MS;
      if (useStars) {
        const nextTs = nextFreeTimestamp(accounts);
        if (nextTs) {
          sleepMs = Math.min(sleepMs, Math.max(1000, nextTs - Date.now()));
        }
      }
      console.log(`[run ${runNumber}] sleep ${sleepMs} мс до следующего запуска...`);
      await delayWithStop(sleepMs, stopSignal);
      if (stopRequested) break;
      continue;
    }
  } catch (err) {
    console.error(`searchPublicPosts failed: ${err.message || err}`);
    process.exitCode = 1;
  } finally {
    shuttingDown = true;
    try {
      if (detachUpdates) detachUpdates();
    } catch (_) {
      // ignore
    }
    try {
      if (typeof client.close === "function") {
        await client.close();
      }
      client.destroy();
    } catch (_) {
      // ignore
    }
    try {
      dbOps.db.close();
    } catch (_) {
      // ignore
    }
  }
}

if (require.main === module) {
  main().catch((err) => {
    console.error(err);
    process.exitCode = 1;
  });
}
