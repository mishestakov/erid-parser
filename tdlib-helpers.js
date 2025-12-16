"use strict";

const fs = require("node:fs/promises");
const fsSync = require("node:fs");
const path = require("node:path");
const readline = require("node:readline");
const tdl = require("tdl");
const {
  TDLIB_DATABASE_DIR,
  TDLIB_FILES_DIR,
  TDLIB_PATH
} = require("./config/paths");

const API_ID = Number(process.env.TELEGRAM_API_ID);
const API_HASH = process.env.TELEGRAM_API_HASH;

if (!Number.isFinite(API_ID) || !API_HASH) {
  throw new Error("Set TELEGRAM_API_ID and TELEGRAM_API_HASH in the environment (see .env.example).");
}

if (TDLIB_PATH && fsSync.existsSync(TDLIB_PATH)) {
  tdl.configure({ tdjson: TDLIB_PATH });
} else if (process.env.TDLIB_PATH) {
  console.warn(`TDLib binary not found at ${TDLIB_PATH}, falling back to default lookup.`);
}

async function ensureDirectories() {
  await fs.mkdir(TDLIB_DATABASE_DIR, { recursive: true });
  await fs.mkdir(TDLIB_FILES_DIR, { recursive: true });
}

function createClient() {
  return createClientWithDirs({ databaseDirectory: TDLIB_DATABASE_DIR, filesDirectory: TDLIB_FILES_DIR });
}

function createClientWithDirs({ databaseDirectory, filesDirectory }) {
  return tdl.createClient({
    apiId: API_ID,
    apiHash: API_HASH,
    databaseDirectory,
    filesDirectory,
    tdlibParameters: {
      use_test_dc: false,
      use_file_database: true,
      use_chat_info_database: true,
      use_message_database: true,
      use_secret_chats: false,
      system_language_code: "ru",
      device_model: `nodejs ${process.version}`,
      system_version: `${process.platform} ${process.arch}`,
      application_version: "0.1.0",
      enable_storage_optimizer: true,
      ignore_file_names: false
    }
  });
}

function ask(question) {
  const rl = readline.createInterface({ input: process.stdin, output: process.stdout });
  return new Promise((resolve) => {
    rl.question(question, (answer) => {
      rl.close();
      resolve(answer.trim());
    });
  });
}

async function login(client) {
  await client.login(async (retry) => {
    if (retry?.error) {
      console.error("Auth error:", retry.error.message || retry.error);
    }

    return {
      type: "user",
      getPhoneNumber: async () => ask("Введите номер телефона: "),
      getAuthCode: async () => ask("Введите код из Telegram: "),
      getPassword: async () => {
        const password = await ask("Введите пароль 2FA (если есть): ");
        return password || undefined;
      }
    };
  });
}

function delay(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

module.exports = {
  API_ID,
  API_HASH,
  TDLIB_DATABASE_DIR,
  TDLIB_FILES_DIR,
  ensureDirectories,
  createClient,
  createClientWithDirs,
  login,
  delay
};
