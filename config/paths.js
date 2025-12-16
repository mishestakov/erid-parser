"use strict";

require("dotenv").config();

const path = require("node:path");

const ROOT_DIR = path.join(__dirname, "..");
const DATA_DIR = process.env.DATA_DIR || path.join(ROOT_DIR, "data");
const TMP_DIR = process.env.TMP_DIR || path.join(DATA_DIR, "tmp");

const TDLIB_DATABASE_DIR =
  process.env.TDLIB_DATABASE_DIR || path.join(DATA_DIR, "tdlib", "database");
const TDLIB_FILES_DIR =
  process.env.TDLIB_FILES_DIR || path.join(DATA_DIR, "tdlib", "files");
const TDLIB_PATH = process.env.TDLIB_PATH || null;

const PUBLIC_SEARCH_DB_PATH =
  process.env.PUBLIC_SEARCH_DB_PATH || path.join(DATA_DIR, "db", "public-search.sqlite");
const PUBLIC_SEARCH_OUTPUT =
  process.env.PUBLIC_SEARCH_OUTPUT || path.join(DATA_DIR, "public-search-links.txt");
const PUBLIC_SEARCH_ACCOUNTS_CONFIG =
  process.env.PUBLIC_SEARCH_ACCOUNTS_CONFIG || path.join(DATA_DIR, "public-search-accounts.json");
const GROWTH_SETS_PATH =
  process.env.GROWTH_SETS_PATH || path.join(DATA_DIR, "growth-sets.json");

module.exports = {
  ROOT_DIR,
  DATA_DIR,
  TMP_DIR,
  TDLIB_DATABASE_DIR,
  TDLIB_FILES_DIR,
  TDLIB_PATH,
  PUBLIC_SEARCH_DB_PATH,
  PUBLIC_SEARCH_OUTPUT,
  PUBLIC_SEARCH_ACCOUNTS_CONFIG,
  GROWTH_SETS_PATH
};
