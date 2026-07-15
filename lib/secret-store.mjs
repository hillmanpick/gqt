import {
  createCipheriv,
  createDecipheriv,
  randomBytes,
  scryptSync,
  timingSafeEqual,
} from "node:crypto";
import { existsSync, mkdirSync, readFileSync, writeFileSync } from "node:fs";
import path from "node:path";
import { DatabaseSync } from "node:sqlite";

const SECRET_NAMES = [
  "binance_api_key",
  "binance_api_secret",
  "openai_api_key",
  "anthropic_api_key",
  "deepseek_api_key",
];

function loadMasterKey(dataRoot) {
  const fromEnvironment = process.env.CREDENTIAL_MASTER_KEY?.trim();
  if (fromEnvironment) {
    const decoded = Buffer.from(fromEnvironment, "base64");
    if (decoded.length !== 32) {
      throw new Error("CREDENTIAL_MASTER_KEY must be a base64-encoded 32-byte key");
    }
    return decoded;
  }

  const keyPath = path.join(dataRoot, ".master-key");
  if (existsSync(keyPath)) {
    const decoded = Buffer.from(readFileSync(keyPath, "utf8").trim(), "base64");
    if (decoded.length !== 32) throw new Error("Stored credential master key is invalid");
    return decoded;
  }

  const generated = randomBytes(32);
  writeFileSync(keyPath, `${generated.toString("base64")}\n`, { mode: 0o600 });
  return generated;
}

export class SecretStore {
  constructor(options = {}) {
    this.dataRoot = path.resolve(options.dataRoot ?? path.join(process.cwd(), "data"));
    mkdirSync(this.dataRoot, { recursive: true, mode: 0o700 });
    this.databasePath = path.resolve(
      options.databasePath ?? process.env.KEY_DATABASE_PATH ?? path.join(this.dataRoot, "key.db"),
    );
    mkdirSync(path.dirname(this.databasePath), { recursive: true, mode: 0o700 });
    this.masterKey = options.masterKey ?? loadMasterKey(this.dataRoot);
    this.database = new DatabaseSync(this.databasePath);
    this.database.exec(`
      PRAGMA journal_mode = WAL;
      PRAGMA foreign_keys = ON;
      CREATE TABLE IF NOT EXISTS settings (
        name TEXT PRIMARY KEY,
        value TEXT NOT NULL,
        updated_at TEXT NOT NULL
      );
      CREATE TABLE IF NOT EXISTS secrets (
        name TEXT PRIMARY KEY,
        payload TEXT NOT NULL,
        updated_at TEXT NOT NULL
      );
    `);
  }

  close() {
    this.database.close();
  }

  isSetup() {
    return Boolean(this.getSetting("admin_password"));
  }

  setAdminPassword(password) {
    if (typeof password !== "string" || password.length < 12 || password.length > 256) {
      throw new Error("管理员密码需要 12-256 个字符");
    }
    const salt = randomBytes(16);
    const hash = scryptSync(password, salt, 32);
    this.setSetting(
      "admin_password",
      JSON.stringify({ salt: salt.toString("base64"), hash: hash.toString("base64") }),
    );
  }

  verifyAdminPassword(password) {
    const stored = this.getSetting("admin_password");
    if (!stored || typeof password !== "string" || password.length > 256) return false;
    try {
      const parsed = JSON.parse(stored);
      const expected = Buffer.from(parsed.hash, "base64");
      const actual = scryptSync(password, Buffer.from(parsed.salt, "base64"), expected.length);
      return timingSafeEqual(actual, expected);
    } catch {
      return false;
    }
  }

  setSecret(name, value) {
    if (!SECRET_NAMES.includes(name)) throw new Error("Unsupported secret name");
    if (typeof value !== "string" || !value.trim()) throw new Error("密钥不能为空");
    const iv = randomBytes(12);
    const cipher = createCipheriv("aes-256-gcm", this.masterKey, iv);
    const encrypted = Buffer.concat([cipher.update(value.trim(), "utf8"), cipher.final()]);
    const payload = JSON.stringify({
      version: 1,
      iv: iv.toString("base64"),
      tag: cipher.getAuthTag().toString("base64"),
      ciphertext: encrypted.toString("base64"),
    });
    this.database
      .prepare(`
        INSERT INTO secrets(name, payload, updated_at)
        VALUES (?, ?, ?)
        ON CONFLICT(name) DO UPDATE SET payload = excluded.payload, updated_at = excluded.updated_at
      `)
      .run(name, payload, new Date().toISOString());
  }

  getSecret(name) {
    if (!SECRET_NAMES.includes(name)) throw new Error("Unsupported secret name");
    const row = this.database.prepare("SELECT payload FROM secrets WHERE name = ?").get(name);
    if (!row) return null;
    const payload = JSON.parse(row.payload);
    const decipher = createDecipheriv(
      "aes-256-gcm",
      this.masterKey,
      Buffer.from(payload.iv, "base64"),
    );
    decipher.setAuthTag(Buffer.from(payload.tag, "base64"));
    return Buffer.concat([
      decipher.update(Buffer.from(payload.ciphertext, "base64")),
      decipher.final(),
    ]).toString("utf8");
  }

  removeSecret(name) {
    if (!SECRET_NAMES.includes(name)) throw new Error("Unsupported secret name");
    this.database.prepare("DELETE FROM secrets WHERE name = ?").run(name);
  }

  secretStatus() {
    const configured = new Set(
      this.database.prepare("SELECT name FROM secrets").all().map((row) => row.name),
    );
    return Object.fromEntries(SECRET_NAMES.map((name) => [name, configured.has(name)]));
  }

  setSetting(name, value) {
    this.database
      .prepare(`
        INSERT INTO settings(name, value, updated_at)
        VALUES (?, ?, ?)
        ON CONFLICT(name) DO UPDATE SET value = excluded.value, updated_at = excluded.updated_at
      `)
      .run(name, String(value), new Date().toISOString());
  }

  getSetting(name) {
    return this.database.prepare("SELECT value FROM settings WHERE name = ?").get(name)?.value ?? null;
  }
}

export const supportedSecretNames = Object.freeze([...SECRET_NAMES]);
