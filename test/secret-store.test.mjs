import test from "node:test";
import assert from "node:assert/strict";
import { mkdtempSync, readFileSync, readdirSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import path from "node:path";

import { SecretStore } from "../lib/secret-store.mjs";

test("SecretStore hashes passwords and encrypts API credentials at rest", () => {
  const dataRoot = mkdtempSync(path.join(tmpdir(), "gqt-secret-store-"));
  const databasePath = path.join(dataRoot, "key.db");
  const apiKey = "test-binance-api-key-not-real";
  const apiSecret = "test-binance-secret-not-real";
  const store = new SecretStore({
    dataRoot,
    databasePath,
    masterKey: Buffer.alloc(32, 7),
  });

  try {
    store.setAdminPassword("correct horse battery staple");
    store.setSecret("binance_api_key", apiKey);
    store.setSecret("binance_api_secret", apiSecret);

    assert.equal(store.verifyAdminPassword("correct horse battery staple"), true);
    assert.equal(store.verifyAdminPassword("wrong password"), false);
    assert.equal(store.getSecret("binance_api_key"), apiKey);
    assert.deepEqual(store.secretStatus(), {
      binance_api_key: true,
      binance_api_secret: true,
      openai_api_key: false,
      anthropic_api_key: false,
      deepseek_api_key: false,
    });
  } finally {
    store.close();
  }

  const storedBytes = Buffer.concat(
    readdirSync(dataRoot).map((name) => readFileSync(path.join(dataRoot, name))),
  );
  assert.equal(storedBytes.includes(Buffer.from(apiKey)), false);
  assert.equal(storedBytes.includes(Buffer.from(apiSecret)), false);
  assert.equal(storedBytes.includes(Buffer.from("correct horse battery staple")), false);
  rmSync(dataRoot, { recursive: true, force: true });
});

test("SecretStore rejects unsupported secret names and short passwords", () => {
  const dataRoot = mkdtempSync(path.join(tmpdir(), "gqt-secret-store-"));
  const store = new SecretStore({ dataRoot, masterKey: Buffer.alloc(32, 9) });
  try {
    assert.throws(() => store.setAdminPassword("too-short"), /12/);
    assert.throws(() => store.setAdminPassword("x".repeat(257)), /256/);
    assert.throws(() => store.setSecret("unknown", "long-enough-secret"), /Unsupported/);
  } finally {
    store.close();
    rmSync(dataRoot, { recursive: true, force: true });
  }
});
