use std::path::Path;

use aes_gcm::{
    Aes256Gcm, Nonce,
    aead::{Aead, KeyInit},
};
use anyhow::{Context, Result, anyhow, bail};
use base64::{Engine as _, engine::general_purpose::STANDARD};
use rand::{RngCore, rngs::OsRng};
use rusqlite::{Connection, OptionalExtension, params};
use serde::{Deserialize, Serialize};
use windows::{
    Win32::{
        Foundation::{HLOCAL, LocalFree},
        Security::Cryptography::{
            CRYPT_INTEGER_BLOB, CRYPTPROTECT_UI_FORBIDDEN, CryptProtectData, CryptUnprotectData,
        },
    },
    core::w,
};
use zeroize::Zeroizing;

use crate::model::{CredentialDraft, SecretStatus};

const SECRET_NAMES: [&str; 6] = [
    "binance_api_key",
    "binance_api_secret",
    "openai_api_key",
    "anthropic_api_key",
    "deepseek_api_key",
    "relay_api_key",
];

#[derive(Debug, Serialize, Deserialize)]
struct EncryptedPayload {
    version: u8,
    nonce: String,
    ciphertext: String,
}

pub struct SecretStore {
    connection: Connection,
}

impl SecretStore {
    pub fn open(path: &Path) -> Result<Self> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent).context("无法创建本地数据目录")?;
        }
        let connection = Connection::open(path).context("无法打开 key.db")?;
        connection.execute_batch(
            "PRAGMA journal_mode=WAL;
             PRAGMA foreign_keys=ON;
             CREATE TABLE IF NOT EXISTS settings (
                 name TEXT PRIMARY KEY,
                 value TEXT NOT NULL,
                 updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
             );
             CREATE TABLE IF NOT EXISTS secrets (
                 name TEXT PRIMARY KEY,
                 payload TEXT NOT NULL,
                 updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
             );",
        )?;
        Ok(Self { connection })
    }

    pub fn is_setup(&self) -> Result<bool> {
        Ok(self.setting("credential_dpapi_key")?.is_some())
    }

    pub fn setup(&mut self, draft: &CredentialDraft) -> Result<Zeroizing<[u8; 32]>> {
        validate_credentials(draft, true)?;
        if self.is_setup()? {
            bail!("客户端已经完成首次设置");
        }

        let mut key = Zeroizing::new([0_u8; 32]);
        OsRng.fill_bytes(key.as_mut());
        let wrapped_key = protect_for_current_user(key.as_ref())?;

        let transaction = self.connection.transaction()?;
        transaction.execute("DELETE FROM secrets", [])?;
        upsert_setting(&transaction, "credential_dpapi_key", &wrapped_key)?;
        store_draft(&transaction, &key, draft)?;
        transaction.commit()?;
        Ok(key)
    }

    pub fn unlock(&self) -> Result<Zeroizing<[u8; 32]>> {
        let wrapped_key = self
            .setting("credential_dpapi_key")?
            .ok_or_else(|| anyhow!("客户端尚未完成首次设置"))?;
        let plaintext = unprotect_for_current_user(&wrapped_key)?;
        let key: [u8; 32] = plaintext
            .as_slice()
            .try_into()
            .map_err(|_| anyhow!("Windows 凭据密钥长度无效"))?;
        Ok(Zeroizing::new(key))
    }

    pub fn secret_status(&self) -> Result<SecretStatus> {
        let configured = |name: &str| -> Result<bool> {
            Ok(self
                .connection
                .query_row("SELECT 1 FROM secrets WHERE name = ?1", [name], |_| Ok(()))
                .optional()?
                .is_some())
        };
        Ok(SecretStatus {
            binance: configured("binance_api_key")? && configured("binance_api_secret")?,
            openai: configured("openai_api_key")?,
            claude: configured("anthropic_api_key")?,
            deepseek: configured("deepseek_api_key")?,
            relay: configured("relay_api_key")?,
        })
    }

    pub fn update_credentials(&mut self, key: &[u8; 32], draft: &CredentialDraft) -> Result<()> {
        validate_credentials(draft, false)?;
        let transaction = self.connection.transaction()?;
        store_draft(&transaction, key, draft)?;
        transaction.commit()?;
        Ok(())
    }

    pub fn get_secret(&self, key: &[u8; 32], name: &str) -> Result<Option<Zeroizing<String>>> {
        if !SECRET_NAMES.contains(&name) {
            bail!("不支持的密钥名称");
        }
        let payload: Option<String> = self
            .connection
            .query_row(
                "SELECT payload FROM secrets WHERE name = ?1",
                [name],
                |row| row.get(0),
            )
            .optional()?;
        payload.map(|value| decrypt(key, &value)).transpose()
    }

    pub fn setting(&self, name: &str) -> Result<Option<String>> {
        Ok(self
            .connection
            .query_row(
                "SELECT value FROM settings WHERE name = ?1",
                [name],
                |row| row.get(0),
            )
            .optional()?)
    }

    pub fn set_setting(&self, name: &str, value: &str) -> Result<()> {
        upsert_setting(&self.connection, name, value)
    }
}

fn validate_credentials(draft: &CredentialDraft, setup: bool) -> Result<()> {
    let key_present = !draft.binance_key.trim().is_empty();
    let secret_present = !draft.binance_secret.trim().is_empty();
    if setup && (!key_present || !secret_present) {
        bail!("首次设置必须填写 Binance API Key 和 Secret");
    }
    if key_present != secret_present {
        bail!("Binance API Key 和 Secret 必须同时填写");
    }
    for value in [
        &draft.binance_key,
        &draft.binance_secret,
        &draft.openai_key,
        &draft.claude_key,
        &draft.deepseek_key,
        &draft.relay_key,
    ] {
        let length = value.trim().len();
        if length != 0 && !(10..=512).contains(&length) {
            bail!("密钥长度无效");
        }
    }
    Ok(())
}

fn store_draft(connection: &Connection, key: &[u8; 32], draft: &CredentialDraft) -> Result<()> {
    let values = [
        ("binance_api_key", draft.binance_key.trim()),
        ("binance_api_secret", draft.binance_secret.trim()),
        ("openai_api_key", draft.openai_key.trim()),
        ("anthropic_api_key", draft.claude_key.trim()),
        ("deepseek_api_key", draft.deepseek_key.trim()),
        ("relay_api_key", draft.relay_key.trim()),
    ];
    for (name, value) in values {
        if value.is_empty() {
            continue;
        }
        let payload = encrypt(key, value)?;
        connection.execute(
            "INSERT INTO secrets(name, payload, updated_at)
             VALUES (?1, ?2, CURRENT_TIMESTAMP)
             ON CONFLICT(name) DO UPDATE SET payload=excluded.payload, updated_at=CURRENT_TIMESTAMP",
            params![name, payload],
        )?;
    }
    Ok(())
}

fn encrypt(key: &[u8; 32], value: &str) -> Result<String> {
    let cipher = Aes256Gcm::new_from_slice(key).map_err(|_| anyhow!("加密密钥无效"))?;
    let mut nonce_bytes = [0_u8; 12];
    OsRng.fill_bytes(&mut nonce_bytes);
    let ciphertext = cipher
        .encrypt(Nonce::from_slice(&nonce_bytes), value.as_bytes())
        .map_err(|_| anyhow!("密钥加密失败"))?;
    Ok(serde_json::to_string(&EncryptedPayload {
        version: 1,
        nonce: STANDARD.encode(nonce_bytes),
        ciphertext: STANDARD.encode(ciphertext),
    })?)
}

fn decrypt(key: &[u8; 32], value: &str) -> Result<Zeroizing<String>> {
    let payload: EncryptedPayload = serde_json::from_str(value).context("密钥数据损坏")?;
    if payload.version != 1 {
        bail!("不支持的密钥数据版本");
    }
    let nonce = STANDARD.decode(payload.nonce)?;
    if nonce.len() != 12 {
        bail!("密钥随机数损坏");
    }
    let ciphertext = STANDARD.decode(payload.ciphertext)?;
    let cipher = Aes256Gcm::new_from_slice(key).map_err(|_| anyhow!("解密密钥无效"))?;
    let plaintext = cipher
        .decrypt(Nonce::from_slice(&nonce), ciphertext.as_ref())
        .map_err(|_| anyhow!("密钥解密失败"))?;
    Ok(Zeroizing::new(
        String::from_utf8(plaintext).context("密钥编码损坏")?,
    ))
}

fn protect_for_current_user(value: &[u8]) -> Result<String> {
    let input = CRYPT_INTEGER_BLOB {
        cbData: value.len().try_into().context("本地密钥过长")?,
        pbData: value.as_ptr().cast_mut(),
    };
    let mut output = CRYPT_INTEGER_BLOB::default();
    unsafe {
        CryptProtectData(
            &input,
            w!("GQT Trader credentials"),
            None,
            None,
            None,
            CRYPTPROTECT_UI_FORBIDDEN,
            &mut output,
        )
    }
    .map_err(|error| anyhow!("Windows 凭据加密失败: {error}"))?;
    let protected = copy_and_free_blob(output);
    Ok(STANDARD.encode(protected))
}

fn unprotect_for_current_user(value: &str) -> Result<Zeroizing<Vec<u8>>> {
    let encrypted = STANDARD.decode(value).context("Windows 凭据数据损坏")?;
    let input = CRYPT_INTEGER_BLOB {
        cbData: encrypted.len().try_into().context("Windows 凭据数据过长")?,
        pbData: encrypted.as_ptr().cast_mut(),
    };
    let mut output = CRYPT_INTEGER_BLOB::default();
    unsafe {
        CryptUnprotectData(
            &input,
            None,
            None,
            None,
            None,
            CRYPTPROTECT_UI_FORBIDDEN,
            &mut output,
        )
    }
    .map_err(|error| anyhow!("无法使用当前 Windows 账户解密凭据: {error}"))?;
    Ok(Zeroizing::new(copy_and_free_blob(output)))
}

fn copy_and_free_blob(blob: CRYPT_INTEGER_BLOB) -> Vec<u8> {
    let value = if blob.pbData.is_null() || blob.cbData == 0 {
        Vec::new()
    } else {
        unsafe { std::slice::from_raw_parts(blob.pbData, blob.cbData as usize).to_vec() }
    };
    if !blob.pbData.is_null() {
        unsafe {
            LocalFree(Some(HLOCAL(blob.pbData.cast())));
        }
    }
    value
}

fn upsert_setting(connection: &Connection, name: &str, value: &str) -> Result<()> {
    connection.execute(
        "INSERT INTO settings(name, value, updated_at)
         VALUES (?1, ?2, CURRENT_TIMESTAMP)
         ON CONFLICT(name) DO UPDATE SET value=excluded.value, updated_at=CURRENT_TIMESTAMP",
        params![name, value],
    )?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn encrypts_credentials_for_current_windows_user() {
        let path = std::env::temp_dir().join(format!("gqt-store-{}.db", rand::random::<u64>()));
        let mut store = SecretStore::open(&path).unwrap();
        let draft = CredentialDraft {
            binance_key: "fake-binance-key-for-tests".into(),
            binance_secret: "fake-binance-secret-for-tests".into(),
            relay_key: "fake-relay-key-for-tests".into(),
            ..Default::default()
        };
        let key = store.setup(&draft).unwrap();
        assert!(store.secret_status().unwrap().binance);
        assert!(store.secret_status().unwrap().relay);
        assert_eq!(
            store
                .get_secret(&key, "binance_api_key")
                .unwrap()
                .unwrap()
                .as_str(),
            draft.binance_key
        );
        assert_eq!(store.unlock().unwrap().as_ref(), key.as_ref());
        drop(store);
        let bytes = std::fs::read(&path).unwrap();
        assert!(
            !bytes
                .windows(draft.binance_key.len())
                .any(|part| part == draft.binance_key.as_bytes())
        );
        assert!(
            !bytes
                .windows(draft.relay_key.len())
                .any(|part| part == draft.relay_key.as_bytes())
        );
        let _ = std::fs::remove_file(&path);
        let _ = std::fs::remove_file(path.with_extension("db-wal"));
        let _ = std::fs::remove_file(path.with_extension("db-shm"));
    }
}
