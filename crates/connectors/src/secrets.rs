//! Secrets are encrypted outside RocksDB. Values never implement Debug or Serialize.
use aes_gcm::{
    aead::{rand_core::RngCore, Aead, KeyInit, OsRng},
    Aes256Gcm, Nonce,
};
use anyhow::{ensure, Context, Result};
use std::{
    fs,
    path::{Path, PathBuf},
    sync::Arc,
};
use zeroize::Zeroizing;

pub trait SecretProvider: Send + Sync {
    fn get(&self, name: &str) -> Result<Option<Zeroizing<String>>>;
}
pub struct EnvironmentSecrets;
impl SecretProvider for EnvironmentSecrets {
    fn get(&self, name: &str) -> Result<Option<Zeroizing<String>>> {
        validate_name(name)?;
        Ok(
            std::env::var(format!("SNIPER_{}", name.to_ascii_uppercase()))
                .ok()
                .map(Zeroizing::new),
        )
    }
}
pub struct SecretVault {
    root: PathBuf,
    cipher: Aes256Gcm,
    environment: Arc<dyn SecretProvider>,
}
impl SecretVault {
    pub fn new(root: PathBuf, key: &str) -> Result<Self> {
        let mut bytes = Zeroizing::new(Vec::new());
        ensure!(
            key.len() == 64 && key.bytes().all(|b| b.is_ascii_hexdigit()),
            "SNIPER_MASTER_KEY must contain 64 hex characters"
        );
        for i in (0..64).step_by(2) {
            bytes.push(u8::from_str_radix(&key[i..i + 2], 16)?);
        }
        fs::create_dir_all(&root)?;
        restrict(&root, true)?;
        Ok(Self {
            root,
            cipher: Aes256Gcm::new_from_slice(&bytes)
                .map_err(|_| anyhow::anyhow!("invalid encryption key"))?,
            environment: Arc::new(EnvironmentSecrets),
        })
    }
    pub fn save(&self, name: &str, value: &str) -> Result<()> {
        validate_name(name)?;
        ensure!(
            !value.is_empty() && value.len() < 16384,
            "invalid secret length"
        );
        let mut nonce = [0; 12];
        OsRng.fill_bytes(&mut nonce);
        let encrypted = self
            .cipher
            .encrypt(
                Nonce::from_slice(&nonce),
                aes_gcm::aead::Payload {
                    msg: value.as_bytes(),
                    aad: name.as_bytes(),
                },
            )
            .map_err(|_| anyhow::anyhow!("secret encryption failed"))?;
        let mut payload = nonce.to_vec();
        payload.extend(encrypted);
        let temporary = self.root.join(format!("{name}.pending"));
        let target = self.root.join(name);
        // Atomic replacement. Sync before rename; secret files are excluded from DB checkpoints.
        use std::io::Write;
        let mut file = fs::OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(&temporary)?;
        restrict(&temporary, false)?;
        file.write_all(&payload)?;
        file.sync_all()?;
        drop(file);
        fs::rename(&temporary, &target)?;
        Ok(())
    }
    pub fn clear(&self, name: &str) -> Result<()> {
        validate_name(name)?;
        let path = self.root.join(name);
        if path.exists() {
            fs::remove_file(path)?;
        }
        Ok(())
    }
}
impl SecretProvider for SecretVault {
    fn get(&self, name: &str) -> Result<Option<Zeroizing<String>>> {
        validate_name(name)?;
        if let Some(value) = self.environment.get(name)? {
            return Ok(Some(value));
        }
        let path = self.root.join(name);
        if !path.exists() {
            return Ok(None);
        }
        let bytes = fs::read(path)?;
        ensure!(bytes.len() >= 28, "truncated encrypted secret");
        let plaintext = Zeroizing::new(
            self.cipher
                .decrypt(
                    Nonce::from_slice(&bytes[..12]),
                    aes_gcm::aead::Payload {
                        msg: &bytes[12..],
                        aad: name.as_bytes(),
                    },
                )
                .map_err(|_| anyhow::anyhow!("secret authentication failed"))?,
        );
        Ok(Some(Zeroizing::new(
            String::from_utf8(plaintext.to_vec()).context("invalid secret encoding")?,
        )))
    }
}
fn validate_name(name: &str) -> Result<()> {
    ensure!(
        !name.is_empty()
            && name.len() <= 80
            && name.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'_'),
        "invalid secret name"
    );
    Ok(())
}
fn restrict(path: &Path, directory: bool) -> Result<()> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(
            path,
            fs::Permissions::from_mode(if directory { 0o700 } else { 0o600 }),
        )?;
    }
    #[cfg(not(unix))]
    {
        let _ = (path, directory);
    }
    Ok(())
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn authenticated_encryption() {
        let t = tempfile::tempdir().unwrap();
        let v = SecretVault::new(t.path().into(), &"ab".repeat(32)).unwrap();
        v.save("test_key", "sensitive-value").unwrap();
        assert_eq!(
            v.get("test_key").unwrap().unwrap().as_str(),
            "sensitive-value"
        );
        let bytes = fs::read(t.path().join("test_key")).unwrap();
        assert!(!String::from_utf8_lossy(&bytes).contains("sensitive-value"));
        assert!(v.save("../escape", "x").is_err());
        let bad = SecretVault::new(t.path().into(), &"cd".repeat(32)).unwrap();
        assert!(bad.get("test_key").is_err());
    }
}
