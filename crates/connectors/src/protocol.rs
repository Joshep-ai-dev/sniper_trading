//! Vendored official IDLs drive account order, privileges, discriminators and event decoding.
//! Unknown fields/accounts fail closed; no guessed account lists are submitted.
use anyhow::{bail, ensure, Context, Result};
use serde_json::{json, Value};
use sniper_domain::{Key, Venue};
use solana_sdk::{
    instruction::{AccountMeta, Instruction},
    pubkey::Pubkey,
};
use std::{collections::HashMap, str::FromStr};

pub const PUMP: &str = "6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P";
pub const SWAP: &str = "pAMMBay6oceH9fJKBRHGP5D4bD4sWpmSwMn52FMfXEA";
pub const TOKEN: &str = "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA";
pub const TOKEN_2022: &str = "TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb";
pub const ATA: &str = "ATokenGPvbdGVxr1b2hvZbsiqW5xWH25efTNsLJA8knL";
pub const WSOL: &str = "So11111111111111111111111111111111111111112";
pub fn pubkey(key: Key) -> Pubkey {
    Pubkey::new_from_array(key.0)
}
pub fn key(pk: Pubkey) -> Key {
    Key(pk.to_bytes())
}
pub fn ata(owner: Pubkey, mint: Pubkey, token: Pubkey) -> Pubkey {
    Pubkey::find_program_address(
        &[owner.as_ref(), token.as_ref(), mint.as_ref()],
        &Pubkey::from_str(ATA).expect("constant"),
    )
    .0
}
#[derive(Clone)]
pub struct Idl {
    pub value: Value,
    pub program: Pubkey,
}
impl Idl {
    pub fn load(venue: Venue) -> Self {
        let raw = match venue {
            Venue::PumpFun => include_str!("../../../protocol/idl/pump.json"),
            Venue::PumpSwap => include_str!("../../../protocol/idl/pump_amm.json"),
        };
        let value: Value = serde_json::from_str(raw).expect("vendored IDL");
        let program =
            Pubkey::from_str(value["address"].as_str().expect("address")).expect("program");
        Self { value, program }
    }
    pub fn instruction(&self, name: &str) -> Result<&Value> {
        self.value["instructions"]
            .as_array()
            .context("IDL instructions")?
            .iter()
            .find(|i| i["name"] == name)
            .context("unsupported instruction")
    }
    pub fn identify(&self, data: &[u8]) -> Option<&str> {
        self.value["instructions"]
            .as_array()?
            .iter()
            .find(|i| discriminator(i).ok().is_some_and(|d| data.starts_with(&d)))
            .and_then(|i| i["name"].as_str())
    }
    pub fn instruction_accounts(
        &self,
        name: &str,
        accounts: &[Pubkey],
    ) -> Result<HashMap<String, Pubkey>> {
        let list = self.instruction(name)?["accounts"]
            .as_array()
            .context("accounts")?;
        ensure!(
            accounts.len() >= list.len(),
            "incomplete instruction account list"
        );
        Ok(list
            .iter()
            .zip(accounts)
            .map(|(v, k)| (v["name"].as_str().unwrap_or_default().into(), *k))
            .collect())
    }
    pub fn build(
        &self,
        name: &str,
        args: &[u64],
        context: &mut HashMap<String, Pubkey>,
    ) -> Result<Instruction> {
        let instruction = self.instruction(name)?;
        let mut data = discriminator(instruction)?.to_vec();
        let account_fields = instruction["accounts"].as_array().context("accounts")?;
        for field in account_fields {
            if let Some(address) = field["address"].as_str() {
                context
                    .entry(field["name"].as_str().context("name")?.into())
                    .or_insert(Pubkey::from_str(address)?);
            }
        }
        for _ in 0..account_fields.len() {
            for field in account_fields {
                let name = field["name"].as_str().context("name")?;
                if !context.contains_key(name) && field.get("pda").is_some() {
                    if let Ok(address) = self.derive(&field["pda"], context) {
                        context.insert(name.into(), address);
                    }
                }
            }
        }
        let fields = instruction["args"].as_array().context("args")?;
        let mut supplied = args.iter();
        for field in fields {
            if field["type"] == "u64" {
                data.extend_from_slice(&supplied.next().context("missing argument")?.to_le_bytes());
            } else if field["type"]["defined"]["name"] == "OptionBool" {
                data.push(0);
            }
            // explicitly false
            else {
                bail!("unsupported instruction argument");
            }
        }
        ensure!(supplied.next().is_none(), "excess instruction argument");
        let mut accounts = Vec::new();
        for field in instruction["accounts"].as_array().context("accounts")? {
            let name = field["name"].as_str().context("account name")?;
            let address = if let Some(address) = context.get(name) {
                *address
            } else if let Some(address) = field["address"].as_str() {
                Pubkey::from_str(address)?
            } else if field.get("pda").is_some() {
                self.derive(&field["pda"], context)?
            } else {
                bail!("account cache missing {name}");
            };
            context.insert(name.into(), address);
            let signer = field["signer"].as_bool().unwrap_or(false);
            accounts.push(if field["writable"].as_bool().unwrap_or(false) {
                AccountMeta::new(address, signer)
            } else {
                AccountMeta::new_readonly(address, signer)
            });
        }
        Ok(Instruction {
            program_id: self.program,
            accounts,
            data,
        })
    }
    fn derive(&self, pda: &Value, ctx: &HashMap<String, Pubkey>) -> Result<Pubkey> {
        let mut seeds = Vec::new();
        for seed in pda["seeds"].as_array().context("PDA seeds")? {
            seeds.push(match seed["kind"].as_str() {
                Some("const") => seed["value"]
                    .as_array()
                    .context("constant seed")?
                    .iter()
                    .map(|v| v.as_u64().map(|v| v as u8).context("seed byte"))
                    .collect::<Result<Vec<_>>>()?,
                Some("account") => ctx
                    .get(seed["path"].as_str().context("seed path")?)
                    .context("PDA seed not cached")?
                    .to_bytes()
                    .to_vec(),
                _ => bail!("unsupported PDA seed"),
            });
        }
        let program = match pda["program"]["kind"].as_str() {
            Some("account") => *ctx
                .get(
                    pda["program"]["path"]
                        .as_str()
                        .context("PDA program path")?,
                )
                .context("PDA program missing")?,
            Some("const") => {
                let bytes: Vec<u8> = pda["program"]["value"]
                    .as_array()
                    .context("PDA program")?
                    .iter()
                    .map(|v| v.as_u64().unwrap_or(0) as u8)
                    .collect();
                Pubkey::new_from_array(
                    bytes
                        .try_into()
                        .map_err(|_| anyhow::anyhow!("invalid program seed"))?,
                )
            }
            None => self.program,
            _ => bail!("unsupported PDA program"),
        };
        Ok(Pubkey::find_program_address(
            &seeds.iter().map(Vec::as_slice).collect::<Vec<_>>(),
            &program,
        )
        .0)
    }
    pub fn decode_account(&self, data: &[u8]) -> Result<(String, Value)> {
        let account = self.value["accounts"]
            .as_array()
            .context("IDL accounts")?
            .iter()
            .find(|v| discriminator(v).ok().is_some_and(|d| data.starts_with(&d)))
            .context("unknown account discriminator")?;
        let name = account["name"].as_str().context("account type")?;
        let mut reader = Reader {
            data: &data[8..],
            offset: 0,
        };
        Ok((
            name.into(),
            self.decode_type(&json!({"defined":{"name":name}}), &mut reader, 0)?,
        ))
    }
    pub fn decode_event(&self, data: &[u8]) -> Result<(String, Value)> {
        let event = self.value["events"]
            .as_array()
            .context("events")?
            .iter()
            .find(|v| discriminator(v).ok().is_some_and(|d| data.starts_with(&d)))
            .context("unknown event discriminator")?;
        let name = event["name"].as_str().context("event name")?;
        let mut reader = Reader {
            data: &data[8..],
            offset: 0,
        };
        Ok((
            name.into(),
            self.decode_type(&json!({"defined":{"name":name}}), &mut reader, 0)?,
        ))
    }
    fn decode_type(&self, ty: &Value, r: &mut Reader<'_>, depth: usize) -> Result<Value> {
        ensure!(depth < 16, "IDL type nesting exceeded");
        if let Some(name) = ty.as_str() {
            return Ok(match name {
                "u8" => json!(r.take(1)?[0]),
                "bool" => {
                    let b = r.take(1)?[0];
                    ensure!(b <= 1, "invalid bool");
                    json!(b == 1)
                }
                "u16" => json!(u16::from_le_bytes(r.take(2)?.try_into()?)),
                "u32" => json!(u32::from_le_bytes(r.take(4)?.try_into()?)),
                "u64" => json!(u64::from_le_bytes(r.take(8)?.try_into()?)),
                "i64" => json!(i64::from_le_bytes(r.take(8)?.try_into()?)),
                "u128" => json!(u128::from_le_bytes(r.take(16)?.try_into()?).to_string()),
                "i128" => json!(i128::from_le_bytes(r.take(16)?.try_into()?).to_string()),
                "pubkey" => json!(Pubkey::new_from_array(r.take(32)?.try_into()?).to_string()),
                "string" => {
                    let n = u32::from_le_bytes(r.take(4)?.try_into()?) as usize;
                    ensure!(n <= 4096, "string too long");
                    json!(std::str::from_utf8(r.take(n)?)?)
                }
                _ => bail!("unsupported IDL scalar"),
            });
        }
        if let Some(inner) = ty.get("option") {
            let flag = r.take(1)?[0];
            ensure!(flag <= 1, "invalid option");
            return if flag == 0 {
                Ok(Value::Null)
            } else {
                self.decode_type(inner, r, depth + 1)
            };
        }
        if let Some(array) = ty.get("array").and_then(Value::as_array) {
            let n = array[1].as_u64().context("array size")?;
            ensure!(n <= 1024, "array too large");
            return Ok(Value::Array(
                (0..n)
                    .map(|_| self.decode_type(&array[0], r, depth + 1))
                    .collect::<Result<_>>()?,
            ));
        }
        if let Some(inner) = ty.get("vec") {
            let n = u32::from_le_bytes(r.take(4)?.try_into()?) as usize;
            ensure!(n <= 1024, "vector too large");
            return Ok(Value::Array(
                (0..n)
                    .map(|_| self.decode_type(inner, r, depth + 1))
                    .collect::<Result<_>>()?,
            ));
        }
        let name = ty["defined"]["name"]
            .as_str()
            .or_else(|| ty["defined"].as_str())
            .context("defined type name")?;
        let definition = self.value["types"]
            .as_array()
            .context("types")?
            .iter()
            .find(|t| t["name"] == name)
            .context("type definition")?;
        if definition["type"]["kind"] == "struct" {
            if let Some(fields) = definition["type"]["fields"].as_array() {
                let mut out = serde_json::Map::new();
                for field in fields {
                    out.insert(
                        field["name"].as_str().context("field name")?.into(),
                        self.decode_type(&field["type"], r, depth + 1)?,
                    );
                }
                return Ok(Value::Object(out));
            }
        }
        bail!("unsupported IDL type")
    }
}
pub fn discriminator(v: &Value) -> Result<[u8; 8]> {
    let bytes = v["discriminator"]
        .as_array()
        .context("discriminator")?
        .iter()
        .map(|v| v.as_u64().map(|v| v as u8).context("discriminator byte"))
        .collect::<Result<Vec<_>>>()?;
    bytes
        .try_into()
        .map_err(|_| anyhow::anyhow!("discriminator length"))
}
struct Reader<'a> {
    data: &'a [u8],
    offset: usize,
}
impl<'a> Reader<'a> {
    fn take(&mut self, n: usize) -> Result<&'a [u8]> {
        let end = self.offset.checked_add(n).context("overflow")?;
        ensure!(end <= self.data.len(), "truncated Borsh payload");
        let slice = &self.data[self.offset..end];
        self.offset = end;
        Ok(slice)
    }
}
pub fn create_ata(payer: Pubkey, owner: Pubkey, mint: Pubkey, token: Pubkey) -> Instruction {
    Instruction {
        program_id: Pubkey::from_str(ATA).expect("constant"),
        data: vec![1],
        accounts: vec![
            AccountMeta::new(payer, true),
            AccountMeta::new(ata(owner, mint, token), false),
            AccountMeta::new_readonly(owner, false),
            AccountMeta::new_readonly(mint, false),
            AccountMeta::new_readonly(solana_sdk::system_program::id(), false),
            AccountMeta::new_readonly(token, false),
        ],
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn official_discriminators() {
        for venue in [Venue::PumpFun, Venue::PumpSwap] {
            let idl = Idl::load(venue);
            assert_eq!(
                idl.identify(&[102, 6, 61, 18, 1, 218, 235, 234]),
                Some("buy")
            );
            assert!(idl.decode_event(&[0; 3]).is_err());
        }
    }
}
