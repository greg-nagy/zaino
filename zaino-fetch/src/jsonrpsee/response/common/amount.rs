// src/jsonrpsee/response/common/amount.rs
use serde::{Deserialize, Deserializer, Serialize};

/// Amount in zatoshis (integer). Accepts wire inputs as:
/// - JSON number (u64/i64 >= 0) → zats
/// - JSON float (>= 0) → ZEC, rounded to 8 dp then to zats
/// - String decimal ("2.5", "0.00000001") → ZEC → zats
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize)]
#[serde(transparent)]
pub struct Zatoshis(pub u64);

#[derive(Deserialize)]
#[serde(untagged)]
enum NumLike {
    Str(String),
    F64(f64),
    U64(u64),
    I64(i64),
}

impl<'de> Deserialize<'de> for Zatoshis {
    fn deserialize<D: Deserializer<'de>>(de: D) -> Result<Self, D::Error> {
        const ZAT_PER_ZEC: f64 = 100_000_000.0;

        match NumLike::deserialize(de)? {
            NumLike::U64(u) => Ok(Zatoshis(u)),
            NumLike::I64(i) if i >= 0 => Ok(Zatoshis(i as u64)),
            NumLike::I64(_) => Err(serde::de::Error::custom("negative amount")),
            NumLike::F64(f) => {
                if !f.is_finite() || f < 0.0 {
                    return Err(serde::de::Error::custom("invalid amount"));
                }
                Ok(Zatoshis((f * ZAT_PER_ZEC).round() as u64))
            }
            NumLike::Str(s) => {
                // TODO: inline decimal parsing with up to 8 fractional digits
                let s = s.trim();
                if s.starts_with('-') {
                    return Err(serde::de::Error::custom("negative amount"));
                }
                let (int, frac) = match s.split_once('.') {
                    Some((i, f)) => (i, f),
                    None => (s, ""),
                };
                if frac.len() > 8 {
                    return Err(serde::de::Error::custom("too many fractional digits"));
                }
                let int_part: u64 = if int.is_empty() {
                    0
                } else {
                    int.parse().map_err(serde::de::Error::custom)?
                };
                // TODO: pad right to 8 fractional digits
                let mut buf = frac.as_bytes().to_vec();
                while buf.len() < 8 {
                    buf.push(b'0');
                }
                let frac_part: u64 = if buf.is_empty() {
                    0
                } else {
                    std::str::from_utf8(&buf)
                        .unwrap()
                        .parse()
                        .map_err(serde::de::Error::custom)?
                };
                let base = int_part
                    .checked_mul(100_000_000)
                    .ok_or_else(|| serde::de::Error::custom("overflow"))?;
                let total = base
                    .checked_add(frac_part)
                    .ok_or_else(|| serde::de::Error::custom("overflow"))?;
                Ok(Zatoshis(total))
            }
        }
    }
}

impl From<Zatoshis> for u64 {
    fn from(z: Zatoshis) -> u64 {
        z.0
    }
}
