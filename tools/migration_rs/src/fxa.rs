use std::collections::HashMap;
use std::fs::File;
use std::io::{BufRead, BufReader};

use base64;
use regex;
use serde::{self, Deserialize};
use uuid::Uuid;

use crate::error::{ApiErrorKind, ApiResult};
use crate::settings::Settings;

#[derive(Debug, Deserialize)]
pub struct FxaCSVRecord {
    pub uid: u64,
    pub email: String,
    pub generation: Option<u64>,
    pub keys_changed_at: Option<u64>,
    pub client_state: String,
}

#[derive(Debug, Clone, serde::Deserialize)]
pub struct FxaData {
    pub fxa_uid: String,
    pub fxa_kid: String,
}

impl Default for FxaData {
    fn default() -> FxaData {
        Self {
            fxa_uid: "".to_owned(),
            fxa_kid: "".to_owned(),
        }
    }
}

#[derive(Debug, Clone, serde::Deserialize)]
pub struct FxaInfo {
    pub users: HashMap<u64, FxaData>,
    pub anon: bool,
}

impl FxaInfo {
    fn gen_uid(record: &FxaCSVRecord) -> ApiResult<String> {
        let parts: Vec<&str> = record.email.splitn(2, '@').collect();
        Ok(parts[0].to_owned())
    }

    fn gen_kid(record: &FxaCSVRecord) -> ApiResult<String> {
        let key_index = record
            .keys_changed_at
            .unwrap_or_else(|| record.generation.unwrap_or(0));
        let key_hash: Vec<u8> = match hex::decode(record.client_state.clone()) {
            Ok(v) => v,
            Err(e) => {
                return Err(ApiErrorKind::Internal(format!("Invalid client state {}", e)).into())
            }
        };
        Ok(format!(
            "{:013}-{}",
            key_index,
            base64::encode_config(&key_hash, base64::URL_SAFE_NO_PAD)
        ))
    }

    pub fn get_fxa_data(&self, uid: u64) -> Option<FxaData> {
        if self.anon {
            return Some(FxaData {
                fxa_uid: Uuid::new_v4().to_simple().to_owned().to_string(),
                fxa_kid: Uuid::new_v4().to_simple().to_owned().to_string(),
            });
        }
        self.users.get(&uid).cloned()
    }

    pub fn new(settings: &Settings) -> ApiResult<Self> {
        if settings.anon {
            debug!("***Anonymized");
            return Ok(Self {
                users: HashMap::new(),
                anon: true,
            });
        }
        // I'd prefer to use csv::Reader::from_reader(...) here, but
        // it's not been super reliable. Thus the hand-roll you see below.
        debug!("Reading {}", &settings.fxa_file);
        let re = regex::Regex::new(r"(\s+)")?;
        let buffer = BufReader::new(File::open(&settings.fxa_file)?);
        let mut users = HashMap::<u64, FxaData>::new();
        for line in buffer.lines().map(|l| l.unwrap()) {
            debug!("Line: {:?}", &line);
            let fixed = re.replace_all(line.trim(), "\t");
            if fixed.starts_with("#") {
                continue;
            }
            let s_record: Vec<&str> = line.split('\t').collect();
            if s_record[0] == "uid" {
                continue;
            }
            debug!("{:?}", &s_record);
            let record = FxaCSVRecord {
                uid: u64::from_str_radix(s_record[0], 10)?,
                email: s_record[1].to_owned(),
                generation: match s_record[2] {
                    "NULL" | "" => None,
                    _ => Some(u64::from_str_radix(s_record[2], 10)?),
                },
                keys_changed_at: match s_record[3] {
                    "NULL" | "" => None,
                    _ => Some(u64::from_str_radix(s_record[3], 10)?),
                },
                client_state: match s_record[4] {
                    "NULL" => "",
                    _ => s_record[4]
                }.to_owned(),
            };
            users.insert(
                record.uid,
                FxaData {
                    fxa_uid: FxaInfo::gen_uid(&record)?,
                    fxa_kid: FxaInfo::gen_kid(&record)?,
                },
            );
        }
        if users.is_empty() {
            return Err(ApiErrorKind::FxAError("No Users found".into()).into());
        }
        debug!("FXA_Data: {:?}", users);
        Ok(Self { users, anon: false })
    }
}
