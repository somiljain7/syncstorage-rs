//! Application settings objects and initialization
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::str::FromStr;

use structopt::StructOpt;
use url::Url;

use crate::error::{ApiError, ApiErrorKind, ApiResult};

static DEFAULT_CHUNK_SIZE: u64 = 1_500_000;
static DEFAULT_READ_CHUNK: u64 = 1_000;
static DEFAULT_OFFSET: u64 = 0;
static DEFAULT_START_BSO: u8 = 0;
static DEFAULT_END_BSO: u8 = 19;
static DEFAULT_FXA_FILE: &str = "users.csv";
static DEFAULT_SPANNER_POOL_SIZE: usize = 32;

#[derive(Clone, Debug)]
pub struct Dsns {
    pub mysql: Option<String>,
    pub spanner: Option<String>,
}

impl Default for Dsns {
    fn default() -> Self {
        Dsns {
            mysql: None,
            spanner: None,
        }
    }
}
impl Dsns {
    fn from_str(raw: &str) -> Result<Self, ApiError> {
        let mut result = Self::default();
        let buffer = BufReader::new(File::open(raw)?);
        for line in buffer.lines().map(|l| l.unwrap()) {
            let url = Url::parse(&line).expect("Invalid DSN url");
            match url.scheme() {
                "mysql" => result.mysql = Some(line),
                "spanner" => result.spanner = Some(line),
                _ => {}
            }
        }
        Ok(result)
    }
}

#[derive(Clone, Debug)]
pub struct Users {
    pub bso: u8,
    pub user_ids: Vec<String>,
}

impl Users {
    fn from_str(raw: &str) -> Result<Users, ApiError> {
        let parts: Vec<&str> = raw.splitn(2, ':').collect();
        if parts.len() == 1 {
            return Err(ApiErrorKind::Internal("bad user option".to_owned()).into());
        }
        let bso = match u8::from_str(parts[0]) {
            Ok(v) => v,
            Err(e) => return Err(ApiErrorKind::Internal(format!("invalid bso: {}", e)).into()),
        };
        let s_ids = parts[1].split(',').collect::<Vec<&str>>();
        let mut user_ids: Vec<String> = Vec::new();
        for id in s_ids {
            user_ids.push(id.to_owned());
        }

        Ok(Users { bso, user_ids })
    }
}

#[derive(Clone, Debug)]
pub struct Abort {
    pub bso: u8,
    pub count: u64,
}

impl Abort {
    fn from_str(raw: &str) -> Result<Self, ApiError> {
        let parts: Vec<&str> = raw.splitn(2, ':').collect();
        if parts.len() == 1 {
            return Err(ApiErrorKind::Internal("Bad abort option".to_owned()).into());
        }
        let bso = match u8::from_str(parts[0]) {
            Ok(v) => v,
            Err(e) => return Err(ApiErrorKind::Internal(format!("invalid bso: {}", e)).into()),
        };
        Ok(Abort {
            bso,
            count: u64::from_str(parts[1]).expect("Bad count for Abort"),
        })
    }
}

#[derive(Clone, Debug)]
pub struct UserRange {
    pub offset: u64,
    pub limit: u64,
}

impl UserRange {
    fn from_str(raw: &str) -> Result<Self, ApiError> {
        let parts: Vec<&str> = raw.splitn(2, ':').collect();
        if parts.len() == 1 {
            return Err(ApiErrorKind::Internal("Bad user range option".to_owned()).into());
        }
        Ok(UserRange {
            offset: u64::from_str(parts[0]).expect("Bad offset"),
            limit: u64::from_str(parts[1]).expect("Bad limit"),
        })
    }
}

#[derive(Clone, Debug)]
pub struct UserPercent {
    pub chunk: u64,
    pub percentage: u64,
}

impl UserPercent {
    fn from_str(raw: &str) -> Result<Self, ApiError> {
        let parts: Vec<&str> = raw.splitn(2, ':').collect();
        if parts.len() == 1 {
            return Err(ApiErrorKind::Internal("Bad user percentage option".to_owned()).into());
        }
        Ok(UserPercent {
            chunk: u64::from_str(parts[0]).expect("Bad offset"),
            percentage: u64::from_str(parts[1]).expect("Bad limit"),
        })
    }

    pub fn get_percentage(&self, users: Vec<u64>) -> ApiResult<Vec<u64>> {
        // extract the requested percentage of users from the total list.
        let total_count = users.len() as u64;
        let mut chunk_size =
            f64::floor(total_count as f64 * (self.percentage as f64 * 0.01)).round() as u64;
        if chunk_size < 1 {
            chunk_size = 1;
        }
        let chunk_count = f64::ceil(total_count as f64 / chunk_size as f64).round() as u64;
        let start = std::cmp::max(0, self.chunk - 1);
        let chunk_start = start * chunk_size;
        let mut chunk_end = std::cmp::min(total_count, chunk_start + chunk_count);
        if chunk_size * chunk_count > total_count {
            if self.chunk > chunk_count - 1 {
                chunk_end = total_count;
            }
        }
        Ok(users[chunk_start as usize..chunk_end as usize].to_vec())
    }
}

#[derive(StructOpt, Clone, Debug)]
#[structopt(name = "env")]
pub struct Settings {
    #[structopt(long, parse(try_from_str=Dsns::from_str), env = "MIGRATE_DSNS")]
    pub dsns: Dsns,
    #[structopt(long, env = "MIGRATE_DEBUG")]
    pub debug: bool,
    #[structopt(short, env = "MIGRATE_VERBOSE")]
    pub verbose: bool,
    #[structopt(long)]
    pub quiet: bool,
    #[structopt(long)]
    pub full: bool,
    #[structopt(long)]
    pub anon: bool,
    #[structopt(long)]
    pub skip_collections: bool,
    #[structopt(long)]
    pub dryrun: bool,
    #[structopt(long, parse(from_flag = std::ops::Not::not), default_value="true")]
    pub human_logs: bool,
    #[structopt(long, default_value = "users.csv")]
    pub fxa_file: String,
    #[structopt(long)]
    pub chunk_limit: Option<u64>,
    #[structopt(long)]
    pub offset: Option<u64>,
    #[structopt(long, default_value = "0")]
    pub start_bso: u8,
    #[structopt(long, default_value = "19")]
    pub end_bso: u8,
    #[structopt(long, default_value = "1666")]
    pub chunk: u64,
    #[structopt(long)]
    pub spanner_pool_size: Option<usize>,
    #[structopt(long, parse(try_from_str=Users::from_str))]
    pub user: Option<Users>,
    #[structopt(long, parse(try_from_str=Abort::from_str))]
    pub abort: Option<Abort>,
    #[structopt(long, parse(try_from_str=UserRange::from_str))]
    pub user_range: Option<UserRange>,
    #[structopt(long, parse(try_from_str=UserPercent::from_str))]
    pub user_percent: Option<UserPercent>,
}

impl Default for Settings {
    fn default() -> Settings {
        Settings {
            dsns: Dsns::default(),
            debug: false,
            verbose: false,
            quiet: false,
            full: false,
            anon: false,
            skip_collections: false,
            dryrun: false,
            human_logs: true,
            chunk_limit: Some(DEFAULT_CHUNK_SIZE),
            offset: Some(DEFAULT_OFFSET),
            start_bso: DEFAULT_START_BSO,
            end_bso: DEFAULT_END_BSO,
            chunk: DEFAULT_READ_CHUNK,
            spanner_pool_size: Some(DEFAULT_SPANNER_POOL_SIZE),
            fxa_file: DEFAULT_FXA_FILE.to_owned(),
            user: None,
            abort: None,
            user_range: None,
            user_percent: None,
        }
    }
}
