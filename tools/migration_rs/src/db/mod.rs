pub mod collections;
pub mod mysql;
pub mod spanner;

use crate::db::collections::Collections;
use crate::error::ApiResult;
use crate::fxa::{FxaData, FxaInfo};
use crate::settings::Settings;

pub struct Dbs {
    settings: Settings,
    mysql: mysql::MysqlDb,
    spanner: spanner::Spanner,
}

#[derive(Debug)]
pub struct Bso {
    col_name: String,
    col_id: u16,
    bso_id: String,
    expiry: u64,
    modify: u64,
    payload: String,
    sort_index: Option<u64>,
}

#[derive(Debug)]
pub struct User {
    uid: u64,
    fxa_data: FxaData,
}

impl Dbs {
    pub fn connect(settings: &Settings) -> ApiResult<Dbs> {
        Ok(Self {
            settings: settings.clone(),
            mysql: mysql::MysqlDb::new(&settings)?,
            spanner: spanner::Spanner::new(&settings)?,
        })
    }

    pub async fn get_users(&self, bso_num: u8, fxa: &FxaInfo) -> ApiResult<Vec<User>> {
        let mut result: Vec<User> = Vec::new();
        let users = self.mysql.get_user_ids(bso_num).await?;
        for uid in users {
            if let Some(fxa_data) = fxa.get_fxa_data(uid) {
                let user = User { uid, fxa_data };
                debug!("user: {:?}", user);
                result.push(user)
            }
        }
        Ok(result)
    }

    pub async fn move_user(
        &mut self,
        user: &User,
        bso_num: u8,
        collections: &Collections,
    ) -> ApiResult<()> {
        debug!("Copying user collections...");
        let user_collections = self.mysql.get_user_collections(user, bso_num).await?;
        self.spanner
            .load_user_collections(user, user_collections)
            .await?;
        // let spanner finish writing the user_collections;
        // std::thread::sleep(std::time::Duration::from_micros(100));
        debug!("Copying user BSOs...");
        // fetch and handle the user BSOs
        let bsos = self.mysql.get_user_bsos(user, bso_num).await?;
        // divvy up according to the readchunk
        let blocks = bsos.chunks(self.settings.readchunk.unwrap_or(1000) as usize);
        for block in blocks {
            // debug!("Block: {:?}", &block);
            // TODO add abort stuff
            match self.spanner.add_user_bsos(user, block, &collections).await {
                Ok(_) => print!("."),
                Err(e) => panic!("Unknown Error: {}", e),
            };
        }
        Ok(())
    }
}
