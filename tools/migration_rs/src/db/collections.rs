use std::collections::HashMap;

use crate::db::Dbs;
use crate::error::ApiResult;
use crate::settings::Settings;

#[derive(Clone, Debug)]
pub struct Collection {
    pub name: String,
    pub collection: u16,
    pub last_modified: u64,
}

#[derive(Clone, Debug)]
pub struct Collections {
    by_name: HashMap<String, Collection>,
}

impl Default for Collections {
    fn default() -> Self {
        let mut names: HashMap<String, Collection> = HashMap::new();
        for (name, idx) in &[
            ("clients", 1),
            ("crypto", 2),
            ("forms", 3),
            ("history", 4),
            ("keys", 5),
            ("meta", 6),
            ("bookmarks", 7),
            ("prefs", 8),
            ("tabs", 9),
            ("passwords", 10),
            ("addons", 11),
            ("addresses", 12),
            ("creditcards", 13),
            ("reserved", 100),
        ] {
            names.insert(
                (*name).to_string(),
                Collection {
                    name: (*name).to_string(),
                    collection: *idx,
                    last_modified: 0,
                },
            );
        }
        Self { by_name: names }
    }
}

impl Collections {
    pub fn empty() -> Collections {
        // used by db::collections to contain "new", unencountered user collections
        // this differs from the "default" set of well-known collections.
        Collections {
            by_name: HashMap::new(),
        }
    }

    pub async fn new(_settings: &Settings, dbs: &Dbs) -> ApiResult<Collections> {
        let mysql = &dbs.mysql;
        let span = dbs.spanner.clone();
        debug!("    Fetching spanner collections...");
        let mut collections = span.get_collections().await.unwrap();
        debug!("    Fetching mysql collections...{:?}", collections);
        let new_collections = mysql.merge_collections(&mut collections).await.unwrap();
        debug!("    Reconciling collections...");
        span.add_new_collections(new_collections).await.unwrap();
        debug!("    Collections collected...");
        Ok(collections)
    }

    pub fn get(&self, key: &str) -> Option<&Collection> {
        self.by_name.get(key)
    }

    pub fn set(&mut self, key: &str, col: Collection) {
        self.by_name.insert(key.to_owned(), col);
    }

    pub fn items(self) -> Vec<Collection> {
        self.by_name
            .values()
            .map(Clone::clone)
            .collect::<Vec<Collection>>()
    }
}
