use std::collections::HashMap;
use std::str::FromStr;
use std::sync::Arc;
use std::{convert::TryInto, u64};

use chrono::{
    offset::{TimeZone, Utc},
    SecondsFormat,
};

use googleapis_raw::spanner::v1::{
    result_set::ResultSet,
    spanner::{BeginTransactionRequest, CommitRequest, CreateSessionRequest, ExecuteSqlRequest},
    spanner_grpc::SpannerClient,
    transaction::{TransactionOptions, TransactionOptions_ReadWrite, TransactionSelector},
    type_pb::{Type, TypeCode},
};
use grpcio::{CallOption, ChannelBuilder, ChannelCredentials, EnvBuilder, MetadataBuilder};
use protobuf::well_known_types::{Struct, Value};

use crate::db::collections::{Collection, Collections};
use crate::db::{Bso, User};
use crate::error::{ApiErrorKind, ApiResult};
use crate::settings::Settings;

const MAX_MESSAGE_LEN: i32 = 104_857_600;

#[derive(Clone)]
pub struct Spanner {
    settings: Settings,
    pub client: SpannerClient,
    database_name: String,
}

fn get_path(raw: &str) -> ApiResult<String> {
    let url = match url::Url::parse(raw) {
        Ok(v) => v,
        Err(e) => return Err(ApiErrorKind::Internal(format!("Invalid Spanner DSN {}", e)).into()),
    };
    Ok(format!(
        "{}{}",
        url.host_str().unwrap_or("localhost"),
        url.path()
    ))
}

const SPANNER_ADDRESS: &str = "spanner.googleapis.com:443";

impl Spanner {
    pub fn new(settings: &Settings) -> ApiResult<Self> {
        if settings.dsns.spanner.is_none() || settings.dsns.mysql.is_none() {
            return Err(ApiErrorKind::Internal("No DSNs set".to_owned()).into());
        }
        let creds = ChannelCredentials::google_default_credentials()?;
        let env = Arc::new(EnvBuilder::new().build());
        let spanner_path = &settings.dsns.spanner.clone().expect("No spanner DSN found");
        debug!("Opening spanner: {:?}", &spanner_path);
        let database_name = get_path(&spanner_path)?;
        let chan = ChannelBuilder::new(env)
            .max_send_message_len(MAX_MESSAGE_LEN)
            .max_receive_message_len(MAX_MESSAGE_LEN)
            .secure_connect(SPANNER_ADDRESS, creds);

        let client = SpannerClient::new(chan);

        Ok(Self {
            settings: settings.clone(),
            client,
            database_name,
        })
    }

    pub async fn transaction(
        &self,
        sql: &str,
        params: Option<(HashMap<String, Value>, HashMap<String, Type>)>,
    ) -> ApiResult<ResultSet> {
        // generate the session
        let session = {
            let mut req = CreateSessionRequest::new();
            let mut meta = MetadataBuilder::new();
            meta.add_str("google-cloud-resource-prefix", &self.database_name)?;
            meta.add_str("x-goog-api-client", "gcp-grpc-rs")?;
            req.database = self.database_name.clone();
            let opt = CallOption::default().headers(meta.build());
            self.client.create_session_opt(&req, opt)?
        };
        let session_name = session.name;

        // generate the transaction
        let mut opts = TransactionOptions::new();
        let mut treq = BeginTransactionRequest::new();
        let mut txns = TransactionSelector::new();
        opts.set_read_write(TransactionOptions_ReadWrite::new());
        treq.set_session(session_name.clone());
        treq.set_options(opts);
        let mut txn = self.client.begin_transaction(&treq)?;
        let txn_id = txn.take_id();
        txns.set_id(txn_id.clone());

        // build and execute the SQL
        let mut sreq = ExecuteSqlRequest::new();
        sreq.set_session(session_name.clone());
        sreq.set_transaction(txns);
        sreq.set_sql(sql.to_owned());
        if let Some((params, types)) = params {
            let mut sparams = Struct::new();
            sparams.set_fields(params);
            sreq.set_params(sparams);
            sreq.set_param_types(types);
        }
        match self.client.execute_sql(&sreq) {
            Ok(v) => {
                // commit
                let mut creq = CommitRequest::new();
                creq.set_session(session_name);
                creq.set_transaction_id(txn_id.to_vec());
                self.client.commit(&creq)?;
                Ok(v)
            }
            Err(e) => {
                Err(ApiErrorKind::Internal(format!("spanner transaction failed: {:?}", e)).into())
            }
        }
    }

    pub async fn get_collections(&self) -> ApiResult<Collections> {
        // get the default base of collections (in case the original is missing them)
        let mut collections = Collections::default();

        let result = self
            .transaction(
                "SELECT
                DISTINCT uc.collection_id, cc.name,
            FROM
                user_collections as uc,
                collections as cc
            WHERE
                uc.collection_id = cc.collection_id
            ORDER BY
                uc.collection_id",
                None,
            )
            .await?;
        // back fill with the values from the collection db table, which is our source
        // of truth.

        for row in result.get_rows() {
            let id: u16 = u16::from_str(row.values[0].get_string_value())?;
            let name: &str = row.values[1].get_string_value();
            if collections.get(name).is_none() {
                collections.set(
                    name,
                    Collection {
                        name: name.to_owned(),
                        collection: id,
                        last_modified: 0,
                    },
                );
            }
        }
        Ok(collections)
    }

    pub fn as_value(&self, value: &str) -> Value {
        let mut val = Value::new();
        val.set_string_value(value.to_owned());
        val
    }

    pub fn as_type(&self, v: TypeCode) -> Type {
        let mut t = Type::new();
        t.set_code(v);
        t
    }

    pub async fn add_new_collections(&self, new_collections: Collections) -> ApiResult<()> {
        let items = new_collections.items();
        if !items.is_empty() {
            let mut sql_params: HashMap<String, Value> = HashMap::new();
            let mut param_type: HashMap<String, Type> = HashMap::new();
            let mut values: Vec<String> = Vec::new();
            let header = "INSERT INTO collections (collection_id, name)";
            for (count, item) in items.into_iter().enumerate() {
                let l_col_id = format!("collection_id_{}", count);
                let l_name = format!("name_{}", count);
                values.push(format!("(@{}, @{})", &l_col_id, &l_name));
                sql_params.insert(
                    l_col_id.clone(),
                    self.as_value(&item.collection.to_string()),
                );
                param_type.insert(l_col_id, self.as_type(TypeCode::INT64));
                sql_params.insert(l_name, self.as_value(&item.name));
            }
            debug!("Adding new collections");
            let sql = format!("{} VALUES {}", header, values.join(","));
            if !self.settings.dryrun {
                self.transaction(&sql, Some((sql_params, param_type)))
                    .await?;
            }
        }
        debug!("    Finished Reconciliation...");
        Ok(())
    }

    pub fn as_rfc3339(&self, val: u64) -> ApiResult<String> {
        let secs = val / 1000;
        let nsecs = ((val % 1000) * 1_000_000).try_into().map_err(|e| {
            ApiErrorKind::Internal(format!("Invalid timestamp (nanoseconds) {}: {}", val, e))
        })?;
        Ok(Utc
            .timestamp(secs as i64, nsecs)
            .to_rfc3339_opts(SecondsFormat::Nanos, true))
    }

    pub async fn load_user_collections(
        &mut self,
        user: &User,
        collections: Vec<Collection>,
    ) -> ApiResult<()> {
        if !collections.is_empty() {
            debug!("    Loading user collections...");
            let mut sql_params: HashMap<String, Value> = HashMap::new();
            let mut param_type: HashMap<String, Type> = HashMap::new();
            let mut values: Vec<String> = Vec::new();
            let header = "INSERT INTO user_collections (fxa_kid, fxa_uid, collection_id, modified)";
            sql_params.insert("fxa_kid".to_owned(), self.as_value(&user.fxa_data.fxa_kid));
            sql_params.insert("fxa_uid".to_owned(), self.as_value(&user.fxa_data.fxa_uid));
            for (count, item) in collections.into_iter().enumerate() {
                let l_col_id = format!("collection_id_{}", count);
                let l_modified = format!("modified_{}", count);
                values.push(format!(
                    "(@fxa_kid, @fxa_uid, @{}, @{})",
                    &l_col_id, &l_modified
                ));
                sql_params.insert(
                    l_col_id.clone(),
                    self.as_value(&item.collection.to_string()),
                );
                param_type.insert(l_col_id, self.as_type(TypeCode::INT64));
                sql_params.insert(
                    l_modified.clone(),
                    self.as_value(&self.as_rfc3339(item.last_modified)?),
                );
                param_type.insert(l_modified, self.as_type(TypeCode::TIMESTAMP));
            }
            let sql = format!("{} VALUES {}", header, values.join(","));
            if !self.settings.dryrun {
                self.transaction(&sql, Some((sql_params, param_type)))
                    .await?;
            }
        }

        Ok(())
    }

    pub async fn add_user_bsos(
        &mut self,
        user: &User,
        bsos: &[Bso],
        collections: &Collections,
    ) -> ApiResult<usize> {
        debug!("    Loading bso...");
        let mut sql_params: HashMap<String, Value> = HashMap::new();
        let mut param_type: HashMap<String, Type> = HashMap::new();
        let mut values: Vec<String> = Vec::new();
        let header = "INSERT INTO bsos (fxa_kid, fxa_uid,  bso_id, collection_id, expiry, modified, payload, sortindex)";
        sql_params.insert("fxa_kid".to_owned(), self.as_value(&user.fxa_data.fxa_kid));
        sql_params.insert("fxa_uid".to_owned(), self.as_value(&user.fxa_data.fxa_uid));
        for (count, item) in bsos.iter().enumerate() {
            let l_bso_id = format!("bso_{}", count);
            let l_col_id = format!("collection_id_{}", count);
            let l_expiry = format!("expiry_{}", count);
            let l_modified = format!("modified_{}", count);
            let l_payload = format!("payload_{}", count);
            let l_sortindex = format!("sortindex_{}", count);
            values.push(format!(
                "(@fxa_kid, @fxa_uid, @{}, @{}, @{}, @{}, @{}, @{})",
                &l_bso_id, &l_col_id, &l_expiry, &l_modified, &l_payload, &l_sortindex
            ));
            let adj_col = collections
                .get(&item.col_name)
                .unwrap_or(&Collection {
                    name: item.col_name.clone(),
                    collection: item.col_id,
                    last_modified: 0,
                })
                .collection;
            sql_params.insert(l_bso_id, self.as_value(&item.bso_id));
            sql_params.insert(l_col_id.clone(), self.as_value(&adj_col.to_string()));
            param_type.insert(l_col_id, self.as_type(TypeCode::INT64));
            sql_params.insert(
                l_expiry.clone(),
                self.as_value(&self.as_rfc3339(item.expiry * 1000)?),
            );
            param_type.insert(l_expiry, self.as_type(TypeCode::TIMESTAMP));
            sql_params.insert(
                l_modified.clone(),
                self.as_value(&self.as_rfc3339(item.modify)?),
            );
            param_type.insert(l_modified, self.as_type(TypeCode::TIMESTAMP));
            sql_params.insert(l_payload, self.as_value(&item.payload));
            sql_params.insert(
                l_sortindex.clone(),
                self.as_value(&item.sort_index.unwrap_or(0).to_string()),
            );
            param_type.insert(l_sortindex, self.as_type(TypeCode::INT64));
        }
        let sql = format!("{} VALUES {}", header, values.join(","));
        if !self.settings.dryrun{
            self.transaction(&sql, Some((sql_params, param_type)))
                .await?;
        }
        Ok(bsos.len())
    }
}
