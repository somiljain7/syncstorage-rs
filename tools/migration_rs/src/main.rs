#[macro_use]
extern crate slog_scope;

use std::ops::Range;

use structopt::StructOpt;

mod db;
mod error;
mod fxa;
mod logging;
mod settings;
mod report;

#[tokio::main]
async fn main() -> Result<(), error::ApiError> {
    let settings = settings::Settings::from_args();
    let mut user_count: usize = 0;
    let mut bso_count: usize = 0;

    let mut report = report::Report::new(&settings);
    // TODO: set logging level
    match logging::init_logging(settings.human_logs) {
        Ok(_) => {}
        Err(e) => panic!("Logging init failure {:?}", e),
    }
    // create the database connections
    let mut dbs = match db::Dbs::connect(&settings) {
        Ok(v) => v,
        Err(e) => panic!("DB configuration error: {:?}", e),
    };
    // TODO:read in fxa_info file (todo: make db?)
    debug!("Getting FxA user info...");
    let fxa = fxa::FxaInfo::new(&settings)?;
    // reconcile collections
    debug!("Fetching collections...");
    let collections = db::collections::Collections::new(&settings, &dbs).await?;
    // let users = dbs.get_users(&settings, &fxa)?.await;
    let mut start_bso = &settings.start_bso;
    let mut end_bso = &settings.end_bso;
    let suser = &settings.user.clone();
    if let Some(user) = suser {
        start_bso = &user.bso;
        end_bso = &user.bso;
    }

    let range = Range {
        start: *start_bso,
        end: *end_bso,
    };
    debug!("Checking range {:?}", &range);
    for bso_num in range {
        report.set_bso(bso_num);
        debug!("BSO: {}", bso_num);
        let users = dbs.get_users(bso_num, &fxa, &settings, &mut report).await?;
        debug!("Users: {:?}", &users);
        user_count = users.len();
        // divvy up users;
        for user in users {
            dbg!(&user);
            bso_count += dbs.move_user(&user, bso_num, &collections, &mut report).await?;
        }
    }
    info!("Moved {:?} users: {:?} rows", user_count, bso_count);
    Ok(())
}
