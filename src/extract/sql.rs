use clap::Args;
use std::cell::RefCell;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};

use super::ExtractState;

#[derive(Debug, thiserror::Error)]
#[error("Cancelled extract")]
struct CancelledError;

#[derive(Debug, Args)]
pub struct ExtractSqlCommand {
    /// The output database
    #[clap(long = "out", required = true, parse(from_os_str))]
    output: PathBuf,
    /// The target files to extract
    #[clap(required = true, parse(from_os_str))]
    targets: Vec<PathBuf>,
}
struct SqlExtractListener {
    skipped: AtomicU64,
    connection: RefCell<rusqlite::Connection>,
}

impl super::ExtractListener for SqlExtractListener {
    fn on_parse(&self, event: super::ParseEvent) -> Result<(), anyhow::Error> {
        let mut conn = self.connection.borrow_mut();
        let raw_html = event.article.body.html.as_bytes();
        let compressed = zstd::encode_all(raw_html, /* level */ 1)?;
        let tx = conn.transaction()?;
        match tx.execute(
            "INSERT INTO article(name, url) VALUES (?1, ?2);",
            rusqlite::params![&event.article.name, &event.article.url],
        ) {
            Ok(_) => {}
            Err(rusqlite::Error::SqliteFailure(cause, _))
                if cause.code == rusqlite::ffi::ErrorCode::ConstraintViolation =>
            {
                let s = self.skipped.fetch_add(1, Ordering::SeqCst);
                if s % 500 == 0 {
                    eprintln!("Skipped {} files", s);
                }
                // Article already exists, just ignore
                return Ok(());
            }
            Err(cause) => return Err(cause.into()),
        }
        tx.execute(
            "INSERT INTO article_body(name, compressed_html) VALUES(?1, ?2)",
            rusqlite::params![event.article.name, &compressed],
        )?;
        tx.commit()?;
        event.basic_report_progress(/* verbose */ false);
        Ok(())
    }

    fn on_parse_error(
        &self,
        _original_file: &std::path::Path,
        cause: anyhow::Error,
    ) -> Result<(), anyhow::Error> {
        eprintln!("ERROR: Unable to parse file: {}", cause);
        Ok(())
    }
}
pub fn extract(command: ExtractSqlCommand) -> anyhow::Result<()> {
    let target = command.output.clone();
    if !target.is_file() {
        let connection = rusqlite::Connection::open_with_flags(
            target.clone(),
            rusqlite::OpenFlags::SQLITE_OPEN_READ_WRITE | rusqlite::OpenFlags::SQLITE_OPEN_CREATE,
        )?;
        connection.execute_batch(
            "
            CREATE TABLE article_body(
                name VARCHAR(255) PRIMARY KEY NOT NULL,
                compressed_html BLOB
            );
            CREATE TABLE article(
                name VARCHAR(255) PRIMARY KEY NOT NULL,
                url VARCHAR(255) NOT NULL
            );
        ",
        )?;
        connection.close().map_err(|(_, err)| err)?;
    }
    let connection = rusqlite::Connection::open_with_flags(
        &target,
        rusqlite::OpenFlags::SQLITE_OPEN_READ_WRITE,
    )?;
    connection.execute_batch(
        "
        PRAGMA journal_mode = WAL;
    ",
    )?;
    let listener = SqlExtractListener {
        connection: RefCell::new(connection),
        skipped: AtomicU64::new(0),
    };
    let state = ExtractState::new(Box::new(listener));
    for target in &command.targets {
        eprintln!("Processing {}", target.display());
        state.run_extract(target.clone())?;
    }
    eprintln!("Extracted {} files", state.count());
    let listener = *state.listener;
    let connection = RefCell::into_inner(listener.connection);
    connection.close().map_err(|(_, e)| e)?;
    Ok(())
}
