use clap::Args;
use std::cell::RefCell;
use std::sync::Weak as ArcWeak;
use std::{
    path::PathBuf,
    sync::atomic::{AtomicU64, Ordering},
};
use thread_local::ThreadLocal;

use crate::extract::ExtractError;

#[derive(Debug, thiserror::Error)]
#[error("Cancelled extract")]
struct CancelledError;

#[derive(Debug, Args)]
pub struct ExtractSqlCommand {
    /// Skip existing articles
    #[clap(long)]
    skip_existing: bool,
    /// The output database
    #[clap(long = "out", required = true, parse(from_os_str))]
    output: PathBuf,
    /// The target files to extract
    #[clap(required = true, parse(from_os_str))]
    targets: Vec<PathBuf>,
}
struct SqlExtractListener {
    command: ExtractSqlCommand,
    target_db: PathBuf,
    connection: ThreadLocal<RefCell<rusqlite::Connection>>,
}

impl super::ExtractListener for SqlExtractListener {
    fn on_parse(&self, event: super::ParseEvent) -> Result<(), anyhow::Error> {
        let conn = self.connection.get_or_try::<_, anyhow::Error>(|| {
            let conn = rusqlite::Connection::open_with_flags(
                self.target_db.clone(),
                rusqlite::OpenFlags::SQLITE_OPEN_READ_WRITE,
            )?;
            conn.execute("PRAGMA foreign_keys = ON;", [])?;
            Ok(RefCell::new(conn))
        })?;
        let mut conn = conn.borrow_mut();
        let raw_html = event.article.body.html.as_bytes();
        let compressed = zstd::encode_all(raw_html, /* level */ 1)?;
        let tx = conn.transaction()?;
        match tx.execute(
            "INSERT INTO article(name, url, compressed_html) VALUES (?1, ?2, ?3);",
            rusqlite::params![&event.article.name, &event.article.url, &compressed],
        ) {
            Ok(_) => {}
            Err(rusqlite::Error::SqliteFailure(cause, _))
                if cause.code == rusqlite::ffi::ErrorCode::ConstraintViolation =>
            {
                // Article already exists, just ignore
                return Ok(());
            }
            Err(cause) => return Err(cause.into()),
        }
        tx.commit()?;
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
        connection.execute(
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
            [],
        )?;
        connection.close().map_err(|(_, err)| err)?;
    }
    let paths = command.targets.clone();
    let listener = SqlExtractListener {
        command,
        connection: ThreadLocal::new(),
        target_db: target,
    };
    let mut task = super::extract_threaded(paths, Box::new(listener))?;
    match task.wait() {
        Ok(()) => {}
        Err(ExtractError::Listener(ref e)) if e.is::<CancelledError>() => {}
        Err(cause) => return Err(cause.into()),
    }
    assert!(task.is_finished());
    eprintln!("Extracted {} files", task.count());
    Ok(())
}

fn parse_url(url: &str) -> Result<String, String> {
    const PREFIX: &str = "/wiki/";
    match url.find(PREFIX) {
        None => Err(format!("No `/wiki/` in {:?}", url)),
        Some(idx) => Ok(format!("{}.html", &url[idx + PREFIX.len()..])),
    }
}

pub fn sanitize_name(name: &str) -> String {
    name.replace('/', "__")
        .replace(':', "__colon__")
        .replace('*', "__star__")
}
