use anyhow::anyhow;
use anyhow::Result;
use clap::Args;
use crossbeam::channel::{Receiver, Sender};
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::thread::JoinHandle;

use super::ExtractError;
use super::ExtractState;

const ARTICLE_CHANNEL_BOUND: usize = 50;

#[derive(Debug, thiserror::Error)]
#[error("Cancelled extract")]
struct CancelledError;

#[derive(Debug, Args)]
pub struct ExtractSqlCommand {
    /// The output database
    #[clap(long = "out", required = true, parse(from_os_str))]
    output: PathBuf,
    #[clap(long = "workers", short = 'j', default_value = "4")]
    workers: u32,
    /// The limit on the number of articles to extract
    #[clap(long = "limit")]
    limit: Option<u64>,
    /// The target files to extract
    #[clap(required = true, parse(from_os_str))]
    targets: Vec<PathBuf>,
}

struct SqlArticleMessage {
    name: String,
    url: String,
    count: u64,
    compressed_html: Vec<u8>,
}

struct SqlMessageListener {
    article_sender: Sender<SqlArticleMessage>,
    limit: Option<u64>,
}

impl super::ExtractListener for SqlMessageListener {
    fn on_parse(&self, event: super::ParseEvent) -> Result<(), anyhow::Error> {
        if let Some(limit) = self.limit {
            if event.count > limit {
                return Err(CancelledError.into());
            }
        }
        let raw_html = event.article.body.html.as_bytes();
        let compressed = zstd::encode_all(raw_html, /* level */ 1)?;
        self.article_sender
            .send(SqlArticleMessage {
                name: event.article.name,
                url: event.article.url,
                compressed_html: compressed,
                count: event.count,
            })
            .unwrap();
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
fn serialize_article(
    conn: &mut rusqlite::Connection,
    skipped: &AtomicU64,
    message: SqlArticleMessage,
) -> Result<(), anyhow::Error> {
    let tx = conn.transaction()?;
    match tx.execute(
        "INSERT INTO article(name, url) VALUES (?1, ?2);",
        rusqlite::params![&message.name, &message.url],
    ) {
        Ok(_) => {}
        Err(rusqlite::Error::SqliteFailure(cause, _))
            if cause.code == rusqlite::ffi::ErrorCode::ConstraintViolation =>
        {
            let s = skipped.fetch_add(1, Ordering::SeqCst);
            if s % 500 == 0 {
                eprintln!("Skipped {} files", s);
            }
            // Article already exists, just ignore
            return Ok(());
        }
        Err(cause) => return Err(cause.into()),
    }
    let article_id = tx.last_insert_rowid();
    if message.count % 500 == 0 {
        let actual_article_id = tx.query_row(
            "SELECT id FROM article WHERE name=?",
            rusqlite::params![&message.name],
            |row| row.get::<_, i64>(0),
        )?;
        assert_eq!(article_id, actual_article_id);
    }
    tx.execute(
        "INSERT INTO article_body(article_id, compressed_html) VALUES(?1, ?2)",
        rusqlite::params![&article_id, &message.compressed_html],
    )?;
    tx.commit()?;
    super::basic_report_progress(message.count, &message.name, false);
    Ok(())
}
fn spawn_worker(
    state: Arc<ExtractState>,
    article_sender: Sender<SqlArticleMessage>,
    path_recev: Receiver<PathBuf>,
    limit: Option<u64>,
) -> JoinHandle<anyhow::Result<()>> {
    std::thread::spawn(move || {
        let listener = SqlMessageListener {
            article_sender,
            limit,
        };
        while let Ok(target) = path_recev.recv() {
            eprintln!("Processing {}", target.display());
            match state.run_extract(target, &listener) {
                Ok(()) => {}
                Err(ExtractError::Listener(cause)) if cause.is::<CancelledError>() => {} // ignore
                Err(cause) => return Err(cause.into()),
            }
        }
        Ok(())
    })
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
            PRAGMA foreign_keys = ON;
            CREATE TABLE article(
                id INTEGER PRIMARY KEY,
                name VARCHAR(255) UNIQUE NOT NULL,
                url VARCHAR(255) NOT NULL
            );
            CREATE TABLE article_body(
                id INTEGER PRIMARY KEY,
                article_id INTEGER NOT NULL,
                compressed_html BLOB,
                FOREIGN KEY(article_id) REFERENCES article(id)
            );
            CREATE INDEX article_idx_url ON article(url);
        ",
        )?;
        connection.close().map_err(|(_, err)| err)?;
    }
    let mut connection = rusqlite::Connection::open_with_flags(
        &target,
        rusqlite::OpenFlags::SQLITE_OPEN_READ_WRITE,
    )?;
    connection.execute_batch(
        "
        PRAGMA foreign_keys = ON;
        PRAGMA journal_mode = WAL;
    ",
    )?;
    let (article_sender, article_recev) = crossbeam::channel::bounded(ARTICLE_CHANNEL_BOUND);
    let (path_sender, path_recev) = crossbeam::channel::unbounded();
    let state = Arc::new(ExtractState::new());
    assert!(command.workers > 0);
    let mut handles = Vec::new();
    for _ in 0..command.workers {
        handles.push(spawn_worker(
            Arc::clone(&state),
            article_sender.clone(),
            path_recev.clone(),
            command.limit.clone(),
        ))
    }
    drop(article_sender);
    drop(path_recev);
    for target in &command.targets {
        path_sender.send(target.clone()).unwrap();
    }
    drop(path_sender);
    eprintln!("Extracted {} files", state.count());
    let skipped = AtomicU64::new(0);
    while let Ok(article) = article_recev.recv() {
        serialize_article(&mut connection, &skipped, article)?;
    }
    connection.close().map_err(|(_, e)| e)?;
    for worker in handles {
        worker
            .join()
            .map_err(|_| anyhow!("Unexpected panic in worker thread"))??;
    }
    eprintln!(
        "Extracted {} articles from {} different source files",
        state.count(),
        command.targets.len()
    );
    Ok(())
}
