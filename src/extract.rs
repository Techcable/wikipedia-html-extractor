use std::fs::File;
use std::io::BufReader;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex};

use serde::Deserialize;
use serde_json::StreamDeserializer;

pub mod files;
pub mod sql;

pub struct ExtractState {
    count: AtomicU64,
    should_stop: AtomicBool,
    error: Mutex<Option<ExtractError>>,
    error_cond: Condvar,
}
impl ExtractState {
    /// Get a count of the number of items that have been extracted
    #[inline]
    pub fn count(&self) -> u64 {
        self.count.load(Ordering::SeqCst)
    }
    pub fn new() -> Self {
        ExtractState {
            count: AtomicU64::new(0),
            should_stop: AtomicBool::new(false),
            error: Mutex::new(None),
            error_cond: Condvar::new(),
        }
    }
    fn provide_error(&self, error: ExtractError) {
        let mut lock = self.error.lock().unwrap();
        if lock.is_none() {
            *lock = Some(error);
        }
        self.error_cond.notify_all();
    }
    pub fn run_extract(
        &self,
        target: PathBuf,
        listener: &dyn ExtractListener,
    ) -> Result<(), ExtractError> {
        let f = File::open(&target).map_err(|cause| ExtractError::FileIo {
            target: target.clone(),
            cause,
        })?;
        let f = BufReader::new(f);
        let stream: StreamDeserializer<_, Article> =
            serde_json::de::Deserializer::from_reader(f).into_iter();
        for value in stream {
            if self.should_stop.load(Ordering::SeqCst) {
                return Ok(());
            }
            match value {
                Ok(article) => {
                    let count = self.count.fetch_add(1, Ordering::SeqCst);
                    listener
                        .on_parse(ParseEvent {
                            original_file: &target,
                            count,
                            article,
                        })
                        .map_err(ExtractError::Listener)?;
                }
                Err(cause) => {
                    listener
                        .on_parse_error(&target, cause.into())
                        .map_err(ExtractError::Listener)?;
                    continue;
                }
            }
        }
        Ok(())
    }
}

pub struct ThreadedExtractTask {
    handles: Vec<std::thread::JoinHandle<()>>,
    pub state: Arc<ExtractState>,
    pub listener: Arc<dyn ExtractListener + Send + Sync + 'static>,
}
impl ThreadedExtractTask {
    /// Get a count of the number of items that had been extracted
    #[inline]
    pub fn count(&self) -> u64 {
        self.state.count()
    }
    #[inline]
    pub fn is_finished(&self) -> bool {
        self.handles.is_empty()
    }
    pub fn wait(&mut self) -> Result<(), ExtractError> {
        for handle in std::mem::take(&mut self.handles) {
            match handle.join() {
                Err(_) => {
                    self.state.provide_error(ExtractError::UnexpectedPanic);
                }
                Ok(()) => {}
            }
            let mut lock = self.state.error.lock().unwrap();
            if lock.is_some() {
                return Err(lock.take().unwrap());
            }
        }
        Ok(())
    }
}

#[derive(Debug, thiserror::Error)]
pub enum ExtractError {
    #[error("Fatal IO Error in {target}: {cause}")]
    FileIo {
        target: PathBuf,
        cause: std::io::Error,
    },
    #[error("Not a file: {}", target.display())]
    NotAFile { target: PathBuf },
    #[error("Unexpected panic in thread")]
    UnexpectedPanic,
    #[error(transparent)]
    Listener(anyhow::Error),
}

pub trait ExtractListener {
    fn on_parse(&self, event: ParseEvent) -> Result<(), anyhow::Error>;
    fn on_parse_error(
        &self,
        original_file: &Path,
        cause: anyhow::Error,
    ) -> Result<(), anyhow::Error>;
}

pub fn extract_threaded(
    paths: Vec<PathBuf>,
    listener: Box<dyn ExtractListener + Send + Sync + 'static>,
) -> Result<ThreadedExtractTask, ExtractError> {
    let state = Arc::new(ExtractState::new());
    let mut task = ThreadedExtractTask {
        handles: Vec::new(),
        state: Arc::clone(&state),
        listener: Arc::from(listener),
    };
    for target in paths {
        if !target.is_file() {
            return Err(ExtractError::NotAFile { target });
        }
        let state = Arc::clone(&state);
        let listener = Arc::clone(&task.listener);
        let handle = std::thread::spawn(move || match state.run_extract(target, &*listener) {
            Err(error) => {
                state.should_stop.store(true, Ordering::SeqCst);
                state.provide_error(error);
            }
            Ok(()) => {}
        });
        task.handles.push(handle);
    }
    Ok(task)
}

#[derive(Debug, Deserialize)]
pub struct Article {
    pub name: String,
    pub url: String,
    #[serde(rename = "article_body")]
    pub body: ArticleBody,
}

#[derive(Debug, Deserialize)]
pub struct ArticleBody {
    pub html: String,
}

pub struct ParseEvent<'a> {
    pub original_file: &'a Path,
    pub count: u64,
    pub article: Article,
}

pub fn basic_report_progress(count: u64, article_name: &str, verbose: bool) {
    if count % 100 == 0 {
        eprintln!("Processed {} files", count);
    }
    if count % 500 == 0 || verbose {
        eprintln!("Extracted {}", article_name);
    }
}
