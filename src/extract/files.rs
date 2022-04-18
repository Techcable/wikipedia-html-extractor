use std::{
    path::PathBuf,
    sync::atomic::{AtomicU64, Ordering},
};

use clap::Args;

use crate::extract::ExtractError;

#[derive(Debug, thiserror::Error)]
#[error("Cancelled extract")]
struct CancelledError;

#[derive(Debug, Args)]
pub struct ExtractCommand {
    /// Output verbose information (print every file extracted)
    #[clap(long)]
    verbose: bool,
    /// The limit on the number of files to extract
    #[clap(long)]
    limit: Option<u64>,
    /// Skip existing files
    #[clap(long)]
    skip_existing: bool,
    /// Do not nest the extracted files
    #[clap(long)]
    no_nesting: bool,
    /// The target directory to extract files into
    #[clap(long = "out", parse(from_os_str))]
    output_dir: Option<PathBuf>,
    /// The target files to extract
    #[clap(required = true, parse(from_os_str))]
    targets: Vec<PathBuf>,
}
struct FileExtractListener {
    command: ExtractCommand,
    skipped: AtomicU64,
    target_dir: PathBuf,
}
impl super::ExtractListener for FileExtractListener {
    fn on_parse(&self, event: super::ParseEvent) -> Result<(), anyhow::Error> {
        if let Some(limit) = self.command.limit {
            if event.count >= limit {
                return Err(CancelledError.into());
            }
        }
        let name = match parse_url(&event.article.url) {
            Err(msg) => {
                eprintln!("WARNING: {}", msg);
                return Ok(());
            }
            Ok(name) => sanitize_name(&*name),
        };
        let mut target_file = self.target_dir.clone();
        let mut chars = name.chars();
        if !self.command.no_nesting {
            if let Some(first) = chars.next() {
                target_file.push(String::from(first));
                if let Some(second) = chars.next() {
                    target_file.push(String::from(second));
                }
            }
        }
        match std::fs::create_dir_all(&target_file) {
            Ok(()) => {}
            Err(e) => {
                eprintln!(
                    "WARNING: Unable to create directory {}: {}",
                    target_file.display(),
                    e
                );
                return Ok(());
            }
        }
        target_file.push(name);
        if self.command.skip_existing && target_file.is_file() {
            let i = self.skipped.fetch_add(1, Ordering::SeqCst);
            if i % 500 == 0 {
                eprintln!("Skipped {} files", i);
            }
            return Ok(());
        }
        match std::fs::write(&target_file, event.article.body.html.as_bytes()) {
            Ok(()) => {
                super::basic_report_progress(
                    event.count,
                    &event.article.name,
                    self.command.verbose,
                );
                Ok(())
            }
            Err(e) => {
                eprintln!("ERROR: Failed to write to {}: {}", target_file.display(), e);
                Ok(())
            }
        }
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
pub fn extract(command: ExtractCommand) -> anyhow::Result<()> {
    eprintln!("WARNING: This command is deprecated. It overloads the FS");
    eprintln!("Consider using the new `extract` command (uses SQLite)");
    let target_dir = command
        .output_dir
        .clone()
        .unwrap_or_else(|| PathBuf::from("extracted"));
    if !target_dir.is_dir() {
        std::fs::create_dir(&target_dir)?;
    }
    let paths = command.targets.clone();
    let listener = FileExtractListener {
        command,
        skipped: AtomicU64::new(0),
        target_dir,
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
