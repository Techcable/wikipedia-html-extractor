use std::fs::File;
use std::io::BufReader;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};

use clap::Args;
use serde::Deserialize;
use serde_json::StreamDeserializer;

#[derive(Debug, Args)]
pub struct ExtractCommand {
    /// Output verbose information (print every file extracted)
    #[clap(long)]
    verbose: bool,
    /// Skip existing files
    #[clap(long)]
    skip_existing: bool,
    /// The target directory to extract files into
    #[clap(long = "out", parse(from_os_str))]
    output_dir: Option<PathBuf>,
    /// The target files to extract
    #[clap(required = true, parse(from_os_str))]
    targets: Vec<PathBuf>,
}

#[derive(Debug, Deserialize)]
struct ArticleEntry {
    name: String,
    url: String,
    #[serde(rename = "article_body")]
    body: ArticleBody,
}

#[derive(Debug, Deserialize)]
struct ArticleBody {
    html: String,
}

pub fn extract(command: ExtractCommand) -> anyhow::Result<()> {
    let target_dir = command
        .output_dir
        .clone()
        .unwrap_or_else(|| PathBuf::from("extracted"));
    if !target_dir.is_dir() {
        std::fs::create_dir(&target_dir)?;
    }
    let paths = command.targets;
    let skip_existing = command.skip_existing;
    let verbose = command.verbose;
    for p in &paths {
        if !p.is_file() {
            eprintln!("Error: Not a file: {}", p.display());
            std::process::exit(1);
        }
    }
    let count = AtomicU64::new(0);
    let skipped = AtomicU64::new(0);
    crossbeam::scope(|scope| {
        for p in paths {
            let target_dir = target_dir.clone();
            let p = PathBuf::from(p);
            let count = &count;
            let skipped = &skipped;
            scope.spawn(move |_| {
                let f = match File::open(&p) {
                    Ok(f) => f,
                    Err(e) => {
                        eprintln!("Error: Failed to open file {}: {}", p.display(), e);
                        std::process::exit(1);
                    }
                };
                let f = BufReader::new(f);
                let stream: StreamDeserializer<_, ArticleEntry> =
                    serde_json::de::Deserializer::from_reader(f).into_iter();
                'streamLoop: for value in stream {
                    match value {
                        Ok(article) => {
                            let name = match parse_url(&article.url) {
                                Err(msg) => {
                                    eprintln!("WARNING: {}", msg);
                                    continue 'streamLoop;
                                }
                                Ok(name) => sanitize_name(&*name),
                            };
                            let mut target_file = PathBuf::clone(&target_dir);
                            let mut chars = name.chars();
                            if let Some(first) = chars.next() {
                                target_file.push(String::from(first));
                                if let Some(second) = chars.next() {
                                    target_file.push(String::from(second));
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
                                    continue 'streamLoop;
                                }
                            }
                            target_file.push(name);
                            if skip_existing && target_file.is_file() {
                                let i = skipped.fetch_add(1, Ordering::SeqCst);
                                if i % 500 == 0 {
                                    eprintln!("Skipped {} files", i);
                                }
                                continue 'streamLoop;
                            }
                            match std::fs::write(&target_file, article.body.html.as_bytes()) {
                                Ok(()) => {
                                    let i = count.fetch_add(1, Ordering::SeqCst);
                                    if i % 100 == 0 {
                                        eprintln!("Processed {} files", i);
                                    }
                                    if i % 500 == 0 || verbose {
                                        eprintln!(
                                            "Extracted {:?} to {}",
                                            article.name,
                                            target_file.display()
                                        );
                                    }
                                }
                                Err(e) => {
                                    eprintln!(
                                        "ERROR: Failed to write to {}: {}",
                                        target_file.display(),
                                        e
                                    );
                                    continue 'streamLoop;
                                }
                            }
                        }
                        Err(e) => {
                            eprintln!("ERROR: Failed to read from {}: {}", p.display(), e);
                            continue 'streamLoop;
                        }
                    }
                }
            });
        }
    })
    .unwrap();
    Ok(())
}

fn parse_url(url: &str) -> Result<String, String> {
    const PREFIX: &'static str = "/wiki/";
    match url.find(PREFIX) {
        None => Err(format!("No `/wiki/` in {:?}", url)),
        Some(idx) => Ok(format!("{}.html", &url[idx + PREFIX.len()..])),
    }
}

pub fn sanitize_name(name: &str) -> String {
    name.replace("/", "__")
        .replace(":", "__colon__")
        .replace("*", "__star__")
}
