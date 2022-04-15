use serde::Deserialize;
use serde_json::StreamDeserializer;
use std::env;
use std::fs::File;
use std::io::BufReader;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};

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

pub fn main() -> anyhow::Result<()> {
    let target_dir = PathBuf::from("extracted");
    if !target_dir.is_dir() {
        std::fs::create_dir(&target_dir)?;
    }
    let mut paths = Vec::new();
    let mut skip_existing = false;
    for arg in env::args().skip(1) {
        if arg.starts_with("--") {
            match &*arg {
                "--skip-existing" => {
                    skip_existing = true;
                    continue;
                }
                _ => {
                    eprintln!("Unknown option: {:?}", arg);
                    std::process::exit(1);
                }
            }
        }
        let p = PathBuf::from(arg);
        if !p.is_file() {
            eprintln!("Error: Not a file: {}", p.display());
            std::process::exit(1);
        }
        paths.push(p);
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
                                Ok(name) => name,
                            };
                            let target_file = target_dir.join(
                                name.replace("/", "__")
                                    .replace(":", "__colon__")
                                    .replace("*", "__star__"),
                            );
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
                                        if i % 500 == 0 {
                                            eprintln!(
                                                "Just did {:?} to {}",
                                                article.name,
                                                target_file.display()
                                            );
                                        }
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
