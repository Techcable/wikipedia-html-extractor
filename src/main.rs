use serde::Deserialize;
use serde_json::StreamDeserializer;
use std::env;
use std::fs::File;
use std::io::BufReader;
use std::path::PathBuf;
use std::sync::atomic::{Ordering, AtomicU64};

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
    for arg in env::args().skip(1) {
        let p = PathBuf::from(arg);
        if !p.is_file() {
            eprintln!("Error: Not a file: {}", p.display());
            std::process::exit(1);
        }
        paths.push(p);
    }
    let count = AtomicU64::new(0);
    crossbeam::scope(|scope| {
        for p in paths {
            let target_dir = target_dir.clone();
            let p = PathBuf::from(p);
            let count = &count;
            scope.spawn(move |_| {
                let f = match File::open(&p) {
                    Ok(f) => f,
                    Err(e) => {
                        eprintln!("Error: Failed to open file {}: {}", p.display(), e);
                        return;
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
                            let target_file = target_dir.join(name.replace("/", "__"));
                            match std::fs::write(&target_file, article.body.html.as_bytes()) {
                                Ok(()) => {
                                    let i = count.fetch_add(1, Ordering::SeqCst);
                                    if i % 100 == 0 {
                                        eprintln!("Processed {} files", i);
                                        if i % 500 == 0 {
                                            eprintln!("Just did {:?} to {}", article.name, target_file.display());
                                        }
                                    }
                                },
                                Err(e) => {
                                    eprintln!("ERROR: Failed to write to {}: {}", target_file.display(), e);
                                    continue 'streamLoop;
                                }
                            }
                        },
                        Err(e) => {
                            eprintln!("ERROR: Failed to read from {}: {}", p.display(), e);
                            continue 'streamLoop;
                        }
                    }
                }
            });
        }
    }).unwrap();
    Ok(())
}

fn parse_url(url: &str) -> Result<String, String> {
    const PREFIX: &'static str = "/wiki/"; 
    match url.find(PREFIX) {
        None => {
            Err(format!("No `/wiki/` in {:?}", url))
        },
        Some(idx) => {
            Ok(format!("{}.html", &url[idx + PREFIX.len()..]))
        }
    }
} 