use std::fs::File;
use std::io::{BufReader, BufWriter};
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{Ordering, AtomicU64};

use anyhow::{anyhow, Result};
use clap::Args;
use serde::ser::{SerializeSeq, Serializer};
use serde_json::StreamDeserializer;

#[derive(Debug, Args)]
pub struct IndexCommand {
    /// The target directory to put indexed files into
    #[clap(long = "out", parse(from_os_str))]
    out_dir: Option<PathBuf>,
    /// The files to index
    #[clap(required = true, parse(from_os_str))]
    targets: Vec<PathBuf>,
}

#[derive(serde::Deserialize, serde::Serialize, Debug)]
pub struct ArticleMetadata {
    name: String,
    url: String,
}

pub fn main(command: IndexCommand) -> anyhow::Result<()> {
    let out_dir = command
        .out_dir
        .clone()
        .unwrap_or_else(|| PathBuf::from("index"));
    std::fs::create_dir_all(&out_dir)?;
    let count = Arc::new(AtomicU64::new(0));
    let mut handles = Vec::new();
    for target in command.targets {
        let file_name = target
                .file_stem()
                .ok_or_else(|| anyhow!("Expected file name for {}", target.display()))?
                .to_string_lossy().into_owned();
        let out_file = out_dir.join(format!(
            "{}-index.json",
            &file_name
        ));
        let count = Arc::clone(&count);
        handles.push(std::thread::spawn(handle_errors(move || {
            let f = File::open(&target)
                .map_err(|e| anyhow!("Failed to open file {}: {}", target.display(), e))?;
            let f = BufReader::new(f);
            let stream: StreamDeserializer<_, ArticleMetadata> =
                serde_json::de::Deserializer::from_reader(f).into_iter();
            let out = File::create(&out_file).map_err(|e| {
                anyhow!("Error: Failed to create file {}: {}", out_file.display(), e)
            })?;
            let out = BufWriter::new(out);
            let mut ser = serde_json::Serializer::new(out);
            let mut seq = ser.serialize_seq(None)?;
            'streamLoop: for value in stream {
                match value {
                    Ok(value) => {
                        let meta: ArticleMetadata = value;
                        match seq.serialize_element(&meta) {
                            Ok(()) => {
                                let i = count.fetch_add(1, Ordering::SeqCst);
                                if i % 500 == 0 {
                                    eprintln!("Indexed {} articles", i);
                                }
                                if i % 5000 == 0 {
                                    eprintln!("Indexed {} in {}", &meta.name, &file_name)
                                }
                            }
                            Err(e) => {
                                eprintln!(
                                    "WARNING: Failed to write to {}: {}",
                                    out_file.display(),
                                    e
                                );
                                continue 'streamLoop;
                            }
                        }
                    }
                    Err(e) => {
                        eprintln!("WARNING: Failed to read from {}: {}", target.display(), e);
                        continue 'streamLoop;
                    }
                }
            }
            seq.end()?;
            Ok(())
        })));
    }
    for handle in handles {
        handle
            .join()
            .map_err(|_e| anyhow!("Failed to run thread"))?;
    }
    eprintln!("Indexed total of {} articles", count.load(Ordering::SeqCst));
    Ok(())
}

fn handle_errors(func: impl FnOnce() -> Result<(), anyhow::Error>) -> impl FnOnce() {
    || match func() {
        Err(e) => {
            eprintln!("ERROR: {}", e);
            std::process::exit(1)
        }
        Ok(()) => {}
    }
}
