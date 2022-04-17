use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use anyhow::anyhow;
use clap::Args;

#[derive(Debug, Args)]
pub struct EnsureNested {
    /// The target directory to convert
    #[clap(required = true, parse(from_os_str))]
    target_dir: PathBuf,
}

pub fn main(cmd: EnsureNested) -> anyhow::Result<()> {
    let target_dir = cmd.target_dir;
    let iterdir = std::fs::read_dir(&target_dir)
        .map_err(|e| anyhow!("Unable to read directory {}: {}", target_dir.display(), e))?;
    let counter = Arc::new(AtomicU64::new(0));
    let existing_dirs = Arc::new(Mutex::new(HashSet::<PathBuf>::new()));
    let (sender, receiver) = crossbeam::channel::bounded::<PathBuf>(500);
    let mut handles = Vec::new();
    for _ in 0..15 {
        let target_dir = PathBuf::clone(&target_dir);
        let counter = Arc::clone(&counter);
        let receiver = receiver.clone();
        let existing_dirs = existing_dirs.clone();
        handles.push(std::thread::spawn(move || {
            while let Ok(target) = receiver.recv() {
                process_file(&*counter, &*target_dir, &*existing_dirs, &*target);
            }
            drop(receiver);
        }));
    }
    for entry in iterdir {
        let entry = match entry {
            Ok(entry) => entry,
            Err(e) => {
                eprintln!("WARNING: Failed to read entry: {}", e);
                continue;
            }
        };
        let original_path = entry.path();
        let ft = match entry.file_type() {
            Ok(ft) => ft,
            Err(e) => {
                eprintln!(
                    "WARNING: Failed to fetch file type of {}: {}",
                    original_path.display(),
                    e
                );
                continue;
            }
        };
        if ft.is_dir() {
            continue;
        }
        sender.send(original_path).unwrap();
    }
    drop(sender);
    for handle in handles {
        handle.join().unwrap();
    }
    Ok(())
}

fn process_file(
    i: &AtomicU64,
    target_dir: &Path,
    existing_dirs: &Mutex<HashSet<PathBuf>>,
    original_path: &Path,
) {
    let name = match original_path.file_name() {
        Some(stem) => stem.to_string_lossy().into_owned(),
        None => {
            eprintln!("WARNING: Path has no name: {}", original_path.display());
            return;
        }
    };
    let mut target_file = PathBuf::from(target_dir);
    let mut chars = name.chars();
    if let Some(first) = chars.next() {
        target_file.push(String::from(first));
        if let Some(second) = chars.next() {
            target_file.push(String::from(second));
        }
    }
    let exists = {
        let lock = existing_dirs.lock().unwrap();
        lock.contains(&target_file)
    };
    if !exists {
        match std::fs::create_dir_all(&target_file) {
            Ok(()) => {
                let mut lock = existing_dirs.lock().unwrap();
                lock.insert(target_file.clone());
                drop(lock)
            }
            Err(e) => {
                eprintln!(
                    "WARNING: Unable to create directory {}: {}",
                    target_file.display(),
                    e
                );
                return;
            }
        }
    }
    target_file.push(name);
    match std::fs::rename(&original_path, &target_file) {
        Ok(()) => {}
        Err(e) => {
            eprintln!(
                "WARNING: Failed to rename {}: {}",
                original_path.display(),
                e
            );
            return;
        }
    }
    let i = i.fetch_add(1, Ordering::SeqCst);
    if i % 100 == 0 {
        eprintln!("Moved {} files", i);
    }
    if i % 500 == 0 {
        eprintln!(
            "Moved {} to {}",
            original_path.display(),
            target_file.display()
        );
    }
}
