use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

pub fn main() {
    let mut args = std::env::args().skip(1);
    let target_dir = PathBuf::from(match args.next() {
        Some(val) => val,
        None => {
            eprintln!("ERROR: Please specify a directory to convert");
            std::process::exit(1);
        }
    });
    if args.next().is_some() {
        eprintln!("ERROR: Please only specify 1 argument");
        std::process::exit(1);
    }
    let iterdir = match std::fs::read_dir(&target_dir) {
        Err(e) => {
            eprintln!("Unable to read directory {}: {}", target_dir.display(), e);
            std::process::exit(1);
        }
        Ok(iter) => iter,
    };
    let counter = Arc::new(AtomicU64::new(0));
    let (sender, receiver) = crossbeam::channel::bounded::<PathBuf>(500);
    let mut handles = Vec::new();
    for _ in 0..15 {
        let target_dir = PathBuf::clone(&target_dir);
        let counter = Arc::clone(&counter);
        let receiver = receiver.clone();
        handles.push(std::thread::spawn(move || {
            while let Ok(target) = receiver.recv() {
                process_file(&*counter, &*target_dir, &*target);
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
}

fn process_file(i: &AtomicU64, target_dir: &Path, original_path: &Path) {
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
    match std::fs::create_dir_all(&target_file) {
        Ok(()) => {}
        Err(e) => {
            eprintln!(
                "WARNING: Unable to create directory {}: {}",
                target_file.display(),
                e
            );
            return;
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
