use std::path::PathBuf;

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
    let mut i = 0;
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
        let mut target_file = target_dir.clone();
        let name = match original_path.file_name() {
            Some(stem) => stem.to_string_lossy().into_owned(),
            None => {
                eprintln!("WARNING: Path has no name: {}", original_path.display());
                continue;
            }
        };
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
                continue;
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
                continue;
            }
        }
        i += 1;
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
}
