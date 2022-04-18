use clap::{Parser, Subcommand};

mod ensure_nested;
mod extract_files;
mod index;

#[derive(Parser, Debug)]
#[clap(author, version)]
#[clap(about = "Commands to manipulate and analyse wikipedia HTML dumps")]
#[clap(propagate_version = true)]
struct Cli {
    #[clap(subcommand)]
    command: Command,
}

#[derive(Subcommand, Debug)]
enum Command {
    ExtractFiles(extract_files::ExtractCommand),
    EnsureNested(ensure_nested::EnsureNested),
    Index(index::IndexCommand),
}

pub fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();
    match cli.command {
        Command::ExtractFiles(cmd) => extract_files::extract(cmd),
        Command::EnsureNested(cmd) => ensure_nested::main(cmd),
        Command::Index(cmd) => index::main(cmd),
    }
}
