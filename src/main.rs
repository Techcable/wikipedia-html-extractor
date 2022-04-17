use clap::{Parser, Subcommand};

mod extract;

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
    Extract(extract::ExtractCommand),
}

pub fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();
    match cli.command {
        Command::Extract(cmd) => extract::extract(cmd)?,
    }
    Ok(())
}
