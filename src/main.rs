#[macro_use]
extern crate log;
extern crate env_logger;
extern crate json;
extern crate zmq;
extern crate getopts;
extern crate chrono;

use chrono::*;
use getopts::Options;
use std::env;
use std::process;
use std::path::PathBuf;

const ADDR: &'static str = "tcp://127.0.0.1:9090";

struct Args {
    runs: u64,
    output_file: PathBuf,
    quiet: bool,
}

fn print_usage(program_name: &str, opts: &Options) {
    let description = r#"
Collect and save data from various Monto processes.
The collection happens in a number of runs, with each run consisting of:
    * A version v
    * Any and all of v's associated products i.e. all
      received products with the same revision number.

The program collects data during each run, and after all runs are done, it
writes the data to in a file in JSON format. By default the output file has
a name with the format "YYYY-MM-DD_hh:mm:ss.json", representing the date and
time of the moment the file was written."#;
    let arguments = "[-h | --help] [-r R | --run-count R] [-o FILE | --output FILE]";
    println!("Usage: {} {}\n{}",
             program_name,
             arguments,
             opts.usage(description));
}

fn parse_args() -> Args {
    let args: Vec<String> = env::args().collect();
    let program_name = args[0].clone();

    let mut opts = Options::new();
    opts.optopt("r", "run-count", "The number of runs to capture", "R");
    opts.optopt("o", "output",    "Customize output file name",    "FILE");
    opts.optflag("q", "quiet", "Quiet mode i.e. no logging to stdout");
    opts.optflag("h", "help", "Print this help menu");
    let matches = match opts.parse(&args[1..]) {
        Ok(matches) => matches,
        Err(err) => {
            error!("error parsing matches: {}", err);
            print_usage(&program_name, &opts);
            process::exit(0);
        },
    };

    if matches.opt_present("help") {
        print_usage(&program_name, &opts);
        process::exit(0);
    }

    let filename: String = match matches.opt_str("output") {
        None => Local::now().format("%Y-%m-%d_%H:%M:%S.json").to_string(),
        Some(name) => name,
    };

    const DEFAULT_RUN_COUNT: u64 = 10;
    let runs: u64 = match matches.opt_str("run-count") {
        None => DEFAULT_RUN_COUNT,
        Some(count_string) => match count_string.parse::<u64>() {
            Ok(count) => count,
            Err(err) => {
                error!("Couldn't parse run count: {:?}", err);
                DEFAULT_RUN_COUNT
            },
        },
    };

    Args {
        runs: runs,
        output_file: PathBuf::from(filename),
        quiet: matches.opt_present("quiet"),
    }
}

fn main() {
    let args = parse_args();
    if !args.quiet {
        // Only enable logging if quiet mode is not active
        env_logger::init().unwrap();
    }
    info!("Writing to {}", args.output_file.display());
    info!("Capturing {} runs", args.runs);

    let mut ctx = zmq::Context::new();
    let mut socket = ctx.socket(zmq::PULL).expect("Failed to create socket");
    assert!(socket.connect(ADDR).is_ok());

    // socket.
}
