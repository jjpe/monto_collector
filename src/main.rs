#[macro_use]
extern crate log;
extern crate env_logger;
#[macro_use]
extern crate json;
extern crate zmq;
extern crate getopts;
extern crate chrono;

use chrono::*;
use getopts::Options;
use std::env;
use std::process;
use std::path::PathBuf;

struct Args {
    events: u64,
    output_file: PathBuf,
    quiet: bool,
}

fn print_usage(program_name: &str, opts: &Options) {
    let description = r#"
Collect and save events from various Monto processes.
After some number of events is captured, the data is written
to a file in JSON format. By default the output file has a
name with the format "YYYY-MM-DD_hh:mm:ss.json", representing
the date and time of the moment the file was written."#;
    println!("Usage: {} {}\n{}",
             program_name,
             "[-h | --help] [-e N | --events N] [-o FILE | --output FILE]",
             opts.usage(description));
}

fn parse_args() -> Args {
    let args: Vec<String> = env::args().collect();
    let program_name = args[0].clone();

    let mut opts = Options::new();
    opts.optopt("e", "events", "The number of events to capture", "N");
    opts.optopt("o", "output", "Output file name", "FILE");
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

    const DEFAULT_EVENT_CAPTURE_COUNT: u64 = 10;
    let event_count: u64 = match matches.opt_str("events") {
        None => DEFAULT_EVENT_CAPTURE_COUNT,
        Some(count_string) => match count_string.parse::<u64>() {
            Ok(count) => count,
            Err(err) => {
                error!("Couldn't parse -e/--events arg: {:?}", err);
                DEFAULT_EVENT_CAPTURE_COUNT
            },
        },
    };

    Args {
        events: event_count,
        output_file: PathBuf::from(filename),
        quiet: matches.opt_present("quiet"),
    }
}

fn log_entry_header(eventno: usize) {
    if eventno % HEADER_LOG_PERIOD == 1 {
        // Log the header before every `HEADER_LOG_PERIOD` entries
        info!("    {:->90}", "");
        info!("    {:^15}{:^14}{:^25}{:^20}{:>8}",
              "timestamp",
              "event #",
              "action",
              "process",
              "revision");
        info!("    {:->90}", "");
    }
}

/// Log a header before every X logged events.
const HEADER_LOG_PERIOD: usize = 50;


const TIMESTAMP_FORMAT: &'static str = "%Y-%m-%dT%H:%M:%S%.9f%z";

fn jsonify_timestamp(ts: DateTime<Local>) -> String {
    format!("{}", ts.format(TIMESTAMP_FORMAT))
}

fn unjsonify_timestamp(ts: &json::JsonValue)
                       -> DateTime<FixedOffset> {
    let ts_string = ts.as_str().expect("Deserializing timestamp failed");
    DateTime::parse_from_str(ts_string, TIMESTAMP_FORMAT)
        .expect("Deserializing timestamp failed")
}

fn main() {
    let args = parse_args();
    if !args.quiet {
        env_logger::init().unwrap(); // Only log if quiet mode is not active
    }

    let mut ctx = zmq::Context::new();
    let mut socket = ctx.socket(zmq::PULL).expect("Failed to create socket");
    assert!(socket.bind("tcp://*:9090").is_ok());

    let mut events = array![];
    let mut msg = zmq::Message::new().unwrap();
    const WAIT: i32 = 0;

    info!("Writing to {}", args.output_file.display());
    info!("Capturing {} events", args.events);
    for eventno in 1.. {
        assert_eq!(socket.recv(&mut msg, WAIT).unwrap(),  ());
        let json_str = msg.as_str().unwrap();
        let received_at: DateTime<Local> = Local::now();
        let mut report = json::parse(json_str).unwrap();
        let action_json =   report["action"].take();
        let process_json =  report["process"].take();
        let revision_json = report["revision"].take();
        let action =   action_json.as_str().unwrap();
        let process =  process_json.as_str().unwrap();
        let revision = revision_json.as_u64().unwrap();

        log_entry_header(eventno);
        info!("    {: <15}   {:>3}/{}   {:^25}{:^20}{:>6}",
              received_at.time(),
              eventno, args.events,
              action,
              process,
              revision
        );

        let event = object! {
            "action" => action,
            "process" => process,
            "revision" => revision,
            "timestamp" => jsonify_timestamp(received_at)
        };

        // {
        //     let ts = unjsonify_timestamp(&event["timestamp"]);
        //     info!("timestamp = {}", ts.time());
        // }

        // info!("event: {:?}    {}", event, received_at.time());
        assert_eq!(events.push(event).unwrap(),  ());
        if eventno == args.events as usize {
            break; // Exit after `args.events` events
        }

    }
}
