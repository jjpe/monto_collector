#[macro_use] extern crate log;
extern crate env_logger;
#[macro_use] extern crate json;
extern crate zmq;
extern crate getopts;
#[macro_use(bson, doc)] extern crate bson;
extern crate mongodb;
extern crate url;
extern crate time;

use getopts::Options;
use mongodb::{Client, ThreadedClient};
use mongodb::coll::Collection;
use mongodb::db::ThreadedDatabase;
use std::env;
use std::process;
use url::Url;

struct Args {
    events: u64,
    quiet: bool,
    mongodb_url: Url,
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
    opts.optopt("m", "mongo-url", "URL used to connect to the MongoDB server", "URI");
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

    let mongodb_url: Url = match matches.opt_str("mongo-url") {
        None => Url::parse("mongodb://localhost:27017")
            .expect("Could not parse default MongoDB server URL"),
        Some(raw_url) => Url::parse(&raw_url)
            .expect("Could not parse provided MongoDB server URL"),
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
        quiet: matches.opt_present("quiet"),
        mongodb_url: mongodb_url,
    }
}

/// Log a header before every X logged events.
const HEADER_LOG_PERIOD: usize = 50;

fn to_nanoseconds(time: time::Tm) -> u64 {
    const NANOS_PER_SEC: u64 = 1_000_000_000;
    let (mut nanos, timespec) = (0, time.to_timespec());
    nanos += timespec.sec  as u64 * NANOS_PER_SEC;
    nanos += timespec.nsec as u64;
    return nanos;
}

fn stringify_timestamp(time: &time::Tm) -> String {
    const SECS_PER_HOUR: i32 = 60 * 60;
    format!("{:04}-{:02}-{:02} {:02}:{:02}:{:02}.{:09} (UTC{:+})",
            1900 + time.tm_year,
               1 + time.tm_mon,
                   time.tm_mday,
                   time.tm_hour,
                   time.tm_min,
                   time.tm_sec,
                   time.tm_nsec,
                   time.tm_utcoff / SECS_PER_HOUR)
}

#[allow(unused)]
struct SpoofaxPerfDb {
    client: Client,
    events: Collection,
}

impl SpoofaxPerfDb {
    pub fn connect(url: &Url) -> Self {
        let client = Client::with_uri(url.as_str())
            .ok().expect("Failed to initialize MongoDB connector");
        let coll = client.db("spoofax_perf").collection("events");
        SpoofaxPerfDb {
            client: client,
            events: coll,
        }
    }

    pub fn write_events(self, events: Vec<bson::Document>) -> Self {
        let len = events.len();
        match self.events.insert_many(events, None) {
            Ok(_) => info!("Wrote {} events to MongoDB", len),
            Err(err) => panic!("Inserting events failed: {:?}", err),
        }
        self
    }
}


fn log_event_header(eventno: usize) {
    if eventno % HEADER_LOG_PERIOD == 1 {
        // Log the header every `HEADER_LOG_PERIOD` entries
        info!("    {:->110}", "");
        info!("    {:^37}{:^15}{:^23}{:^20}{:>8}",
              "timestamp",
              "event #",
              "action",
              "process",
              "revision");
        info!("    {:->110}", "");
    }
}

fn log_event(eventno: usize,
             timestamp: &str,
             args: &Args,
             action: &str,
             process: &str,
             revision: u64) {
    log_event_header(eventno);
    // while timestamp.len() < 15 { timestamp.push(' '); }
    info!("    {:-<15}   {:>3}/{}   {:^25}{:^20}{:>6}",
          timestamp,
          eventno, args.events,
          action,
          process,
          revision
    );
}

fn main() {
    let args = parse_args();
    if !args.quiet { env_logger::init().unwrap(); }

    let mut ctx = zmq::Context::new();
    let mut socket = ctx.socket(zmq::PULL).expect("Failed to create socket");
    assert!(socket.bind("tcp://*:9090").is_ok());

    let mut msg = zmq::Message::new().unwrap();
    const WAIT: i32 = 0;

    info!("Capturing {} events", args.events);
    info!("MongoDB URI:  {}", args.mongodb_url);
    let now = time::now_utc().to_local();
    info!("timestamp: {}", stringify_timestamp(&now));

    let mut events = vec![];
    for eventno in 1 .. args.events as usize + 1 {
        assert!(socket.recv(&mut msg, WAIT).is_ok());

        let timestamp: time::Tm = time::now_utc();
        let timestamp_ns: u64 = to_nanoseconds(timestamp);
        let timestamp_string = stringify_timestamp(&timestamp);

        let json_str = msg.as_str().unwrap();
        let mut report: json::JsonValue = json::parse(json_str).unwrap();
        let action_json =   report["action"].take();
        let process_json =  report["process"].take();
        let revision_json = report["revision"].take();
        let duration_json = report["duration"].take();
        let   action =   action_json.as_str().expect("action as &str");
        let  process =  process_json.as_str().expect("process as &str");
        let revision = revision_json.as_u64().expect("revision as u64");
        let duration = duration_json.as_u64().expect("duration as u64");
        log_event(eventno, &timestamp_string, &args, action, process, revision);

        let event = doc! {
            "action" => action,
            "process" => process,
            "revision" => revision,
            "duration_nanos" => duration,
            "collector_timestamp" => timestamp_string,
            "collector_timestamp_ns" => timestamp_ns
        };
        events.push(event);
    }

    let spdb = SpoofaxPerfDb::connect(&args.mongodb_url);
    spdb.write_events(events);
    info!("Exiting");
}

//  LocalWords:  mongo MongoDB url perf stringify ns
