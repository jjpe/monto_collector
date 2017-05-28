#[macro_use(bson, doc)] extern crate bson;
extern crate clap;
extern crate env_logger;
extern crate json;
extern crate libc;
extern crate libcereal;
#[macro_use(info, log, warn)] extern crate log;
extern crate mongodb;
extern crate time;
extern crate url;

use bson::ordered::OrderedDocument;
use clap::{App, Arg, SubCommand};
use libcereal::Method;
use libcereal::amplify::{BReportReceiver, Report, UReportReceiver};
use mongodb::{ThreadedClient};
use mongodb::db::ThreadedDatabase;
use std::fmt;
use url::Url;

const BIN_NAME: &str = env!("CARGO_PKG_NAME");
const VERSION: &str = env!("CARGO_PKG_VERSION");
const AUTHORS: &str = env!("CARGO_PKG_AUTHORS");
const DESCRIPTION: &str = r#"Collect and save events from Amplify processes.
After all events are captured, the data is written to a MongoDB database."#;


#[derive(Clone, Debug)]
struct Args {
    events: LoopStrategy,
    mongodb_url: Url,
    quiet: bool,
    header_period: u64,
    method: Method,
}

impl Args {
    fn process() -> Args {
        let matches = App::new(BIN_NAME)
            .version(VERSION)
            .author(AUTHORS)
            .about(DESCRIPTION)
            .arg(Arg::with_name("events")
                 .short("e")
                 .long("events")
                 .value_name("N")
                 .help("The number of events to capture.
The default is indefinite i.e. don't stop unkil killed."))
            .arg(Arg::with_name("mongo-url")
                 .short("u")
                 .long("mongo-url")
                 .value_name("URL")
                 .help("Customize the URL used to connect to the MongoDB server.
The default is mongodb://localhost:27017."))
            .arg(Arg::with_name("quiet")
                 .short("q")
                 .long("quiet")
                 .help("Quiet mode i.e. no logging to stdout."))
            .arg(Arg::with_name("header-period")
                 .short("p")
                 .long("header-period")
                 .value_name("P")
                 .help("Log a header above every P logged events."))
            .arg(Arg::with_name("serialize-using-json")
                 .short("j")
                 .long("serialize-using-json")
                 .help("Serialize reports using JSON."))
            .arg(Arg::with_name("serialize-using-capnp")
                 .short("c")
                 .long("serialize-using-capnp")
                 .help("Serialize reports using Cap'n Proto."))
        // TODO:
        // .subcommand(SubCommand::with_name("test")
        //             .about("controls testing features")
        //             .version(VERSION)
        //             .author(AUTHORS)
        //             .arg_from_usage("-d, --debug 'Print debug information'"))
            .get_matches();

        let method: Method =
            if matches.is_present("serialize-using-json") { Method::Json }
            else { Method::CapnProto };

        Args {
            events: match matches.value_of("events") {
                None => LoopStrategy::Loop,
                Some(s) => match s{
                    "loop"|"infinite" => LoopStrategy::Loop,
                    s => {
                        let num_events: u64 = s.parse().unwrap(/* TODO: std::num::ParseIntError */);
                        LoopStrategy::Finite(num_events)
                    }
                },
            },
            mongodb_url: matches.value_of("mongo-url")
                .unwrap_or("mongodb://localhost:27017")
                .parse().unwrap(/* TODO: url::ParseError */),
            quiet: matches.is_present("quiet"),
            header_period: matches.value_of("header-period")
                .unwrap_or("50")
                .parse().unwrap(/* TODO: std::num::ParseIntError */),
            method: method,
        }
    }
}


#[derive(Clone, Debug)]
enum LoopStrategy {
    Loop,
    Finite(u64),
}

impl fmt::Display for LoopStrategy {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            LoopStrategy::Loop => write!(f, "loop"),
            LoopStrategy::Finite(num_events) => write!(f, "{}", num_events),
        }
    }
}



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
struct CollectorDb {
    client: mongodb::Client,
    events: mongodb::coll::Collection,
}

impl CollectorDb {
    pub fn connect(url: &Url) -> Self {
        let client = mongodb::Client::with_uri(url.as_str())
            .expect("Failed to initialize MongoDB connector");
        let coll = client.db("collector").collection("events");
        CollectorDb {
            client: client,
            events: coll,
        }
    }

    pub fn write_events(&self, events: Vec<bson::Document>) {
        let len = events.len();
        match self.events.insert_many(events, None) {
            Ok(_) => info!("Wrote {} events to MongoDB", len),
            Err(err) => warn!("Inserting events failed: {:?}", err),
        };
    }
}


fn log_event_header(eventno: u64, args: &Args) {
    if eventno % args.header_period == 1 {
        // Log the header every `args.header_period` entries
        info!("    {:->110}", "");
        info!("    {:^37}{:^15}{:^23}{:^20}{:>8}",
              "timestamp",
              "event #",
              "action",
              "process",
              "request no.");
        info!("    {:->110}", "");
    }
}

fn log_event(eventno: u64, timestamp: &str, args: &Args, report: &Report) {
    log_event_header(eventno, args);
    // while timestamp.len() < 15 { timestamp.push(' '); }
    let eventno_part = match args.events {
        LoopStrategy::Loop => format!("{:>6}", eventno),
        LoopStrategy::Finite(num_events) =>
            format!("{:>3}/{}", eventno, num_events),
    };
    info!("    {:-<15}   {}   {:^25}{:^20}{:>6}",
          timestamp,
          eventno_part,
          report.action_ref(),
          report.process_ref(),
          report.request_number(),
    );
}

fn main() {
    std::env::set_var("RUST_LOG", BIN_NAME);
    let args = Args::process();
    if !args.quiet { env_logger::init().unwrap(/* TODO: SetLoggerErr */); }

    info!("MongoDB @ {}", args.mongodb_url);
    let now = time::now_utc().to_local();
    info!("Started @ {}", stringify_timestamp(&now));
    match args.events {
        LoopStrategy::Loop => info!("Capturing events until killed"),
        LoopStrategy::Finite(n) => info!("Capturing {} events", n),
    }
    info!("Waiting for connection");

    let mut receiver: BReportReceiver = UReportReceiver::new()
        .unwrap(/* TODO: ReportErr */)
        .serialization_method(args.method)
        .bind().unwrap(/* TODO: ReportErr */);
    let db = CollectorDb::connect(&args.mongodb_url);
    let mut events = vec![];
    let mut report = Report::default();

    let loop_entry = &mut |eventno: u64, mut events: Vec<OrderedDocument>| {
        receiver.receive(&mut report).unwrap(/* TODO: ReportErr */);

        match report.command_ref() {
            None => {/* Not a command but a report, so deal with it below. */},
            Some("flush") => if events.len() > 0 {
                db.write_events(events);
                info!("Flushed events");
                return vec![]; // new events
            },
            Some("exit") => {
                info!("Exiting");
                std::process::exit(0);
            },
            Some(cmd) => {
                warn!("Ignoring unknown command '{}'", cmd);
                return events;
            },
        }

        let timestamp: time::Tm = time::now_utc();
        let timestamp_ns: u64 = to_nanoseconds(timestamp);
        let timestamp_string: String = stringify_timestamp(&timestamp);
        log_event(eventno, &timestamp_string, &args, &report);

        events.push(doc! {
            "action" => { report.action_ref() },
            "process" => { report.process_ref() },
            // TODO: Change `revision` to `request_number` in MongoDB:
            "revision" => { report.request_number() },
            "duration_nanos" => { report.duration_nanos() },
            "collector_timestamp" => timestamp_string,
            "collector_timestamp_ns" => timestamp_ns
        });
        events
    };

    match args.events {
        LoopStrategy::Loop =>
            for eventno in 1 .. {
                events = loop_entry(eventno, events);
            },
        LoopStrategy::Finite(num_events) =>
            for eventno in 1 .. num_events + 1 {
                events = loop_entry(eventno, events);
            },
    }
}

//  LocalWords:  mongo MongoDB url perf stringify ns capnp
