use log::error;

use std::collections::HashMap;
use std::sync::Arc;

use hyper::{rt::Future, service::service_fn, Body, Method, Request, Response, Server, StatusCode};

use futures::future;
use futures::stream::{poll_fn, Stream};

mod lines;
mod parser;
mod processors;
mod settings;

use bytes::Bytes;
use crate::lines::Reader;
use crate::parser::get_measurement_name;
use crate::processors::MetricProcessor;
use crate::settings::Settings;
use futures::Poll;

use clap::{App, Arg, ArgMatches};

type BoxFut = Box<Future<Item = Response<Body>, Error = hyper::Error> + Send>;

fn run(buf: &[u8], processors: Arc<HashMap<String, MetricProcessor>>) -> usize {
    let measurement_name = get_measurement_name(buf);
    match measurement_name {
        Some((remaining, name)) => match processors.get(name) {
            Some(processor) => processor.process(name, remaining),
            None => 0,
        },
        None => 0,
    }
}

fn intercept(req: Request<Body>, processors: Arc<HashMap<String, MetricProcessor>>) -> BoxFut {
    let mut response = Response::new(Body::empty());
    match (req.method(), req.uri().path()) {
        (&Method::POST, "/write") => {
            println!("/write");
            let body = req.into_body();
            let mut reader = Reader::new(body);

            let mapping =
                poll_fn(move || -> Poll<Option<Bytes>, hyper::Error> { reader.read_line() })
                    .fold(0, move |counter, buf| {
                        run(&buf, processors.clone());
                        future::ok::<_, hyper::Error>(counter + 1)
                    }).then(move |_| {
                        *response.status_mut() = StatusCode::OK;
                        future::ok::<_, hyper::Error>(response)
                    });

            return Box::new(mapping);
        }
        _ => {
            *response.status_mut() = StatusCode::NOT_FOUND;
        }
    };

    Box::new(future::ok(response))
}

fn build_processors(settings: &mut Settings) -> HashMap<String, MetricProcessor> {
    let mut map = HashMap::new();
    match &settings.measurements {
        Some(m) => {
            for (key, value) in m {
                println!("Measurement {} goes to {}/{}", key, value.server, value.db);
                let processor = match &value.strip_tags {
                    Some(tags) => MetricProcessor::new(tags.clone()),
                    None => MetricProcessor::new(Vec::new()),
                };
                map.insert(key.clone(), processor);
            }
        }
        None => {}
    }
    map
}

fn args() -> ArgMatches {
    App::new("Interflux")
        .version("0.1.0")
        .author("Mark Rendle <mark@rendlelabs.com>")
        .about("Pre-processing and routing for InfluxDB metrics")
        .arg(Arg::with_name("config")
            .short("c")
            .long("config")
            .value_name("FILE")
            .help("Sets a custom config file")
            .required(false)
            .takes_value(true))
        .get_matches()
}

fn main() {
    let arg_matches = args();

    let config_path = arg_matches.value_of("config").unwrap_or("config.toml");

    let result = settings::load(config_path);
    let mut settings: Settings;

    match result {
        Ok(s) => {
            settings = s;
        }
        Err(err) => {
            error!("Config load error {}", err);
            return;
        }
    }

    let processors = Arc::new(build_processors(&mut settings));

    let addr = ([0, 0, 0, 0], 8080).into();

    let service = move || {
        let processors = processors.clone();

        service_fn(move |req| intercept(req, processors.clone()))
    };

    let server = Server::bind(&addr)
        .serve(service)
        .map_err(|e| eprintln!("Server error: {}", e));

    println!("Started http server: 0.0.0.0:8080");

    hyper::rt::run(server);
}
