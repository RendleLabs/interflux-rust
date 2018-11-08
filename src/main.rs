use log::error;

use std::collections::HashMap;
use std::sync::Arc;

use hyper::{
    rt::Future, service::service_fn, Body, Method, Request, Response, Server, StatusCode,
};

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
use futures::future::ok;
use futures::Poll;

type BoxFut = Box<Future<Item = Response<Body>, Error = hyper::Error> + Send>;

fn run(buf: &[u8], processors: Arc<HashMap<String, MetricProcessor>>) -> usize {
    let name = get_measurement_name(buf);
    match name {
        Some((r, n)) => match processors.get(n) {
            Some(a) => a.process(n, r),
            None => 0,
        },
        None => 0,
    }
}

fn intercept(req: Request<Body>, processors: Arc<HashMap<String, MetricProcessor>>) -> BoxFut {
    let mut response = Response::new(Body::empty());
    match (req.method(), req.uri().path()) {
        (&Method::POST, "/write") => {
            let body = req.into_body();
            let mut reader = Reader::new(body);

            let lines = poll_fn(move || -> Poll<Option<Bytes>, hyper::Error> { reader.read_line() });

            let result = lines
                .for_each(move |buf| {
                    run(&buf, processors.clone());
                    ok(())
                }).and_then(|_| ok(response));

            return Box::new(result);
        }
        _ => {
            *response.status_mut() = StatusCode::NOT_FOUND;
        }
    };

    Box::new(future::ok(response))
}

fn build_processors() -> HashMap<String, MetricProcessor> {
    let mut map = HashMap::new();
    let p = MetricProcessor::new(vec![String::from("product_id")]);
    map.insert(String::from("product_lookup"), p);
    map
}

fn main() {
    let result = settings::load("configs/config.toml");

    match result {
        Ok(settings) => {
            for (key, value) in settings.measurements.unwrap() {
                println!("Measurement {} goes to {}/{}", key, value.server, value.db);
            }
        }
        Err(err) => {
            error!("Config load error {}", err);
        }
    }

    let processors = Arc::new(build_processors());

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
