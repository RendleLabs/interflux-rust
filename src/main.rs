extern crate config;
extern crate serde;

#[macro_use]
extern crate serde_derive;

#[macro_use]
extern crate log;

extern crate nom;

extern crate hyper;
extern crate futures;

use std::env;

use hyper::{Body, Request, Response, Server, Method, StatusCode};
use hyper::rt::{Future, Stream};
use hyper::service::service_fn;

use futures::future;

mod settings;
mod parser;

use parser::{parse_metric, Metric};

type BoxFut = Box<Future<Item=Response<Body>, Error=hyper::Error> + Send>;

fn intercept(req: Request<Body>) -> BoxFut {
    let mut v: Vec<u8> = Vec::with_capacity(1024);

    req.into_body()
        .map(move |chunk| {
            let mut metrics: Vec<Metric> = Vec::new();

            v.copy_from_slice(chunk.as_bytes());
            loop {
                match parse_metric(v.as_slice()) {
                    Some((remaining, metric)) => {
                        metrics.push(metric);
                        v.clear();
                        v.copy_from_slice(remaining);
                    }
                    None => {
                        break;
                    }
                }
            }
            metrics;
        })
        .flatten()
        .for_each(move |metric| {
            println!("Measurement: {}", metric.measurement);
    });
}

fn main() {
    let result = settings::load(env::args().nth(1).unwrap());

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
}

