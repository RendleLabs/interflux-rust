extern crate actix;
extern crate actix_web;
extern crate bytes;
extern crate config;
extern crate serde;
extern crate serde_json;

#[macro_use]
extern crate serde_derive;

#[macro_use]
extern crate log;

extern crate nom;

extern crate futures;

use std::env;
use std::str;

use actix_web::{
    error, http, middleware, server, App, AsyncResponder, Error, FutureResponse, HttpMessage,
    HttpRequest, HttpResponse,
};

use bytes::BytesMut;

use futures::{Future, Stream};

mod parser;
mod settings;

use parser::{parse_metric, Metric};

#[derive(Debug, Serialize, Deserialize)]
struct MyObj {
    name: String,
    number: i32,
}

const MIN_SIZE: usize = 1024 * 1024; // max payload size is 5MiB
const MAX_SIZE: usize = 5 * 1024 * 1024; // max payload size is 5MiB

fn intercept(req: &HttpRequest) -> Box<Future<Item = HttpResponse, Error = Error>> {
    let x = req.payload().from_err();
    x.fold(BytesMut::with_capacity(MIN_SIZE), move |mut body, chunk| {
        if body.len() + chunk.len() > MAX_SIZE {
            Err(error::ErrorBadRequest("overflow"))
        } else {
            body.extend_from_slice(&chunk);
            Ok(body)
        }
    }).and_then(|body| {
        let mut buf = body.freeze();
        loop {
            match parse_metric(&buf.clone()) {
                Some((remaining, metric)) => {
                    buf = bytes::Bytes::from(remaining);
                    println!("{:?}", metric.measurement);
                }
                None => break,
            }
        }
        Ok(HttpResponse::Ok().finish())
    }).responder()
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

    let sys = actix::System::new("interflux");

    server::new(|| App::new().resource("/write", |r| r.method(http::Method::POST).f(intercept)))
        .bind("127.0.0.1:8080")
        .unwrap()
        .shutdown_timeout(1)
        .start();

    println!("Started http server: 127.0.0.1:8080");
    let _ = sys.run();
}
