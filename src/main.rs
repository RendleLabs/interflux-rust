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
    HttpRequest, HttpResponse, Responder,
};

use actix_web::dev::PayloadBuffer;
use actix_web::error::PayloadError;

use bytes::{Bytes, BytesMut};

use futures::stream::poll_fn;
use futures::{Async, Future, Poll, Stream};

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

type BoxFut = Box<Future<Item = HttpResponse, Error = Error>>;

fn intercept(req: &HttpRequest) -> BoxFut {
    let mut payload_buffer = PayloadBuffer::new(req.payload() );
    poll_fn(move || -> Poll<Option<Bytes>, PayloadError> { payload_buffer.readline() })
        .from_err()
        .fold(
            0,
            move |counter, buf| {
                let metric = match parse_metric(&buf) {
                    Some((_, metric)) => Some(metric),
                    None => None,
                };
                match metric {
                    Some(m) => {
                        match String::from_utf8(m.measurement.to_vec()) {
                            Ok(s) => {
                                println!("{}", s);
                                Ok(counter + 1)
                            },
                            Err(_) => Err(error::ErrorInternalServerError("Fuck"))
                        }
                    },
                    None => Ok(counter)
                }
            }
        ).and_then(|_| Ok(HttpResponse::Ok().finish()))
        .responder()
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
