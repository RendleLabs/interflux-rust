use log::error;
use serde_derive::{Deserialize, Serialize};

use std::borrow::Borrow;
use std::collections::HashMap;
use std::env;
use std::str;
use std::sync::Arc;

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
mod processors;

use crate::parser::get_measurement_name;
use crate::settings::*;
use crate::processors::MetricProcessor;

#[derive(Debug, Serialize, Deserialize)]
struct MyObj {
    name: String,
    number: i32,
}

const MIN_SIZE: usize = 1024 * 1024;
// max payload size is 5MiB
const MAX_SIZE: usize = 5 * 1024 * 1024; // max payload size is 5MiB

type BoxFut = Box<Future<Item = HttpResponse, Error = Error>>;

struct AppState {
    actors: Arc<HashMap<String, MetricProcessor>>,
}

fn run(buf: &[u8], actors: Arc<HashMap<String, MetricProcessor>>) -> usize {
    let name = get_measurement_name(buf);
    match name {
        Some((r, n)) => {
            match actors.get(n) {
                Some(a) => {
                    a.process(n, r)
                },
                None => 0,
            }
        }
        None => 0,
    }
}

fn intercept(req: &HttpRequest<AppState>) -> BoxFut {
    let mut payload_buffer = PayloadBuffer::new(req.payload());
    let actors = req.state().actors.clone();
    poll_fn(move || -> Poll<Option<Bytes>, PayloadError> { payload_buffer.readline() })
        .from_err()
        .fold(0, move |counter, buf| {
            let n = run(&buf, actors.clone());
            if n > 0 {
                Ok(counter + 1)
            } else {
                Err(error::ErrorBadRequest("Oops"))
            }
        }).and_then(|_| Ok(HttpResponse::Ok().finish()))
        .responder()
}

fn build_actors() -> HashMap<String, MetricProcessor> {
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

    let sys = actix::System::new("interflux");

    let actors = Arc::new(build_actors());

    server::new(move || {
        App::with_state(AppState {
            actors: actors.clone(),
        }).resource("/write", |r| r.method(http::Method::POST).f(intercept))
    }).bind("127.0.0.1:8080")
    .unwrap()
    .shutdown_timeout(1)
    .start();

    println!("Started http server: 127.0.0.1:8080");
    let _ = sys.run();
}
