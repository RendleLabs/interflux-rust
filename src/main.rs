extern crate config;
extern crate serde;

#[macro_use]
extern crate serde_derive;

#[macro_use]
extern crate log;

#[macro_use]
extern crate nom;

use std::env;

mod settings;
mod parser;

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

