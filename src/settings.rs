use config::{Config, ConfigError, Environment, File};
use serde_derive::Deserialize;
use std::collections::HashMap;

#[derive(Debug, Deserialize)]
pub struct Settings {
    pub measurements: Option<HashMap<String, Measurement>>,
}

#[derive(Debug, Deserialize)]
pub struct Measurement {
    pub server: String,
    pub db: String,
    pub rp: Option<String>,
    pub strip_tags: Option<Vec<String>>,
}

pub fn load(path: &str) -> Result<Settings, ConfigError> {
    let mut config = Config::new();

    config.merge(File::with_name(path))?;

    config.merge(Environment::with_prefix("interflux"))?;

    config.try_into()
}
