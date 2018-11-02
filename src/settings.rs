use std::collections::HashMap;
use config::{ConfigError, Config, File, Environment};

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

pub fn load(path: String) -> Result<Settings, ConfigError> {

    let mut s = Config::new();

    s.merge(File::with_name(path.as_str()))?;

    s.merge(Environment::with_prefix("interflux"))?;

    s.try_into()
}