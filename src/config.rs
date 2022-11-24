use serde::Deserialize;
use std::fs::read_to_string;
use std::io;

#[derive(Deserialize, Debug)]
pub struct AppConfig {
    #[serde(rename = "Apps")]
    pub apps: Vec<App>,
}

#[derive(Deserialize, Debug)]
pub struct App {
    #[serde(rename = "Name")]
    pub name: String,
    #[serde(rename = "Ports")]
    pub ports: Vec<u16>,
    #[serde(rename = "Targets")]
    pub targets: Vec<String>,
}

#[derive(Debug)]
pub enum AppConfigError {
    JsonDecodeError(serde_json::Error),
    IOError(io::Error),
}

pub fn get_app_config() -> Result<AppConfig, AppConfigError> {
    let file_content = read_to_string("./config.json").map_err(AppConfigError::IOError)?;

    serde_json::from_str(&file_content).map_err(AppConfigError::JsonDecodeError)
}
