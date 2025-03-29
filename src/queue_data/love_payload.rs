use chrono::Utc;

#[derive(Debug, Deserialize, Serialize, Default)]
pub struct LovePayload {
    category: String,
    data: Vec<LoveData>,
    subscription: String,
    tracing: LoveTracing,
}

#[derive(Debug, Deserialize, Serialize, Default)]
pub struct LoveData {
    csc: String,
    salindex: i32,
    data: LoveDataType,
}

#[derive(Debug, Deserialize, Serialize, Default)]
pub struct LoveDataType {
    #[serde(rename = "scriptsStream")]
    script_stream: Option<ScriptStream>,
    #[serde(rename = "stateStream")]
    state_stream: Option<StateStream>,
    #[serde(rename = "availableScriptsStream")]
    available_scripts_stream: Option<AvailableScripts>,
}

#[derive(Debug, Deserialize, Serialize, Default, Clone)]
pub struct ScriptStream {
    pub current_scripts: Vec<ScriptInfo>,
    pub waiting_scripts: Vec<ScriptInfo>,
    pub finished_scripts: Vec<ScriptInfo>,
}

#[derive(Debug, Deserialize, Serialize, Default, Clone)]
pub struct ScriptInfo {
    remote: Option<String>,
    setup: bool,
    pub index: u32,
    pub path: String,
    #[serde(rename = "type")]
    script_type: String,
    pub process_state: String,
    pub script_state: String,
    #[serde(rename = "timestampConfigureEnd")]
    timestamp_configure_end: f64,
    #[serde(rename = "timestampConfigureStart")]
    timestamp_configure_start: f64,
    #[serde(rename = "timestampProcessEnd")]
    timestamp_process_end: f64,
    #[serde(rename = "timestampProcessStart")]
    timestamp_process_start: f64,
    #[serde(rename = "timestampRunStart")]
    timestamp_run_start: f64,
    pub expected_duration: f64,
    last_checkpoint: String,
    description: String,
    classname: String,
    remotes: String,
    last_heartbeat_timestamp: f64,
    lost_heartbeats: u32,
    pause_checkpoints: String,
    stop_checkpoints: String,
    log_level: u8,
}

#[derive(Debug, Deserialize, Serialize, Default, Clone, Copy)]
pub struct StateStream {
    pub enabled: bool,
    pub running: bool,
}

#[derive(Debug, Deserialize, Serialize, Default, Clone)]
pub struct AvailableScripts {
    available_scripts: Vec<Script>,
}

#[derive(Debug, Deserialize, Serialize, Default, Clone)]
pub struct Script {
    #[serde(rename = "type")]
    script_type: String,
    pub path: String,
    #[serde(rename = "configSchema")]
    config_schema: String,
}

#[derive(Debug, Deserialize, Serialize, Default)]
struct LoveTracing {
    producer_snd: f64,
    manager_rcv_from_producer: f64,
    manager_snd_to_group: f64,
    manager_rcv_from_group: f64,
    manager_snd_to_client: f64,
}

impl LovePayload {
    pub fn get_script_stream(&self) -> Option<ScriptStream> {
        if let Some(data) = self.data.get(0) {
            if let Some(script_stream) = &data.data.script_stream {
                Some(script_stream.clone())
            } else {
                None
            }
        } else {
            None
        }
    }
    pub fn get_state_stream(&self) -> Option<StateStream> {
        if let Some(data) = self.data.get(0) {
            if let Some(state_stream) = &data.data.state_stream {
                Some(*state_stream)
            } else {
                None
            }
        } else {
            None
        }
    }
    pub fn get_available_scripts(&self) -> Option<AvailableScripts> {
        if let Some(data) = self.data.get(0) {
            if let Some(available_scripts) = &data.data.available_scripts_stream {
                Some(available_scripts.clone())
            } else {
                None
            }
        } else {
            None
        }
    }
}

impl ScriptInfo {
    pub fn get_elapsed_time(&self) -> f64 {
        if self.timestamp_process_end == 0.0 {
            Utc::now().timestamp_millis() as f64 * 1e-3 + 37.0 - self.timestamp_run_start
        } else {
            self.timestamp_process_end - self.timestamp_run_start
        }
    }
}

impl AvailableScripts {
    pub fn get_standard_scripts(&self) -> Vec<Script> {
        self.available_scripts
            .iter()
            .filter_map(|available_script| {
                if available_script.script_type == "standard" {
                    Some(available_script.clone())
                } else {
                    None
                }
            })
            .collect()
    }
    pub fn get_external_scripts(&self) -> Vec<Script> {
        self.available_scripts
            .iter()
            .filter_map(|available_script| {
                if available_script.script_type == "external" {
                    Some(available_script.clone())
                } else {
                    None
                }
            })
            .collect()
    }
    pub fn number_of_scripts(&self) -> u16 {
        self.available_scripts.len() as u16
    }
}
