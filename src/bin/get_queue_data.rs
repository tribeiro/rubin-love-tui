use std::{env, sync::mpsc, thread};

use rubin_love_tui::queue_data::love_payload::LovePayload;
use tungstenite::{connect, Message};

fn main() {
    let (tx, rx) = mpsc::channel();

    let url = env::var("LSST_LOVE_MANAGER_URL").unwrap();
    let password = env::var("LSST_LOVE_MANAGER_PASSWORD").unwrap();

    thread::spawn(move || {
        let client_url = format!("wss://{url}/love/manager/ws/subscription/?password={password}");

        let message_subscribe_state_stream = Message::text(
            r#"{"option": "subscribe", "category": "event", "csc": "ScriptQueueState", "salindex": 1, "stream": "stateStream"}"#,
        );
        let message_subscribe_scripts_stream = Message::text(
            r#"{"option": "subscribe", "category": "event", "csc": "ScriptQueueState", "salindex": 1, "stream": "scriptsStream"}"#,
        );
        let message_subscribe_heartbeat = Message::text(
            r#"{"option": "subscribe", "category": "event", "csc": "ScriptQueueState", "salindex": 1, "stream": "availableScriptsStream"}"#,
        );
        let (mut socket, _) = connect(client_url).expect("Can't connect");
        socket.send(message_subscribe_state_stream).unwrap();
        socket.send(message_subscribe_scripts_stream).unwrap();
        socket.send(message_subscribe_heartbeat).unwrap();
        loop {
            let msg = socket.read().expect("Error reading message");
            if let Ok(love_payload) = serde_json::from_str::<LovePayload>(&msg.to_string()) {
                tx.send(love_payload).unwrap();
            } else {
                println!("Could not parse {msg} into a LovePayload.");
            }
        }
    });

    loop {
        let love_payload = rx.recv().unwrap();
        if let Some(script_stream) = love_payload.get_script_stream() {
            println!("{script_stream:?}.");
        } else if let Some(state_stream) = love_payload.get_state_stream() {
            println!("{state_stream:?}");
        } else if let Some(available_scripts) = love_payload.get_available_scripts() {
            println!("{available_scripts:?}");
        } else {
            println!("Got something else");
        }
    }
}
