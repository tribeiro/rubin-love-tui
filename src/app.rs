use std::{env, error::Error, io, sync::mpsc, thread, time::Duration};

use crate::queue_data::love_payload::{LovePayload, Script, ScriptStream, StateStream};
use crossterm::event::{self, Event, KeyCode, KeyEvent, KeyEventKind};
use ratatui::{
    buffer::Buffer,
    layout::{Constraint, Direction, Layout, Rect},
    style::Stylize,
    symbols::border,
    text::{Line, Text},
    widgets::{Block, Paragraph, Widget},
    DefaultTerminal, Frame,
};
use tungstenite::{connect, Message};

#[derive(Default, Debug, PartialEq)]
enum Panel {
    #[default]
    None,
    State,
    CurrentScripts,
    AvailableScripts,
    WaitingScripts,
    DoneScripts,
}

#[derive(Default, Debug, PartialEq)]
enum SelectedScriptType {
    #[default]
    None,
    Standard(String),
    External(String),
}

#[derive(Default, Debug)]
pub struct App {
    counter: u8,
    available_scripts_scroll: u16,
    available_scripts_scroll_max: u16,
    availabale_script_cursor_position: u16,
    selected_panel: Panel,
    exit: bool,
    updated: bool,
    script_stream: Option<ScriptStream>,
    state_stream: Option<StateStream>,
    standard_scripts: Option<Vec<Script>>,
    external_scripts: Option<Vec<Script>>,
    selected_script_type: SelectedScriptType,
    visible_scripts_text: Vec<String>,
}

impl App {
    pub fn run(&mut self, terminal: &mut DefaultTerminal) -> Result<(), Box<dyn Error>> {
        let (tx, rx) = mpsc::channel();

        let url = env::var("LSST_LOVE_MANAGER_URL")?;
        let password = env::var("LSST_LOVE_MANAGER_PASSWORD")?;

        thread::spawn(move || {
            let client_url =
                format!("wss://{url}/love/manager/ws/subscription/?password={password}");

            let message_subscribe_state_stream = Message::text(
                r#"{"option": "subscribe", "category": "event", "csc": "ScriptQueueState", "salindex": 3, "stream": "stateStream"}"#,
            );
            let message_subscribe_scripts_stream = Message::text(
                r#"{"option": "subscribe", "category": "event", "csc": "ScriptQueueState", "salindex": 3, "stream": "scriptsStream"}"#,
            );
            let message_subscribe_available_scripts_stream = Message::text(
                r#"{"option": "subscribe", "category": "event", "csc": "ScriptQueueState", "salindex": 3, "stream": "availableScriptsStream"}"#,
            );
            if let Ok((mut socket, _)) = connect(client_url) {
                let _ = socket.send(message_subscribe_state_stream);
                let _ = socket.send(message_subscribe_scripts_stream);
                let _ = socket.send(message_subscribe_available_scripts_stream);
                loop {
                    match socket.read() {
                        Ok(msg) => {
                            if let Ok(love_payload) =
                                serde_json::from_str::<LovePayload>(&msg.to_string())
                            {
                                let _ = tx.send(love_payload);
                            };
                        }
                        Err(_) => break,
                    };
                }
            };
        });

        while !self.exit {
            match rx.try_recv() {
                Ok(love_payload) => {
                    if let Some(script_stream) = love_payload.get_script_stream() {
                        self.script_stream = Some(script_stream);
                        self.updated = true;
                    }
                    if let Some(state_stream) = love_payload.get_state_stream() {
                        self.state_stream = Some(state_stream);
                        self.updated = true;
                    }
                    if let Some(available_scripts) = love_payload.get_available_scripts() {
                        let scripts = available_scripts.get_standard_scripts();
                        self.standard_scripts = Some(scripts);
                        let scripts = available_scripts.get_external_scripts();
                        self.external_scripts = Some(scripts);
                        self.available_scripts_scroll_max = available_scripts.number_of_scripts();
                        let visible_scripts = self.get_visible_scripts();
                        self.visible_scripts_text = visible_scripts;
                    }
                    self.counter = 0;
                }
                Err(error) => match error {
                    mpsc::TryRecvError::Empty => self.increment_counter(),
                    mpsc::TryRecvError::Disconnected => {
                        return Err(Box::new(io::Error::new(
                            io::ErrorKind::Other,
                            "Error communicating with the backend.".to_string(),
                        )))
                    }
                },
            }
            terminal.draw(|frame| self.draw(frame))?;
            self.handle_events()?;
        }
        Ok(())
    }

    fn draw(&self, frame: &mut Frame) {
        let outer_layout = Layout::default()
            .direction(Direction::Vertical)
            .constraints(vec![Constraint::Percentage(50), Constraint::Percentage(50)])
            .split(frame.area());

        let top_banner = Layout::default()
            .direction(Direction::Horizontal)
            .constraints(vec![Constraint::Percentage(25), Constraint::Percentage(75)])
            .split(outer_layout[0]);

        let lower_banner = Layout::default()
            .direction(Direction::Horizontal)
            .constraints(vec![
                Constraint::Percentage(34),
                Constraint::Percentage(33),
                Constraint::Percentage(33),
            ])
            .split(outer_layout[1]);

        frame.render_widget(self.get_state(), top_banner[0]);
        frame.render_widget(self.get_running_scripts(), top_banner[1]);
        frame.render_widget(self.get_available_scripts(), lower_banner[0]);
        frame.render_widget(self.get_waiting_scripts(), lower_banner[1]);
        frame.render_widget(self.get_done_scripts(), lower_banner[2]);
    }

    fn handle_events(&mut self) -> io::Result<()> {
        if event::poll(Duration::from_millis(100))? {
            match event::read()? {
                Event::Key(key_event) if key_event.kind == KeyEventKind::Press => {
                    self.handle_key_event(key_event)
                }
                _ => {}
            };
        }
        Ok(())
    }

    fn handle_key_event(&mut self, key_event: KeyEvent) {
        match key_event.code {
            KeyCode::Char('q') => self.exit(),
            KeyCode::Char('a') => {
                if self.selected_panel == Panel::None {
                    self.selected_panel = Panel::AvailableScripts
                }
            }
            KeyCode::Char('s') => {
                if self.selected_panel == Panel::None {
                    self.selected_panel = Panel::State
                }
            }
            KeyCode::Char('c') => {
                if self.selected_panel == Panel::None {
                    self.selected_panel = Panel::CurrentScripts
                }
            }
            KeyCode::Char('w') => {
                if self.selected_panel == Panel::None {
                    self.selected_panel = Panel::WaitingScripts
                }
            }
            KeyCode::Char('d') => {
                if self.selected_panel == Panel::None {
                    self.selected_panel = Panel::DoneScripts
                }
            }
            KeyCode::Char('k') => self.handle_up(),
            KeyCode::Char('j') => self.handle_down(),
            KeyCode::Esc => self.selected_panel = Panel::None,
            KeyCode::Left => self.decrement_counter(),
            KeyCode::Right => self.increment_counter(),
            KeyCode::Up => self.handle_up(),
            KeyCode::Down => self.handle_down(),
            KeyCode::Enter => self.handle_enter(),
            _ => {}
        }
    }

    fn decrement_counter(&mut self) {
        if self.counter == 0 {
            self.counter = u8::MAX;
        } else {
            self.counter -= 1;
        }
    }

    fn increment_counter(&mut self) {
        if self.counter == u8::MAX {
            self.counter = 0;
        } else {
            self.counter += 1;
        }
    }

    fn handle_down(&mut self) {
        match self.selected_panel {
            Panel::AvailableScripts => {
                if self.available_scripts_scroll >= self.available_scripts_scroll_max {
                    self.available_scripts_scroll = 0;
                    self.availabale_script_cursor_position = 0;
                } else {
                    self.available_scripts_scroll += 1;
                    self.availabale_script_cursor_position += 1;
                }
            }
            _ => {}
        }
    }

    fn handle_up(&mut self) {
        match self.selected_panel {
            Panel::AvailableScripts => {
                if self.available_scripts_scroll != 0 {
                    self.available_scripts_scroll -= 1;
                    self.availabale_script_cursor_position -= 1;
                }
            }
            _ => {}
        }
    }

    fn handle_enter(&mut self) {
        match self.selected_panel {
            Panel::AvailableScripts => {
                match &self.selected_script_type {
                    SelectedScriptType::None => {
                        if self.availabale_script_cursor_position == 0 {
                            self.selected_script_type = SelectedScriptType::Standard("".to_string())
                        } else {
                            self.selected_script_type = SelectedScriptType::External("".to_string())
                        }
                        self.availabale_script_cursor_position = 0;
                        self.available_scripts_scroll = 0;
                    }
                    SelectedScriptType::Standard(current_path) => {
                        let visible_scripts_text = &self.visible_scripts_text
                            [self.availabale_script_cursor_position as usize];

                        if visible_scripts_text.contains(&"<<".to_string()) {
                            if let Some(new_path) = current_path.strip_suffix("/") {
                                if let Some((new_path, _)) = new_path.rsplit_once("/") {
                                    self.selected_script_type =
                                        SelectedScriptType::Standard(format!("{}/", new_path));
                                } else {
                                    self.selected_script_type =
                                        SelectedScriptType::Standard("".to_string());
                                }
                            } else {
                                self.selected_script_type = SelectedScriptType::None;
                            }
                        } else if visible_scripts_text.contains(&">>".to_string()) {
                            self.selected_script_type = SelectedScriptType::Standard(format!(
                                "{}{}/",
                                current_path,
                                visible_scripts_text.replace(" >>", "").replace(" - ", ""),
                            ))
                        }
                        self.availabale_script_cursor_position = 0;
                        self.available_scripts_scroll = 0;
                    }
                    SelectedScriptType::External(_) => {
                        let new_script_selected_path = self.visible_scripts_text
                            [self.availabale_script_cursor_position as usize]
                            .rsplit_once("/")
                            .unwrap_or(("", ""));
                        SelectedScriptType::External(new_script_selected_path.0.to_string());
                    }
                }
                let visible_scripts = self.get_visible_scripts();
                self.visible_scripts_text = visible_scripts;
            }
            _ => {}
        }
    }

    fn exit(&mut self) {
        self.exit = true;
    }

    fn get_state(&self) -> Paragraph {
        let (queue_running_text, enabled_text) = {
            if let Some(state_stream) = &self.state_stream {
                (
                    if state_stream.running {
                        "  RUNNING  ".to_string().black().on_green()
                    } else {
                        "  STOPPED  ".to_string().black().on_yellow()
                    },
                    if state_stream.enabled {
                        "  ENABLED  ".to_string().black().on_green()
                    } else {
                        "  DISABLED ".to_string().black().on_blue()
                    },
                )
            } else {
                (
                    " Unknown ".to_string().black().on_gray(),
                    " Unknown ".to_string().black().on_gray(),
                )
            }
        };
        let state_header = {
            if self.updated {
                "State (updated) \u{1F494} \u{2764}\u{FE0F}:".to_string()
            } else {
                let counter = self.counter;
                format!("State (never updated) [{counter}]:")
            }
        };
        let state_text = Text::from(vec![
            Line::from(vec!["SummaryState: ".into(), enabled_text]),
            Line::from(vec!["Queue State:  ".into(), queue_running_text]),
            Line::from(vec![
                format!("Selected Panel:  {:?}", self.selected_panel).into()
            ]),
        ]);
        Paragraph::new(state_text).block(Block::bordered().title(state_header))
    }

    fn get_available_scripts(&self) -> Paragraph {
        let available_scripts: Vec<Line> = {
            let cursor_position = self.availabale_script_cursor_position as usize;
            let visible_scripts: Vec<Line> = self
                .visible_scripts_text
                .iter()
                .enumerate()
                .map(|(i, script)| {
                    if self.selected_panel == Panel::AvailableScripts && i == cursor_position {
                        Line::from(script.clone()).on_gray()
                    } else {
                        script.clone().into()
                    }
                })
                .collect();
            visible_scripts
        };
        // let standard_scripts = {
        //     if let Some(scripts) = &self.standard_scripts {
        //         scripts
        //             .iter()
        //             .map(|script| Line::from(vec![format!("  - {}", script.path).into()]))
        //             .collect()
        //     } else {
        //         vec![]
        //     }
        // };
        // let external_scripts = {
        //     if let Some(scripts) = &self.external_scripts {
        //         scripts
        //             .iter()
        //             .map(|script| Line::from(vec![format!("  - {}", script.path).into()]))
        //             .collect()
        //     } else {
        //         vec![]
        //     }
        // };
        // let available_scripts: Vec<Line> = {
        //     let mut available_scripts: Vec<Line> = vec![Line::from(vec![self
        //         .available_scripts_selected_path
        //         .into()])]
        //     .into_iter()
        //     .chain(visible_scripts)
        //     .collect();
        //     if self.selected_panel == Panel::AvailableScripts {
        //         available_scripts[self.availabale_script_cursor_position as usize] =
        //             available_scripts[self.availabale_script_cursor_position as usize]
        //                 .clone()
        //                 .on_gray();
        //     }
        //     available_scripts
        // };

        let available_scripts_text = Text::from(available_scripts);
        let title = format!("Available Scripts: {:?}", self.selected_script_type);
        Paragraph::new(available_scripts_text)
            .scroll((self.available_scripts_scroll, 0))
            .block(Block::bordered().title(title))
    }

    fn get_visible_scripts(&self) -> Vec<String> {
        match &self.selected_script_type {
            SelectedScriptType::None => {
                return vec![
                    "Standard Scripts >>".to_string(),
                    "External Scripts >>".to_string(),
                ]
            }
            SelectedScriptType::Standard(path) => {
                let mut standard_scripts: Vec<String> = {
                    if let Some(standard_scripts) = &self.standard_scripts {
                        standard_scripts
                            .iter()
                            .filter_map(|script| {
                                if script.path.starts_with(path) {
                                    if let Some((_, path)) = script.path.split_once(path) {
                                        if let Some((path, _)) = path.split_once("/") {
                                            Some(format!(" - {} >>", path))
                                        } else {
                                            Some(format!(" - {}", path))
                                        }
                                    } else {
                                        None
                                    }
                                } else {
                                    None
                                }
                            })
                            .collect()
                    } else {
                        vec![]
                    }
                };
                standard_scripts.dedup();
                vec![" << ".to_string()]
                    .into_iter()
                    .chain(standard_scripts)
                    .collect()
            }
            SelectedScriptType::External(path) => {
                let mut external_scripts: Vec<String> = {
                    if let Some(external_scripts) = &self.external_scripts {
                        external_scripts
                            .iter()
                            .filter_map(|script| {
                                if script.path.starts_with(path) {
                                    if let Some((_, path)) = script.path.split_once(path) {
                                        if let Some((path, _)) = path.split_once("/") {
                                            Some(format!(" - {} >>", path))
                                        } else {
                                            Some(format!(" - {}", path))
                                        }
                                    } else {
                                        None
                                    }
                                } else {
                                    None
                                }
                            })
                            .collect()
                    } else {
                        vec![]
                    }
                };
                external_scripts.dedup();
                vec![" <<  ".to_string()]
                    .into_iter()
                    .chain(external_scripts)
                    .collect()
            }
        }
    }

    fn get_waiting_scripts(&self) -> Paragraph {
        let waiting_scripts = {
            if let Some(script_stream) = &self.script_stream {
                script_stream
                    .waiting_scripts
                    .iter()
                    .map(|script_info| {
                        Line::from(vec![format!(
                            " [{}-{}] {}",
                            script_info.index, script_info.process_state, script_info.path
                        )
                        .into()])
                    })
                    .collect()
            } else {
                vec![]
            }
        };

        Paragraph::new(waiting_scripts)
            .scroll((0 as u16, 0))
            .block(Block::bordered().title("Waiting Scripts"))
    }

    fn get_done_scripts(&self) -> Paragraph {
        let finished_scripts = {
            if let Some(script_stream) = &self.script_stream {
                script_stream
                    .finished_scripts
                    .iter()
                    .map(|finished_script_info| {
                        Line::from(vec![format!(
                            " [{}-{}-{}]:{}",
                            finished_script_info.index,
                            finished_script_info.process_state,
                            finished_script_info.script_state,
                            finished_script_info.path
                        )
                        .into()])
                    })
                    .collect()
            } else {
                vec![]
            }
        };
        Paragraph::new(finished_scripts)
            .scroll((0 as u16, 0))
            .block(Block::bordered().title("Done Scripts"))
    }

    fn get_running_scripts(&self) -> Paragraph {
        let running_scripts = {
            if let Some(script_stream) = &self.script_stream {
                script_stream
                    .current_scripts
                    .iter()
                    .map(|script_info| {
                        vec![
                            Line::from(vec![format!(
                                " [{}-{}] {}",
                                script_info.index, script_info.process_state, script_info.path
                            )
                            .into()]),
                            Line::from(vec![format!(
                                " Expected duration {}s; Elapsed: {:.1}s",
                                script_info.expected_duration,
                                script_info.get_elapsed_time(),
                            )
                            .into()]),
                        ]
                    })
                    .flatten()
                    .collect()
            } else {
                vec![]
            }
        };
        Paragraph::new(running_scripts)
            .scroll((0 as u16, 0))
            .block(Block::bordered().title("Current Scripts"))
    }
}

impl Widget for &App {
    fn render(self, area: Rect, buf: &mut Buffer)
    where
        Self: Sized,
    {
        let title = Line::from(" RubinOCS ScriptQueue ".bold());
        let instructions = Line::from(vec![
            " Decrement ".into(),
            "<Left>".blue().bold(),
            " Increment ".into(),
            "<Right>".blue().bold(),
            " Quit".into(),
            "<Q>".blue().bold(),
        ]);
        let block = Block::bordered()
            .title(title.centered())
            .title_bottom(instructions.centered())
            .border_set(border::THICK);
        let counter_text = Text::from(vec![Line::from(vec![
            "Value: ".into(),
            self.counter.to_string().yellow(),
        ])]);
        Paragraph::new(counter_text)
            .centered()
            .block(block)
            .render(area, buf);
    }
}
