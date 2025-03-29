use rubin_love_tui::app::App;
use std::error::Error;

fn main() -> Result<(), Box<dyn Error>> {
    let mut terminal = ratatui::init();
    let app_result = App::default().run(&mut terminal);
    ratatui::restore();
    app_result
}
