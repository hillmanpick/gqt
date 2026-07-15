#![cfg_attr(not(debug_assertions), windows_subsystem = "windows")]

mod ai;
mod app;
mod exchange;
mod market;
mod model;
mod store;
mod theme;
mod trading;

use eframe::egui;
use std::sync::Arc;

fn main() -> eframe::Result {
    let options = eframe::NativeOptions {
        renderer: eframe::Renderer::Glow,
        viewport: egui::ViewportBuilder::default()
            .with_title("GQT Trader")
            .with_icon(app_icon())
            .with_inner_size([1440.0, 900.0])
            .with_min_inner_size([1080.0, 700.0]),
        ..Default::default()
    };
    eframe::run_native(
        "GQT Trader",
        options,
        Box::new(|creation_context| Ok(Box::new(app::GqtApp::new(creation_context)))),
    )
}

fn app_icon() -> Arc<egui::IconData> {
    Arc::new(egui::IconData {
        rgba: include_bytes!(concat!(env!("OUT_DIR"), "/gqt_icon.rgba")).to_vec(),
        width: 64,
        height: 64,
    })
}
