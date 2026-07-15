use std::{path::PathBuf, sync::Arc};

use eframe::egui::{
    self, Color32, FontData, FontDefinitions, FontFamily, FontId, RichText, Stroke, Theme,
    style::{Selection, Spacing, WidgetVisuals, Widgets},
};
use iconflow::{Pack, Size, Style, fonts, try_icon};

pub const BG: Color32 = Color32::from_rgb(11, 14, 17);
pub const SIDEBAR: Color32 = Color32::from_rgb(15, 17, 21);
pub const SURFACE: Color32 = Color32::from_rgb(24, 26, 32);
pub const SURFACE_2: Color32 = Color32::from_rgb(30, 35, 41);
pub const SURFACE_3: Color32 = Color32::from_rgb(43, 49, 57);
pub const BORDER: Color32 = Color32::from_rgb(43, 49, 57);
pub const TEXT: Color32 = Color32::from_rgb(234, 236, 239);
pub const MUTED: Color32 = Color32::from_rgb(132, 142, 156);
pub const YELLOW: Color32 = Color32::from_rgb(240, 185, 11);
pub const GREEN: Color32 = Color32::from_rgb(14, 203, 129);
pub const RED: Color32 = Color32::from_rgb(246, 70, 93);

pub fn configure(ctx: &egui::Context) {
    install_fonts(ctx);
    ctx.set_theme(Theme::Dark);
    let mut visuals = egui::Visuals::dark();
    visuals.panel_fill = BG;
    visuals.window_fill = SURFACE;
    visuals.extreme_bg_color = BG;
    visuals.faint_bg_color = SURFACE_2;
    visuals.code_bg_color = Color32::from_rgb(14, 17, 21);
    visuals.override_text_color = Some(TEXT);
    visuals.hyperlink_color = YELLOW;
    visuals.selection = Selection {
        bg_fill: YELLOW,
        stroke: Stroke::new(1.0, Color32::BLACK),
    };
    visuals.widgets = Widgets {
        noninteractive: widget(SURFACE, BORDER, MUTED),
        inactive: widget(SURFACE, BORDER, TEXT),
        hovered: widget(SURFACE_2, YELLOW, TEXT),
        active: widget(SURFACE_3, YELLOW, TEXT),
        open: widget(SURFACE_2, YELLOW, TEXT),
    };
    visuals.window_corner_radius = 6.into();
    visuals.menu_corner_radius = 4.into();
    ctx.set_visuals_of(Theme::Dark, visuals);

    let mut style = (*ctx.style_of(Theme::Dark)).clone();
    style.spacing = Spacing {
        item_spacing: egui::vec2(10.0, 10.0),
        button_padding: egui::vec2(12.0, 7.0),
        interact_size: egui::vec2(40.0, 36.0),
        ..Default::default()
    };
    style.text_styles.insert(
        egui::TextStyle::Heading,
        FontId::new(22.0, FontFamily::Proportional),
    );
    style.text_styles.insert(
        egui::TextStyle::Body,
        FontId::new(14.0, FontFamily::Proportional),
    );
    style.text_styles.insert(
        egui::TextStyle::Button,
        FontId::new(14.0, FontFamily::Proportional),
    );
    style.text_styles.insert(
        egui::TextStyle::Small,
        FontId::new(12.0, FontFamily::Proportional),
    );
    ctx.set_style_of(Theme::Dark, style);
}

pub fn icon(name: &str, size: f32, color: Color32) -> RichText {
    let icon = try_icon(Pack::Lucide, name, Style::Regular, Size::Regular).unwrap_or_else(|_| {
        try_icon(Pack::Lucide, "circle", Style::Regular, Size::Regular).expect("Lucide circle icon")
    });
    let glyph = char::from_u32(icon.codepoint).unwrap_or('?');
    RichText::new(glyph.to_string())
        .font(FontId::new(size, FontFamily::Name(icon.family.into())))
        .color(color)
}

pub fn primary_button(text: impl Into<String>) -> egui::Button<'static> {
    egui::Button::new(
        RichText::new(text.into())
            .color(Color32::from_rgb(20, 20, 20))
            .strong(),
    )
    .fill(YELLOW)
    .stroke(Stroke::NONE)
    .corner_radius(4)
}

pub fn secondary_button(text: impl Into<String>) -> egui::Button<'static> {
    egui::Button::new(RichText::new(text.into()).color(TEXT))
        .fill(SURFACE_2)
        .stroke(Stroke::new(1.0, BORDER))
        .corner_radius(4)
}

fn widget(fill: Color32, border: Color32, foreground: Color32) -> WidgetVisuals {
    WidgetVisuals {
        bg_fill: fill,
        weak_bg_fill: fill,
        bg_stroke: Stroke::new(1.0, border),
        fg_stroke: Stroke::new(1.0, foreground),
        corner_radius: 4.into(),
        expansion: 0.0,
    }
}

fn install_fonts(ctx: &egui::Context) {
    let mut definitions = FontDefinitions::default();
    if let Some(path) = chinese_font_path()
        && let Ok(bytes) = std::fs::read(path)
    {
        let name = "gqt-chinese".to_string();
        definitions
            .font_data
            .insert(name.clone(), Arc::new(FontData::from_owned(bytes)));
        definitions
            .families
            .entry(FontFamily::Proportional)
            .or_default()
            .push(name.clone());
        definitions
            .families
            .entry(FontFamily::Monospace)
            .or_default()
            .push(name);
    }
    let fallback_fonts: Vec<String> = definitions.font_data.keys().cloned().collect();
    for font in fonts() {
        definitions.font_data.insert(
            font.family.to_string(),
            Arc::new(FontData::from_static(font.bytes)),
        );
        let family = definitions
            .families
            .entry(FontFamily::Name(font.family.into()))
            .or_default();
        family.insert(0, font.family.to_string());
        for fallback in &fallback_fonts {
            if fallback != font.family {
                family.push(fallback.clone());
            }
        }
    }
    ctx.set_fonts(definitions);
}

fn chinese_font_path() -> Option<PathBuf> {
    let windows = std::env::var_os("WINDIR")?;
    ["msyh.ttc", "msyh.ttf", "simhei.ttf"]
        .iter()
        .map(|name| PathBuf::from(&windows).join("Fonts").join(name))
        .find(|path| path.exists())
}
