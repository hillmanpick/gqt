use std::{env, error::Error, fs, path::PathBuf};

use ico::{IconDir, IconDirEntry, IconImage, ResourceType};

fn main() -> Result<(), Box<dyn Error>> {
    println!("cargo:rerun-if-changed=build.rs");
    println!("cargo:rerun-if-changed=assets/gqt-mark.svg");

    let out_dir = PathBuf::from(env::var("OUT_DIR")?);
    fs::write(out_dir.join("gqt_icon.rgba"), icon_pixels(64))?;

    let icon_path = out_dir.join("gqt.ico");
    let mut icon = IconDir::new(ResourceType::Icon);
    for size in [16, 32, 48, 64, 256] {
        let image = IconImage::from_rgba_data(size, size, icon_pixels(size));
        icon.add_entry(IconDirEntry::encode(&image)?);
    }
    icon.write(fs::File::create(&icon_path)?)?;

    if env::var("CARGO_CFG_TARGET_OS").as_deref() == Ok("windows") {
        let mut resource = winresource::WindowsResource::new();
        resource
            .set_icon(icon_path.to_str().ok_or("invalid icon path")?)
            .set("ProductName", "GQT Trader")
            .set("FileDescription", "GQT Native Futures Workstation")
            .set("LegalCopyright", "HillmanPick");
        resource.compile()?;
    }
    Ok(())
}

fn icon_pixels(size: u32) -> Vec<u8> {
    let mut pixels = Vec::with_capacity((size * size * 4) as usize);
    for y in 0..size {
        for x in 0..size {
            let unit_x = x as f32 * 64.0 / size as f32;
            let unit_y = y as f32 * 64.0 / size as f32;
            let rounded_corner = {
                let dx = if unit_x < 10.0 {
                    10.0 - unit_x
                } else if unit_x > 54.0 {
                    unit_x - 54.0
                } else {
                    0.0
                };
                let dy = if unit_y < 10.0 {
                    10.0 - unit_y
                } else if unit_y > 54.0 {
                    unit_y - 54.0
                } else {
                    0.0
                };
                dx * dx + dy * dy <= 100.0
            };
            let g = ((15.0..51.0).contains(&unit_x) && (12.0..20.0).contains(&unit_y))
                || ((15.0..23.0).contains(&unit_x) && (12.0..52.0).contains(&unit_y))
                || ((15.0..51.0).contains(&unit_x) && (44.0..52.0).contains(&unit_y))
                || ((32.0..51.0).contains(&unit_x) && (28.0..36.0).contains(&unit_y))
                || ((43.0..51.0).contains(&unit_x) && (28.0..52.0).contains(&unit_y));
            let tail = unit_y >= 42.0
                && unit_y <= 54.0
                && unit_x >= 39.0
                && unit_x <= 54.0
                && unit_x - unit_y >= -9.0
                && unit_x - unit_y <= 1.0;
            let (red, green, blue, alpha) = if !rounded_corner {
                (0, 0, 0, 0)
            } else if g || tail {
                (240, 185, 11, 255)
            } else {
                (11, 14, 17, 255)
            };
            pixels.extend_from_slice(&[red, green, blue, alpha]);
        }
    }
    pixels
}
