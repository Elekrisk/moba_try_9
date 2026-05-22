mod ui;

use bevy::{feathers::{FeathersPlugins, controls::FeathersButton, dark_theme::create_dark_theme, theme::UiTheme}, prelude::*, scene::{CommandsSceneExt, bsn}};

fn main() {
    App::new()
    .add_plugins((DefaultPlugins, FeathersPlugins))
    .insert_resource(UiTheme(create_dark_theme()))
    .add_systems(Startup, setup_ui)
    .run();
}

fn setup_ui(mut commands: Commands) {
    commands.spawn(Camera3d::default());

    commands.spawn_scene(bsn!{
        :FeathersButton {
            @caption: {bsn!{Text("wa")}}
        }
    });
}
