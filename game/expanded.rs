#![feature(prelude_import)]
extern crate std;
#[prelude_import]
use std::prelude::rust_2024::*;
mod ui {
    use bevy::prelude::*;
    struct UiRoot {}
}
use bevy::{
    feathers::controls::FeathersButton, prelude::*, scene::{CommandsSceneExt, bsn},
};
fn main() {
    App::new().add_plugins((DefaultPlugins,)).add_systems(Startup, setup_ui).run();
}
fn setup_ui(mut commands: Commands) {
    commands.spawn(Camera3d::default());
    commands
        .spawn_scene(
            ::bevy::scene::SceneScope({
                let _expr0 = {
                    ::bevy::scene::SceneScope({
                        let _res = ::bevy::scene::SceneFunction(move |_context, _scene| {
                            let value = _scene
                                .get_or_insert_template::<
                                    <Text as ::bevy::ecs::template::FromTemplate>::Template,
                                >(_context);
                            value.0 = "wa".into();
                        });
                        _res
                    })
                }
                    .into();
                let _res = {
                    let mut props = <<FeathersButton as ::bevy::scene::SceneComponent>::Props as ::core::default::Default>::default();
                    let props_ref = &mut props;
                    ::bevy::scene::macro_utils::touch_type::<FeathersButton>();
                    props.caption = _expr0;
                    (
                        <FeathersButton as ::bevy::scene::SceneComponent>::scene(props),
                        <FeathersButton as ::bevy::scene::PatchFromTemplate>::patch(move |
                            value,
                            _context|
                        {}),
                    )
                };
                _res
            }),
        );
}
