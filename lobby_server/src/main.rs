use std::{collections::HashMap, sync::Arc, time::Duration};

use common::{
    ChampionId, ClientToLobby, LobbyId, LobbyInfo, LobbySettings, LobbyState, LobbyToClient,
    PlayerId, PlayerInfo, ReadMsg, ShortLobbyInfo, Team, WriteMsg,
};
use quinn::{
    Connection, Endpoint, Incoming, RecvStream, SendStream, ServerConfig, crypto,
    rustls::pki_types::{CertificateDer, PrivatePkcs8KeyDer},
};
use tokio::{
    select,
    sync::mpsc::{self, UnboundedReceiver, UnboundedSender},
};

#[tokio::main]
async fn main() {
    let endpoint = setup_server();

    let mut state = State::new(endpoint);
    state.run().await;
}

fn setup_server() -> Endpoint {
    let cert = rcgen::generate_simple_self_signed(vec!["localhost".into()]).unwrap();
    let cert_der = CertificateDer::from(cert.cert);
    let priv_key = PrivatePkcs8KeyDer::from(cert.signing_key.serialize_der());

    let server_config =
        ServerConfig::with_single_cert(vec![cert_der.clone()], priv_key.into()).unwrap();

    Endpoint::server(server_config, "0.0.0.0:44699".parse().unwrap()).unwrap()
}

struct Player {
    id: PlayerId,
    username: String,
    in_lobby: Option<LobbyId>,
    message_sender: UnboundedSender<LobbyToClient>,
}

impl Player {
    fn get_info(&self) -> PlayerInfo {
        PlayerInfo {
            id: self.id,
            name: self.username.clone(),
        }
    }
}

struct Lobby {
    id: LobbyId,
    settings: LobbySettings,
    players: Vec<Vec<PlayerInLobby>>,
    leader: PlayerId,
    lobby_state: LobbyState,
}

impl Lobby {
    fn add_player(&mut self, player: PlayerId) {
        let team = self.find_smallest_team();
        self.players[team.0].push(PlayerInLobby {
            id: player,
            ready: false,
            selected_champion: None,
            locked: false,
        });
    }

    fn remove_player(&mut self, player: PlayerId) {
        for team in &mut self.players {
            let Some(index) = team.iter().position(|p| p.id == player) else {
                continue;
            };
            team.remove(index);
            break;
        }
        if self.leader == player
            && let Some(player) = self.players.iter().flatten().map(|p| p.id).next()
        {
            self.leader = player;
        }
    }

    fn find_smallest_team(&self) -> Team {
        Team(
            self.players
                .iter()
                .enumerate()
                .min_by_key(|(_, p)| p.len())
                .unwrap()
                .0,
        )
    }

    fn set_team_count(&mut self, count: usize) {
        if count == 0 {
            panic!();
        }

        if count >= self.players.len() {
            self.players.resize(count, vec![]);
        } else {
            let players = self.players.split_off(count);
            for player in players.into_iter().flatten() {
                let team = self.find_smallest_team();
                self.players[team.0].push(player);
            }
        }
    }

    fn set_players_per_team(&mut self, count: usize) {
        if count == 0 {
            panic!();
        }

        for team_index in 0..self.players.len() {
            while self.players[team_index].len() > count {
                let to_team = self.find_smallest_team();
                if self.players[to_team.0].len() < count {
                    let player = self.players[team_index].pop().unwrap();
                    self.players[to_team.0].push(player);
                } else {
                    return;
                }
            }
        }
    }

    fn update_settings(&mut self, settings: LobbySettings) {
        self.settings = settings;
        self.set_team_count(self.settings.team_count);
        self.set_players_per_team(self.settings.players_per_team);
    }

    fn set_lobby_state(&mut self, new_state: LobbyState) {
        if new_state == LobbyState::Normal {
            for player in self.players.iter_mut().flatten() {
                player.selected_champion = None;
                player.locked = false;
            }
        }
    }

    fn get_info(&self) -> LobbyInfo {
        LobbyInfo {
            id: self.id,
            settings: self.settings.clone(),
            players: self
                .players
                .iter()
                .map(|p| {
                    p.iter()
                        .map(|p| common::PlayerInLobby {
                            id: p.id,
                            selected_champion: p.selected_champion,
                            locked: p.locked,
                        })
                        .collect()
                })
                .collect(),
            leader: self.leader,
            lobby_state: self.lobby_state,
        }
    }

    fn get_short_info(&self) -> ShortLobbyInfo {
        ShortLobbyInfo {
            id: self.id,
            name: self.settings.name.clone(),
            players: self.player_count(),
            max_player_count: self.max_player_count(),
        }
    }

    fn player_count(&self) -> usize {
        self.players.iter().map(Vec::len).sum()
    }

    fn max_player_count(&self) -> usize {
        self.settings.players_per_team * self.settings.team_count
    }

    fn empty_player_slots(&self) -> usize {
        self.player_count().saturating_sub(self.max_player_count())
    }

    fn can_join(&self) -> bool {
        !self.settings.locked
            && self.lobby_state == LobbyState::Normal
            && self.player_count() < self.max_player_count()
    }
}

#[derive(Clone)]
struct PlayerInLobby {
    id: PlayerId,
    ready: bool,
    selected_champion: Option<ChampionId>,
    locked: bool,
}

pub struct State {
    server: Endpoint,
    players: HashMap<PlayerId, Player>,
    player_names: HashMap<String, PlayerId>,
    lobbies: HashMap<LobbyId, Lobby>,
}

impl State {
    pub fn new(server: Endpoint) -> Self {
        Self {
            server,
            players: HashMap::new(),
            player_names: HashMap::new(),
            lobbies: HashMap::new(),
        }
    }
}

pub enum Event {
    NewConnection {
        id: PlayerId,
        requested_username: String,
        message_sender: UnboundedSender<LobbyToClient>,
    },
    LostConnection {
        id: PlayerId,
    },
    Message {
        from: PlayerId,
        message: ClientToLobby,
    },
}

impl State {
    async fn run(&mut self) {
        let (event_sender, mut events) = mpsc::unbounded_channel::<Event>();

        loop {
            let accept = self.server.accept();

            select! {
                incoming = accept => {
                    let Some(incoming) = incoming else {
                        break
                    };

                    tokio::spawn(connection_handler(incoming, event_sender.clone()));
                },
                event = events.recv() => {
                    self.handle_event(event.unwrap()).await;
                }
            }
        }
    }

    async fn handle_event(&mut self, event: Event) {
        match event {
            Event::NewConnection {
                id,
                requested_username,
                message_sender,
            } => {
                let username = self.find_username(&requested_username);
                self.player_names.insert(username.clone(), id);
                _ = message_sender.send(LobbyToClient::Handshake {
                    id,
                    username: username.clone(),
                });
                self.players.insert(
                    id,
                    Player {
                        id,
                        username,
                        in_lobby: None,
                        message_sender,
                    },
                );
            }
            Event::LostConnection { id } => todo!(),
            Event::Message { from, message } => {
                self.handle_message(from, message).await;
            }
        }
    }

    async fn handle_message(&mut self, from: PlayerId, message: ClientToLobby) {
        match message {
            ClientToLobby::Handshake { username: _ } => {
                // Ignore
            }
            ClientToLobby::GetLobbyList => {
                let lobbies = self
                    .lobbies
                    .values()
                    .map(|lobby| lobby.get_short_info())
                    .collect();
                self.send_message(from, LobbyToClient::LobbyList { lobbies });
            }
            ClientToLobby::GetLobbyInfo { id } => {
                let Some(lobby) = self.lobbies.get(&id) else {
                    return;
                };
                let info = lobby.get_info();
                self.send_message(from, LobbyToClient::LobbyInfo { info });
            }
            ClientToLobby::GetPlayerInfo { player } => {
                let Some(player) = self.players.get(&player) else {
                    return;
                };
                self.send_message(
                    from,
                    LobbyToClient::PlayerInfo {
                        info: player.get_info(),
                    },
                );
            }
            ClientToLobby::CreateLobby => {
                let Some(player) = self.players.get(&from) else {
                    return;
                };
                if player.in_lobby.is_some() {
                    return;
                }

                let lobby = Lobby {
                    id: LobbyId::new(),
                    settings: LobbySettings {
                        name: "New Lobby".into(),
                        team_count: 2,
                        players_per_team: 5,
                        locked: false,
                    },
                    players: vec![
                        vec![PlayerInLobby {
                            id: from,
                            ready: false,
                            selected_champion: None,
                            locked: false,
                        }],
                        vec![],
                    ],
                    leader: from,
                    lobby_state: LobbyState::Normal,
                };
                let lobby_id = lobby.id;
                self.lobbies.insert(lobby_id, lobby);
                self.send_message(from, LobbyToClient::JoinedLobby { id: lobby_id });
            }
            ClientToLobby::JoinLobby { id } => {
                let Some(player) = self.players.get(&from) else {
                    return;
                };
                if player.in_lobby.is_some() {
                    return;
                }
                let Some(lobby) = self.lobbies.get_mut(&id) else {
                    return;
                };
                if !lobby.can_join() {
                    return;
                }
            }
            ClientToLobby::LeaveLobby => {
                let Some(player) = self.players.get(&from) else {
                    return;
                };
                let Some(lobby_id) = player.in_lobby else {
                    return;
                };
                let Some(lobby) = self.lobbies.get_mut(&lobby_id) else {
                    return;
                };
                let old_leader = lobby.leader;
                lobby.remove_player(from);
                let new_leader = lobby.leader;
                if lobby.player_count() == 0 {
                    self.lobbies.remove(&lobby_id);
                } else {
                    if lobby.lobby_state == LobbyState::ChampSelect {
                        lobby.set_lobby_state(LobbyState::Normal);
                        self.send_message_to_lobby(
                            lobby_id,
                            LobbyToClient::LobbyStateChanged {
                                new_state: LobbyState::Normal,
                            },
                        );
                    }
                    if old_leader != new_leader {
                        self.send_message_to_lobby(
                            lobby_id,
                            LobbyToClient::PlayerBecameLeader { player: new_leader },
                        );
                    }
                    self.send_message_to_lobby(
                        lobby_id,
                        LobbyToClient::PlayerLeftLobby { player: from },
                    );
                }
            }
            ClientToLobby::KickPlayer { id } => {
                let kicker = self.players.get(&from).unwrap();
                let kickee = self.players.get(&id).unwrap();
            },
            ClientToLobby::SwitchPlayerTeam { id, team } => todo!(),
            ClientToLobby::GetLobbySettings => todo!(),
            ClientToLobby::SetLobbySettings { settings } => todo!(),
            ClientToLobby::SetReady { ready } => todo!(),
            ClientToLobby::ForceLobbyReady => todo!(),
            ClientToLobby::SelectChampion { champion } => todo!(),
            ClientToLobby::SetChampionLocked { locked } => todo!(),
        }
    }

    fn find_username(&self, wanted: &str) -> String {
        if self.player_names.contains_key(wanted) {
            return wanted.to_string();
        }

        let mut bytes = wanted.as_bytes().to_vec();
        if bytes.last().is_none_or(|ch| !ch.is_ascii_digit()) {
            bytes.push(b'1');
        }

        while self
            .player_names
            .contains_key(str::from_utf8(&bytes).unwrap())
        {
            let mut i = bytes.len() - 1;
            loop {
                match bytes[i] {
                    b'0'..=b'8' => {
                        bytes[i] += 1;
                        break;
                    }
                    b'9' => {
                        if i == 0 || !bytes[i - 1].is_ascii_digit() {
                            bytes[i] = b'1';
                            bytes.push(b'0');
                            break;
                        } else {
                            bytes[i] = b'0';
                            i -= 1;
                            continue;
                        }
                    }
                    _ => unreachable!(),
                }
            }
        }

        String::from_utf8(bytes).unwrap()
    }

    fn send_message(&self, to: PlayerId, message: LobbyToClient) {
        if let Some(player) = self.players.get(&to) {
            _ = player.message_sender.send(message);
        }
    }

    fn send_message_to_lobby(&self, to: LobbyId, message: LobbyToClient) {
        let Some(lobby) = self.lobbies.get(&to) else {
            return;
        };
        for player in lobby
            .players
            .iter()
            .flatten()
            .map(|p| p.id)
            .collect::<Vec<_>>()
        {
            self.send_message(player, message.clone());
        }
    }

    fn broadcast_message(&mut self, message: LobbyToClient) {
        for player in self.players.values_mut() {
            _ = player.message_sender.send(message.clone());
        }
    }
}

async fn connection_handler(incoming: Incoming, event_sender: UnboundedSender<Event>) {
    let Ok(connection) = incoming.await else {
        return;
    };

    let Ok(Ok((mut send, mut recv))) =
        tokio::time::timeout(Duration::from_secs(3), connection.accept_bi()).await
    else {
        return;
    };

    let Ok(Ok(ClientToLobby::Handshake { username })) =
        tokio::time::timeout(Duration::from_secs(3), recv.read_msg::<ClientToLobby>()).await
    else {
        return;
    };

    let id = PlayerId::new();

    let (message_sender, mut messages) = mpsc::unbounded_channel();

    if event_sender
        .send(Event::NewConnection {
            id,
            requested_username: username,
            message_sender,
        })
        .is_err()
    {
        return;
    }

    let mut recv_fut = Box::pin(recv.read_msg::<ClientToLobby>());

    loop {
        select! {
            msg = messages.recv() => {
                let Some(msg) = msg else {return};
                let res = send.write_msg(&msg).await;
                if res.is_err() {
                    return;
                }
            }
            msg = &mut recv_fut => {
                let Ok(msg) = msg else {return};
                let res = event_sender.send(Event::Message{from:id, message:msg});
                if res.is_err() {
                    return;
                }
            }
        }
    }
}
