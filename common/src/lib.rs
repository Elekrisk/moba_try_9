#![feature(macro_metavar_expr_concat)]

use std::{hash::Hash, marker::PhantomData};

use quinn::{RecvStream, SendStream};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

macro_rules! id_type {
    ($marker:ident) => {
        #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
        pub struct ${concat($marker, Id)} {
            uuid: Uuid,
        }

        impl ${concat($marker, Id)} {
            pub fn new() -> Self {
                Self {
                    uuid: Uuid::new_v4(),
                }
            }
        }
    };
}

id_type!(Player);
id_type!(Lobby);
id_type!(Game);
id_type!(Champion);

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Team(pub usize);

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PlayerInfo {
    pub id: PlayerId,
    pub name: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ShortLobbyInfo {
    pub id: LobbyId,
    pub name: String,
    pub players: usize,
    pub max_player_count: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LobbyInfo {
    pub id: LobbyId,
    pub settings: LobbySettings,
    pub players: Vec<Vec<PlayerInLobby>>,
    pub leader: PlayerId,
    pub lobby_state: LobbyState,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum LobbyState {
    Normal,
    ChampSelect,
    InGame,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PlayerInLobby {
    pub id: PlayerId,
    pub selected_champion: Option<ChampionId>,
    pub locked: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LobbySettings {
    pub name: String,
    pub team_count: usize,
    pub players_per_team: usize,
    pub locked: bool,
    pub allows_team_switching: bool,
}

impl LobbySettings {
    pub fn validate(&self) -> bool {
        !self.name.is_empty() && self.team_count > 0 && self.players_per_team > 0
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ClientToLobby {
    Handshake { username: String },
    GetLobbyList,
    GetLobbyInfo { id: LobbyId },
    GetPlayerInfo { player: PlayerId },
    CreateLobby,
    JoinLobby { id: LobbyId },
    LeaveLobby,
    KickPlayer { id: PlayerId },
    SwitchPlayerTeam { id: PlayerId, team: Team },
    GetLobbySettings,
    SetLobbySettings { settings: LobbySettings },
    SetReady { ready: bool },
    ForceLobbyReady,
    SelectChampion { champion: ChampionId },
    SetChampionLocked { locked: bool },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InvalidLobbyReason {
    pub field: LobbySettingsField,
    pub message: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum LobbySettingsField {
    Name,
    TeamCount,
    PlayersPerTeam,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum LobbyToClient {
    Handshake {
        id: PlayerId,
        username: String,
    },
    LobbyList {
        lobbies: Vec<ShortLobbyInfo>,
    },
    LobbyInfo {
        info: LobbyInfo,
    },
    PlayerInfo {
        info: PlayerInfo,
    },
    JoinedLobby {
        id: LobbyId,
    },
    LeftLobby {
        reason: LeaveReason,
    },
    PlayerJoinedLobby {
        player: PlayerId,
    },
    PlayerLeftLobby {
        player: PlayerId,
    },
    PlayerSwitchedTeam {
        player: PlayerId,
        team: Team,
    },
    PlayerBecameLeader {
        player: PlayerId,
    },
    LobbySettings {
        settings: LobbySettings,
    },
    InvalidLobbySettings {
        reasons: Vec<InvalidLobbyReason>,
    },
    PlayerSetReady {
        player: PlayerId,
        ready: bool,
    },
    LobbyStateChanged {
        new_state: LobbyState,
    },
    PlayerSelectedChampion {
        player: PlayerId,
        champion: ChampionId,
    },
    PlayerSetChampionLocked {
        player: PlayerId,
        locked: bool,
    },
    SettingUpGameServer,
    GameServerReady {
        connection: String,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum LeaveReason {
    Leave,
    Kicked,
}

#[allow(async_fn_in_trait)]
pub trait WriteMsg {
    async fn write_msg<T: Serialize>(&mut self, msg: &T) -> anyhow::Result<()>;
}

impl WriteMsg for SendStream {
    async fn write_msg<T: Serialize>(&mut self, msg: &T) -> anyhow::Result<()> {
        let bytes = postcard::to_allocvec(msg)?;
        let len: u32 = bytes.len().try_into().unwrap();
        self.write_all(&len.to_be_bytes()).await?;
        self.write_all(&bytes).await?;
        Ok(())
    }
}

#[allow(async_fn_in_trait)]
pub trait ReadMsg {
    async fn read_msg<T: for<'de> Deserialize<'de>>(&mut self) -> anyhow::Result<T>;
}

impl ReadMsg for RecvStream {
    async fn read_msg<T: for<'de> Deserialize<'de>>(&mut self) -> anyhow::Result<T> {
        let mut len_buffer = [0; std::mem::size_of::<u32>()];
        self.read_exact(&mut len_buffer).await?;
        let len = u32::from_be_bytes(len_buffer);
        let mut buffer = vec![0; len as usize];
        self.read_exact(&mut buffer).await?;
        let msg = postcard::from_bytes(&buffer)?;
        Ok(msg)
    }
}
