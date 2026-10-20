//! rp — Discord roleplay "possess" proxying.
//!
//! A game-like, Tupperbox-style proxy system. Users create **characters**
//! (name + avatar) and "possess" one; while `start`ed, every message they send
//! is **deleted** and **re-posted as that character** through a Discord webhook
//! (handled by the discord-adapter, which receives a `ProxyIdentity` on the
//! `SendToPlatforms` the module emits).
//!
//!   `!rp possess <char>`  select the character you control
//!   `!rp start` / `!rp stop`   proxy mode on/off
//!   `!rp create <char>`   create a character (attach an image to set the avatar)
//!   `!rp <char> avatar`   set/update a character's avatar (attach an image)
//!   `!rp <char> set <k>=<v>`   e.g. `!rp alice set global=true`
//!   `!rp list` / `!rp <char> delete` / `!rp help`
//!
//! A message whose whole text is wrapped in `||spoiler||` is posted normally
//! (the bypass). `>` is reserved for future statuses/asides.
//!
//! Storage: one `turso` DB per server (`<db_dir>/<guild_id>.db`) + a global DB.
//! The module is a CATCH-ALL: it sees every message and parses `!rp` itself.

use futures_util::{SinkExt, StreamExt};
use prost::Message;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use tokio::sync::Mutex as AsyncMutex;
use tokio_tungstenite::tungstenite::protocol::Message as WsMessage;
use tracing::{info, warn};
use tracing_subscriber::FmtSubscriber;

use cockatiel_client::proto::container_for_engine::Payload as EnginePayload;
use cockatiel_client::proto::container_for_module::Payload as ModulePayload;
use cockatiel_client::proto::*;
use cockatiel_client::CockatielClient;

type WsWriteHalf = futures_util::stream::SplitSink<
    tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>,
    WsMessage,
>;

const RP_COMMAND: &str = "rp";
const DEFAULT_FLAG: &str = "!";
const DEFAULT_DB_DIR: &str = "~/.cockatiel/rp";
const DEFAULT_RECONNECT_BASE_SECS: u32 = 1;
const DEFAULT_RECONNECT_MAX_SECS: u32 = 30;
/// The DB key used when a message has no guild (e.g. a DM).
const DM_DB: &str = "_dm";

// ── config ────────────────────────────────────────────────────────────────

#[derive(Debug, Clone)]
struct Settings {
    command_flag: String,
    db_dir: String,
    reconnect_base_secs: u32,
    reconnect_max_secs: u32,
}

/// Read `module_specific`, filling any missing keys with defaults and writing
/// them back so every knob is editable in place in the module manager.
fn ensure_defaults() -> Settings {
    let root: Option<serde_json::Value> = std::fs::read_to_string("config.json")
        .ok()
        .and_then(|d| serde_json::from_str(&d).ok());
    let ms = root
        .as_ref()
        .and_then(|r| r.get("module_specific"))
        .and_then(|m| m.as_object())
        .cloned()
        .unwrap_or_default();
    let command_flag = ms
        .get("command_flag")
        .and_then(|v| v.as_str())
        .unwrap_or(DEFAULT_FLAG)
        .to_string();
    let db_dir = ms
        .get("db_dir")
        .and_then(|v| v.as_str())
        .unwrap_or(DEFAULT_DB_DIR)
        .to_string();
    let reconnect_base_secs = ms
        .get("reconnect_base_secs")
        .and_then(|v| v.as_u64())
        .map(|v| v as u32)
        .unwrap_or(DEFAULT_RECONNECT_BASE_SECS);
    let reconnect_max_secs = ms
        .get("reconnect_max_secs")
        .and_then(|v| v.as_u64())
        .map(|v| v as u32)
        .unwrap_or(DEFAULT_RECONNECT_MAX_SECS);
    if let Some(mut root) = root {
        if let Some(obj) = root.as_object_mut() {
            if let Some(ms) = obj
                .entry("module_specific".to_string())
                .or_insert_with(|| serde_json::json!({}))
                .as_object_mut()
            {
                ms.entry("command_flag".to_string())
                    .or_insert_with(|| serde_json::json!(command_flag));
                ms.entry("db_dir".to_string())
                    .or_insert_with(|| serde_json::json!(db_dir));
                ms.entry("reconnect_base_secs".to_string())
                    .or_insert_with(|| serde_json::json!(reconnect_base_secs));
                ms.entry("reconnect_max_secs".to_string())
                    .or_insert_with(|| serde_json::json!(reconnect_max_secs));
            }
            let _ = std::fs::write("config.json", serde_json::to_string_pretty(&root).unwrap());
        }
    }
    Settings {
        command_flag,
        db_dir,
        reconnect_base_secs,
        reconnect_max_secs,
    }
}

/// Expand a leading `~` to `$HOME` (module config paths are written by hand).
fn expand_tilde(path: &str) -> String {
    if let Some(rest) = path.strip_prefix("~/") {
        if let Ok(home) = std::env::var("HOME") {
            return format!("{home}/{rest}");
        }
    } else if path == "~" {
        if let Ok(home) = std::env::var("HOME") {
            return home;
        }
    }
    path.to_string()
}

// ── pure helpers (unit-tested) ──────────────────────────────────────────────

/// Parse the args after `!rp` from a raw chat message, or `None` if the message
/// is not a `!rp` command. `flag` is the command sigil ("!"), `name` the command
/// ("rp"); the name must match as a whole word ("!rpx" does not count).
fn parse_rp_args(raw: &str, flag: &str, name: &str) -> Option<String> {
    let t = raw.trim_start();
    let rest = t.strip_prefix(flag)?;
    let after = rest.trim_start();
    let head = after.get(..name.len())?;
    if !head.eq_ignore_ascii_case(name) {
        return None;
    }
    let tail = &after[name.len()..];
    match tail.chars().next() {
        None => Some(String::new()),
        Some(c) if c.is_whitespace() => Some(tail.trim().to_string()),
        _ => None,
    }
}

/// The RP bypass: a message whose whole (trimmed) text is wrapped in `||…||`
/// spoiler tags is posted normally, not proxied.
fn is_spoiler_bypass(text: &str) -> bool {
    let t = text.trim();
    t.len() >= 4 && t.starts_with("||") && t.ends_with("||")
}

/// A parsed `!rp` subcommand.
#[derive(Debug, PartialEq, Eq)]
enum RpCmd {
    Possess(String),
    Start,
    Stop,
    Create(String),
    Delete(String),
    List,
    Help,
    SetChar { name: String, key: String, value: String },
    SetAvatar { name: String },
}

/// Parse the args (already stripped of `!rp`) into a subcommand.
fn parse_rp_command(args: &str) -> Option<RpCmd> {
    let args = args.trim();
    let (head, rest) = match args.split_once(char::is_whitespace) {
        Some((h, r)) => (h, r.trim()),
        None => (args, ""),
    };
    match head.to_ascii_lowercase().as_str() {
        "" => None,
        "help" => Some(RpCmd::Help),
        "list" => Some(RpCmd::List),
        "start" => Some(RpCmd::Start),
        "stop" => Some(RpCmd::Stop),
        "possess" => (!rest.is_empty()).then(|| RpCmd::Possess(rest.to_string())),
        "create" => (!rest.is_empty()).then(|| RpCmd::Create(rest.to_string())),
        "delete" => (!rest.is_empty()).then(|| RpCmd::Delete(rest.to_string())),
        // Otherwise the head is a character NAME followed by a verb.
        _ => {
            let name = head.to_string();
            let (verb, tail) = match rest.split_once(char::is_whitespace) {
                Some((v, t)) => (v, t.trim()),
                None => (rest, ""),
            };
            match verb.to_ascii_lowercase().as_str() {
                "avatar" => Some(RpCmd::SetAvatar { name }),
                "set" => {
                    let (k, v) = tail.split_once('=')?;
                    let k = k.trim();
                    if k.is_empty() {
                        return None;
                    }
                    Some(RpCmd::SetChar {
                        name,
                        key: k.to_string(),
                        value: v.trim().to_string(),
                    })
                }
                _ => None,
            }
        }
    }
}

/// The stable per-character key the adapter caches its webhook under. Namespaced
/// by owner so two users with a same-named character never share a webhook.
fn character_key(user_uuid7: &str, name: &str) -> String {
    format!("{user_uuid7}:{name}")
}

/// Split a `global=true|false` value; anything else is not a bool.
fn parse_bool(v: &str) -> Option<bool> {
    match v.trim().to_ascii_lowercase().as_str() {
        "true" | "1" | "yes" | "on" => Some(true),
        "false" | "0" | "no" | "off" => Some(false),
        _ => None,
    }
}

/// Pull the Discord bits the RP flow needs out of the adapter's raw message
/// JSON (`raw_data`): the message id, the guild id, and the first attachment
/// URL (for `!rp create`/`avatar`). Missing fields come back empty.
fn discord_meta(raw_data: &[u8]) -> (String, String, String) {
    let Ok(v) = serde_json::from_slice::<serde_json::Value>(raw_data) else {
        return (String::new(), String::new(), String::new());
    };
    let s = |k: &str| v.get(k).and_then(|x| x.as_str()).unwrap_or("").to_string();
    let attach = v
        .get("attachments")
        .and_then(|a| a.as_array())
        .and_then(|a| a.first())
        .and_then(|a| a.get("url"))
        .and_then(|u| u.as_str())
        .unwrap_or("")
        .to_string();
    (s("id"), s("guild_id"), attach)
}

// ── store (turso) ───────────────────────────────────────────────────────────

fn vtext(s: &str) -> turso::Value {
    turso::Value::Text(s.to_string())
}
fn vint(i: i64) -> turso::Value {
    turso::Value::Integer(i)
}

/// One SQLite (turso) database: a server's characters + possession state, or the
/// global characters.
struct Store {
    conn: turso::Connection,
}

impl Store {
    async fn open(path: &str) -> Result<Self, String> {
        if let Some(parent) = std::path::Path::new(path).parent() {
            let _ = std::fs::create_dir_all(parent);
        }
        let db = turso::Builder::new_local(path)
            .build()
            .await
            .map_err(|e| format!("open {path}: {e}"))?;
        let conn = db.connect().map_err(|e| format!("connect {path}: {e}"))?;
        let store = Self { conn };
        store.ensure_schema().await?;
        Ok(store)
    }

    async fn ensure_schema(&self) -> Result<(), String> {
        self.conn
            .execute(
                "CREATE TABLE IF NOT EXISTS characters (\
                 user_uuid7 TEXT NOT NULL, name TEXT NOT NULL, avatar BLOB, \
                 updated_at INTEGER NOT NULL DEFAULT 0, \
                 PRIMARY KEY (user_uuid7, name))",
                (),
            )
            .await
            .map_err(|e| e.to_string())?;
        self.conn
            .execute(
                "CREATE TABLE IF NOT EXISTS possession (\
                 user_uuid7 TEXT NOT NULL, channel_id TEXT NOT NULL, \
                 char_name TEXT NOT NULL, started INTEGER NOT NULL DEFAULT 0, \
                 PRIMARY KEY (user_uuid7, channel_id))",
                (),
            )
            .await
            .map_err(|e| e.to_string())?;
        Ok(())
    }

    async fn character_exists(&self, user: &str, name: &str) -> Result<bool, String> {
        let mut rows = self
            .conn
            .query(
                "SELECT 1 FROM characters WHERE user_uuid7 = ?1 AND name = ?2",
                turso::params![vtext(user), vtext(name)],
            )
            .await
            .map_err(|e| e.to_string())?;
        let found = rows.next().await.map_err(|e| e.to_string())?.is_some();
        while rows.next().await.map_err(|e| e.to_string())?.is_some() {}
        Ok(found)
    }

    async fn create_character(&self, user: &str, name: &str, avatar: Option<&[u8]>) -> Result<(), String> {
        let now = now_ms();
        let avatar = avatar.map(|b| turso::Value::Blob(b.to_vec())).unwrap_or(turso::Value::Null);
        self.conn
            .execute(
                "INSERT INTO characters (user_uuid7, name, avatar, updated_at) VALUES (?1, ?2, ?3, ?4)",
                turso::params![vtext(user), vtext(name), avatar, vint(now)],
            )
            .await
            .map(|_| ())
            .map_err(|e| e.to_string())
    }

    async fn set_avatar(&self, user: &str, name: &str, avatar: &[u8]) -> Result<(), String> {
        self.conn
            .execute(
                "UPDATE characters SET avatar = ?3, updated_at = ?4 WHERE user_uuid7 = ?1 AND name = ?2",
                turso::params![
                    vtext(user),
                    vtext(name),
                    turso::Value::Blob(avatar.to_vec()),
                    vint(now_ms())
                ],
            )
            .await
            .map(|_| ())
            .map_err(|e| e.to_string())
    }

    async fn get_avatar(&self, user: &str, name: &str) -> Result<Option<Vec<u8>>, String> {
        let mut rows = self
            .conn
            .query(
                "SELECT avatar FROM characters WHERE user_uuid7 = ?1 AND name = ?2",
                turso::params![vtext(user), vtext(name)],
            )
            .await
            .map_err(|e| e.to_string())?;
        let out = match rows.next().await.map_err(|e| e.to_string())? {
            Some(row) => row.get::<Option<Vec<u8>>>(0).map_err(|e| e.to_string())?,
            None => None,
        };
        while rows.next().await.map_err(|e| e.to_string())?.is_some() {}
        Ok(out)
    }

    async fn delete_character(&self, user: &str, name: &str) -> Result<(), String> {
        self.conn
            .execute(
                "DELETE FROM characters WHERE user_uuid7 = ?1 AND name = ?2",
                turso::params![vtext(user), vtext(name)],
            )
            .await
            .map(|_| ())
            .map_err(|e| e.to_string())
    }

    async fn list_characters(&self, user: &str) -> Result<Vec<String>, String> {
        let mut rows = self
            .conn
            .query(
                "SELECT name FROM characters WHERE user_uuid7 = ?1 ORDER BY name ASC",
                turso::params![vtext(user)],
            )
            .await
            .map_err(|e| e.to_string())?;
        let mut out = Vec::new();
        while let Some(row) = rows.next().await.map_err(|e| e.to_string())? {
            if let Ok(n) = row.get::<String>(0) {
                out.push(n);
            }
        }
        Ok(out)
    }

    async fn set_possession(
        &self,
        user: &str,
        channel: &str,
        char_name: &str,
        started: bool,
    ) -> Result<(), String> {
        self.conn
            .execute(
                "INSERT OR REPLACE INTO possession (user_uuid7, channel_id, char_name, started) \
                 VALUES (?1, ?2, ?3, ?4)",
                turso::params![
                    vtext(user),
                    vtext(channel),
                    vtext(char_name),
                    vint(started as i64)
                ],
            )
            .await
            .map(|_| ())
            .map_err(|e| e.to_string())
    }

    async fn get_possession(&self, user: &str, channel: &str) -> Result<Option<(String, bool)>, String> {
        let mut rows = self
            .conn
            .query(
                "SELECT char_name, started FROM possession WHERE user_uuid7 = ?1 AND channel_id = ?2",
                turso::params![vtext(user), vtext(channel)],
            )
            .await
            .map_err(|e| e.to_string())?;
        let out = match rows.next().await.map_err(|e| e.to_string())? {
            Some(row) => {
                let name: String = row.get(0).map_err(|e| e.to_string())?;
                let started: i64 = row.get(1).map_err(|e| e.to_string())?;
                Some((name, started != 0))
            }
            None => None,
        };
        while rows.next().await.map_err(|e| e.to_string())?.is_some() {}
        Ok(out)
    }
}

fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

/// The per-guild + global stores, opened lazily and cached (the read task owns
/// them, so no locking is needed — the module runs one loop).
struct Stores {
    dir: String,
    by_guild: HashMap<String, Store>,
    global: Option<Store>,
}

impl Stores {
    fn new(dir: String) -> Self {
        Self {
            dir,
            by_guild: HashMap::new(),
            global: None,
        }
    }

    fn path_for(key: &str, dir: &str) -> String {
        format!("{}/{key}.db", expand_tilde(dir).trim_end_matches('/'))
    }

    async fn guild(&mut self, key: &str) -> Result<&Store, String> {
        if !self.by_guild.contains_key(key) {
            let store = Store::open(&Self::path_for(key, &self.dir)).await?;
            self.by_guild.insert(key.to_string(), store);
        }
        Ok(self.by_guild.get(key).unwrap())
    }

    async fn global_store(&mut self) -> Result<&Store, String> {
        if self.global.is_none() {
            let store = Store::open(&Self::path_for("_global", &self.dir)).await?;
            self.global = Some(store);
        }
        Ok(self.global.as_ref().unwrap())
    }

    /// Look up a character's avatar in the server DB, then the global DB.
    async fn find_avatar(&mut self, guild: &str, user: &str, name: &str) -> Option<Vec<u8>> {
        if let Ok(s) = self.guild(guild).await {
            if let Ok(Some(a)) = s.get_avatar(user, name).await {
                return Some(a);
            }
        }
        if let Ok(s) = self.global_store().await {
            if let Ok(Some(a)) = s.get_avatar(user, name).await {
                return Some(a);
            }
        }
        None
    }

    /// Whether a character exists in the server DB or the global DB.
    async fn character_exists_any(&mut self, guild: &str, user: &str, name: &str) -> bool {
        if let Ok(s) = self.guild(guild).await {
            if s.character_exists(user, name).await.unwrap_or(false) {
                return true;
            }
        }
        if let Ok(s) = self.global_store().await {
            if s.character_exists(user, name).await.unwrap_or(false) {
                return true;
            }
        }
        false
    }
}

// ── engine plumbing ─────────────────────────────────────────────────────────

#[derive(Clone)]
struct EngineIdentity {
    auth: String,
    instance: String,
    module: String,
}

async fn send_container(write: &Arc<AsyncMutex<WsWriteHalf>>, c: ContainerForEngine) {
    let mut buf = Vec::new();
    if c.encode(&mut buf).is_ok() {
        let mut w = write.lock().await;
        let _ = w.send(WsMessage::Binary(buf)).await;
    }
}

async fn register_catch_all(write: &Arc<AsyncMutex<WsWriteHalf>>, id: &Arc<AsyncMutex<EngineIdentity>>) {
    let id = id.lock().await.clone();
    let c = ContainerForEngine {
        version: 2,
        auth_token: id.auth,
        module_name: id.module,
        module_instance_uuid7: id.instance,
        payload: Some(EnginePayload::Commands(Commands {
            commands: vec![],
            alert_on_unknown_command: false,
        })),
    };
    send_container(write, c).await;
    info!("registered as a catch-all (parses !rp itself)");
}

/// Build a base SendToPlatforms (actor = the Discord author).
fn base_send(id: &EngineIdentity, chat: &ChatMessage, msg: String) -> SendToPlatforms {
    let handle = chat
        .user_data
        .as_ref()
        .map(|u| u.username.clone())
        .unwrap_or_default();
    SendToPlatforms {
        msg,
        level: 0,
        module_uuid7: id.instance.clone(),
        pid: String::new(),
        platform: chat.platform.clone(),
        actor_platform: chat.platform.clone(),
        actor_handle: handle,
        actor_uuid7: chat.user_uuid7.clone(),
        channel_id: chat.channel_id.clone(),
        ..Default::default()
    }
}

/// A plain chat reply (command feedback).
async fn send_chat(write: &Arc<AsyncMutex<WsWriteHalf>>, id: &EngineIdentity, msg: String, chat: &ChatMessage) {
    let c = ContainerForEngine {
        version: 2,
        auth_token: id.auth.clone(),
        module_name: id.module.clone(),
        module_instance_uuid7: id.instance.clone(),
        payload: Some(EnginePayload::SendToPlatforms(base_send(id, chat, msg))),
    };
    send_container(write, c).await;
}

/// A PROXIED send: post `msg` as the character, then delete the trigger.
#[allow(clippy::too_many_arguments)]
async fn send_proxy(
    write: &Arc<AsyncMutex<WsWriteHalf>>,
    id: &EngineIdentity,
    chat: &ChatMessage,
    msg: String,
    char_key: String,
    display_name: String,
    avatar: Option<Vec<u8>>,
    delete_message_id: String,
) {
    let mut send = base_send(id, chat, msg);
    send.proxy = Some(ProxyIdentity {
        character_key: char_key,
        display_name,
        avatar: avatar.unwrap_or_default(),
    });
    send.delete_message_id = delete_message_id;
    let c = ContainerForEngine {
        version: 2,
        auth_token: id.auth.clone(),
        module_name: id.module.clone(),
        module_instance_uuid7: id.instance.clone(),
        payload: Some(EnginePayload::SendToPlatforms(send)),
    };
    send_container(write, c).await;
}

/// Download an attached image (Discord CDN URL) as bytes.
async fn fetch_bytes(client: &reqwest::Client, url: &str) -> Result<Vec<u8>, String> {
    let resp = client
        .get(url)
        .timeout(std::time::Duration::from_secs(15))
        .send()
        .await
        .map_err(|e| e.to_string())?;
    if !resp.status().is_success() {
        return Err(format!("attachment fetch {}", resp.status()));
    }
    Ok(resp.bytes().await.map_err(|e| e.to_string())?.to_vec())
}

// ── command handling ────────────────────────────────────────────────────────

/// Handle one `!rp` command. Feedback goes back to the channel as a plain,
/// non-proxied reply.
async fn handle_command(
    write: &Arc<AsyncMutex<WsWriteHalf>>,
    id: &EngineIdentity,
    chat: &ChatMessage,
    stores: &mut Stores,
    sent_avatars: &mut HashSet<String>,
    guild: &str,
    cmd: RpCmd,
) {
    let user = chat.user_uuid7.clone();
    let channel = chat.channel_id.clone();
    let reply = |m: String| {
        let (write, id, chat) = (write.clone(), id.clone(), chat.clone());
        async move { send_chat(&write, &id, m, &chat).await }
    };

    match cmd {
        RpCmd::Help => {
            reply(format!(
                "RP: `{f}rp possess <char>` · `{f}rp start`/`stop` · `{f}rp create <char>` (attach an image for the avatar) · `{f}rp <char> set global=true` · `{f}rp list` · `{f}rp <char> delete`",
                f = DEFAULT_FLAG
            ))
            .await;
        }
        RpCmd::List => {
            let mut names = Vec::new();
            if let Ok(s) = stores.guild(guild).await {
                names.extend(s.list_characters(&user).await.unwrap_or_default());
            }
            if let Ok(s) = stores.global_store().await {
                for n in s.list_characters(&user).await.unwrap_or_default() {
                    if !names.contains(&n) {
                        names.push(format!("{n} (global)"));
                    }
                }
            }
            let msg = if names.is_empty() {
                "You have no characters yet. `!rp create <name>` to make one.".to_string()
            } else {
                format!("Your characters: {}", names.join(", "))
            };
            reply(msg).await;
        }
        RpCmd::Create(name) => {
            if stores.character_exists_any(guild, &user, &name).await {
                reply(format!("You already have a character named `{name}`.")).await;
                return;
            }
            let (_, _, attach) = discord_meta(&chat.raw_data);
            let avatar = fetch_attachment(&attach).await;
            match stores.guild(guild).await {
                Ok(s) => match s.create_character(&user, &name, avatar.as_deref()).await {
                    Ok(()) => {
                        reply(match avatar {
                            Some(_) => format!("Created `{name}` with an avatar."),
                            None => format!("Created `{name}`. Add an avatar with `!rp {name} avatar` + an image."),
                        })
                        .await;
                    }
                    Err(e) => reply(format!("Could not create `{name}`: {e}")).await,
                },
                Err(e) => reply(format!("Storage error: {e}")).await,
            }
        }
        RpCmd::Delete(name) => {
            let mut done = false;
            if let Ok(s) = stores.guild(guild).await {
                if s.character_exists(&user, &name).await.unwrap_or(false) {
                    let _ = s.delete_character(&user, &name).await;
                    done = true;
                }
            }
            if let Ok(s) = stores.global_store().await {
                if s.character_exists(&user, &name).await.unwrap_or(false) {
                    let _ = s.delete_character(&user, &name).await;
                    done = true;
                }
            }
            reply(if done {
                format!("Deleted `{name}`.")
            } else {
                format!("No character named `{name}`.")
            })
            .await;
        }
        RpCmd::SetAvatar { name } => {
            let (_, _, attach) = discord_meta(&chat.raw_data);
            let Some(avatar) = fetch_attachment(&attach).await else {
                reply(format!("Attach an image to set `{name}`'s avatar.")).await;
                return;
            };
            let mut ok = false;
            if let Ok(s) = stores.guild(guild).await {
                if s.character_exists(&user, &name).await.unwrap_or(false)
                    && s.set_avatar(&user, &name, &avatar).await.is_ok()
                {
                    ok = true;
                }
            }
            if !ok {
                if let Ok(s) = stores.global_store().await {
                    if s.character_exists(&user, &name).await.unwrap_or(false)
                        && s.set_avatar(&user, &name, &avatar).await.is_ok()
                    {
                        ok = true;
                    }
                }
            }
            if ok {
                sent_avatars.clear(); // force a re-send so the webhook avatar updates
                reply(format!("Updated `{name}`'s avatar.")).await;
            } else {
                reply(format!("No character named `{name}`.")).await;
            }
        }
        RpCmd::SetChar { name, key, value } => {
            if key.eq_ignore_ascii_case("global") {
                match parse_bool(&value) {
                    Some(true) => {
                        // Move the character from the server DB into the global DB.
                        let mut moved = false;
                        if let Ok(s) = stores.guild(guild).await {
                            if s.character_exists(&user, &name).await.unwrap_or(false) {
                                let av = s.get_avatar(&user, &name).await.ok().flatten();
                                let _ = s.delete_character(&user, &name).await;
                                if let Ok(g) = stores.global_store().await {
                                    let _ = g.create_character(&user, &name, av.as_deref()).await;
                                    moved = true;
                                }
                            }
                        }
                        reply(if moved {
                            format!("`{name}` is now global.")
                        } else {
                            format!("No character named `{name}` here.")
                        })
                        .await;
                    }
                    Some(false) => {
                        let mut moved = false;
                        if let Ok(g) = stores.global_store().await {
                            if g.character_exists(&user, &name).await.unwrap_or(false) {
                                let av = g.get_avatar(&user, &name).await.ok().flatten();
                                let _ = g.delete_character(&user, &name).await;
                                if let Ok(s) = stores.guild(guild).await {
                                    let _ = s.create_character(&user, &name, av.as_deref()).await;
                                    moved = true;
                                }
                            }
                        }
                        reply(if moved {
                            format!("`{name}` is no longer global.")
                        } else {
                            format!("No global character named `{name}`.")
                        })
                        .await;
                    }
                    None => reply("`global` must be true or false.".to_string()).await,
                }
            } else {
                reply(format!("Unknown setting `{key}`. Try `global=true`.")).await;
            }
        }
        RpCmd::Possess(name) => {
            if !stores.character_exists_any(guild, &user, &name).await {
                reply(format!("No character named `{name}` — create it first.")).await;
                return;
            }
            // Keep the current started state; just swap the character.
            let started = possession_of(stores, guild, &user, &channel)
                .await
                .map(|(_, s)| s)
                .unwrap_or(false);
            if let Ok(s) = stores.guild(guild).await {
                let _ = s.set_possession(&user, &channel, &name, started).await;
            }
            reply(if started {
                format!("Now speaking as `{name}`.")
            } else {
                format!("Possessing `{name}`. `!rp start` to begin.")
            })
            .await;
        }
        RpCmd::Start => {
            let possessed = possession_of(stores, guild, &user, &channel)
                .await
                .map(|(c, _)| c);
            let Some(name) = possessed else {
                reply("Pick a character first: `!rp possess <char>`.".to_string()).await;
                return;
            };
            if !stores.character_exists_any(guild, &user, &name).await {
                reply(format!("`{name}` no longer exists — pick another.")).await;
                return;
            }
            if let Ok(s) = stores.guild(guild).await {
                let _ = s.set_possession(&user, &channel, &name, true).await;
            }
            sent_avatars.clear();
            reply(format!("Started — your messages now post as `{name}`. `!rp stop` to end.")).await;
        }
        RpCmd::Stop => {
            if let Some((name, _)) = possession_of(stores, guild, &user, &channel).await {
                if let Ok(s) = stores.guild(guild).await {
                    let _ = s.set_possession(&user, &channel, &name, false).await;
                }
            }
            reply("Stopped proxying. Your messages post normally again.".to_string()).await;
        }
    }
}

/// Look up (char_name, started) for a user+channel.
async fn possession_of(stores: &mut Stores, guild: &str, user: &str, channel: &str) -> Option<(String, bool)> {
    if let Ok(s) = stores.guild(guild).await {
        if let Ok(Some(p)) = s.get_possession(user, channel).await {
            return Some(p);
        }
    }
    None
}

/// Fetch an attachment URL as bytes (best-effort — a missing image is fine).
async fn fetch_attachment(url: &str) -> Option<Vec<u8>> {
    if url.is_empty() {
        return None;
    }
    fetch_bytes(&reqwest::Client::new(), url).await.ok()
}

// ── main ────────────────────────────────────────────────────────────────────

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let subscriber = FmtSubscriber::builder()
        .with_max_level(tracing::Level::INFO)
        .with_ansi(false)
        .with_writer(std::io::stderr)
        .finish();
    tracing::subscriber::set_global_default(subscriber).unwrap();

    let client = CockatielClient::connect("config.json").await?;
    let (write, read) = client.stream.split();
    let write_shared: Arc<AsyncMutex<WsWriteHalf>> = Arc::new(AsyncMutex::new(write));
    let module_name = client.config.module_name.clone();
    info!("rp module connected as '{}'", module_name);
    let identity: Arc<AsyncMutex<EngineIdentity>> = Arc::new(AsyncMutex::new(EngineIdentity {
        auth: client.auth_token.clone(),
        instance: client.instance_uuid7.clone(),
        module: module_name,
    }));

    let settings = ensure_defaults();
    let write_for_task = Arc::clone(&write_shared);
    let identity_for_task = Arc::clone(&identity);

    tokio::spawn(async move {
        let mut read = read;
        let mut stores = Stores::new(settings.db_dir.clone());
        let mut sent_avatars: HashSet<String> = HashSet::new();
        loop {
            register_catch_all(&write_for_task, &identity_for_task).await;
            loop {
                let Some(msg) = read.next().await else { break };
                let data = match msg {
                    Ok(WsMessage::Binary(d)) => d,
                    Ok(WsMessage::Close(_)) => break,
                    Ok(_) => continue,
                    Err(e) => {
                        warn!("Engine WebSocket error: {}", e);
                        break;
                    }
                };
                let Ok(container) = ContainerForModule::decode(data.as_ref()) else { continue };
                let id = identity_for_task.lock().await.clone();
                match container.payload {
                    Some(ModulePayload::AuthVerify(_)) => {
                        let reply = ContainerForEngine {
                            version: 2,
                            auth_token: id.auth.clone(),
                            module_name: id.module.clone(),
                            module_instance_uuid7: id.instance.clone(),
                            payload: Some(EnginePayload::AuthVerify(AuthVerify {
                                cur_auth: id.auth.clone(),
                            })),
                        };
                        send_container(&write_for_task, reply).await;
                    }
                    Some(ModulePayload::MessageInProcess(process)) => {
                        if !process.message_uuid7.is_empty() {
                            let receipt = ContainerForEngine {
                                version: 2,
                                auth_token: id.auth.clone(),
                                module_name: id.module.clone(),
                                module_instance_uuid7: id.instance.clone(),
                                payload: Some(EnginePayload::MessageAck(MessageAck {
                                    message_uuid7: process.message_uuid7.clone(),
                                })),
                            };
                            send_container(&write_for_task, receipt).await;
                        }
                        if let Some(chat) = &process.raw_message {
                            handle_message(
                                &write_for_task, &id, chat, &mut stores, &mut sent_avatars,
                                &settings.command_flag,
                            )
                            .await;
                        }
                        // Echo the stage so the pipeline advances.
                        let ack = ContainerForEngine {
                            version: 2,
                            auth_token: id.auth.clone(),
                            module_name: id.module.clone(),
                            module_instance_uuid7: id.instance.clone(),
                            payload: Some(EnginePayload::MessageInProcess(MessageInProcess {
                                message_uuid7: process.message_uuid7,
                                raw_message: process.raw_message,
                                processed_message: process.processed_message,
                                abandon_message: process.abandon_message,
                                audio: process.audio,
                                audio_type: process.audio_type,
                            })),
                        };
                        send_container(&write_for_task, ack).await;
                    }
                    Some(ModulePayload::Shutdown(_)) => {
                        info!("Engine requested shutdown — exiting");
                        std::process::exit(0);
                    }
                    _ => {}
                }
            }

            info!("Engine disconnected — reconnecting...");
            let mut backoff = settings.reconnect_base_secs;
            loop {
                tokio::time::sleep(std::time::Duration::from_secs(backoff as u64)).await;
                match CockatielClient::connect("config.json").await {
                    Ok(conn) => {
                        info!("Reconnected to engine");
                        let (w, r) = conn.stream.split();
                        *write_for_task.lock().await = w;
                        *identity_for_task.lock().await = EngineIdentity {
                            auth: conn.auth_token,
                            instance: conn.instance_uuid7,
                            module: conn.config.module_name,
                        };
                        read = r;
                        break;
                    }
                    Err(e) => {
                        warn!("Engine reconnect failed: {} — retrying in {}s", e, backoff);
                        backoff = (backoff * 2).min(settings.reconnect_max_secs);
                    }
                }
            }
        }
    });

    loop {
        tokio::time::sleep(std::time::Duration::from_secs(3600)).await;
    }
}

/// Handle one message: a `!rp` command, or a proxied post for a started user.
async fn handle_message(
    write: &Arc<AsyncMutex<WsWriteHalf>>,
    id: &EngineIdentity,
    chat: &ChatMessage,
    stores: &mut Stores,
    sent_avatars: &mut HashSet<String>,
    command_flag: &str,
) {
    if chat.platform != "discord" || chat.user_uuid7.is_empty() {
        return;
    }
    let (msg_id, guild, _attach) = discord_meta(&chat.raw_data);
    let guild_key = if guild.is_empty() { DM_DB.to_string() } else { guild };
    let user = chat.user_uuid7.clone();
    let channel = chat.channel_id.clone();

    // A `!rp` command (parsed here — the module is a catch-all, not a command owner).
    if let Some(args) = parse_rp_args(&chat.raw_message, command_flag, RP_COMMAND) {
        if let Some(cmd) = parse_rp_command(&args) {
            handle_command(write, id, chat, stores, sent_avatars, &guild_key, cmd).await;
        }
        return;
    }

    // Otherwise: proxy the message if the user is started here.
    let Some((char_name, started)) = possession_of(stores, &guild_key, &user, &channel).await else {
        return;
    };
    if !started {
        return;
    }
    let text = chat.raw_message.clone();
    if text.trim().is_empty() || is_spoiler_bypass(&text) {
        return; // nothing to post, or an intentional bypass
    }
    let key = character_key(&user, &char_name);
    let cache_key = format!("{channel}\u{1f}{key}");
    let avatar = if sent_avatars.contains(&cache_key) {
        None
    } else {
        let a = stores.find_avatar(&guild_key, &user, &char_name).await;
        if a.is_some() {
            sent_avatars.insert(cache_key);
        }
        a
    };
    send_proxy(
        write,
        id,
        chat,
        text,
        key,
        char_name,
        avatar,
        msg_id,
    )
    .await;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_rp_only_as_a_whole_word() {
        assert_eq!(parse_rp_args("!rp start", "!", "rp").as_deref(), Some("start"));
        assert_eq!(parse_rp_args("!rp", "!", "rp").as_deref(), Some(""));
        assert_eq!(parse_rp_args("  !rp   possess alice ", "!", "rp").as_deref(), Some("possess alice"));
        assert_eq!(parse_rp_args("!RP Start", "!", "rp").as_deref(), Some("Start"));
        assert!(parse_rp_args("!rpx", "!", "rp").is_none());
        assert!(parse_rp_args("hello", "!", "rp").is_none());
        assert!(parse_rp_args("rp start", "!", "rp").is_none());
    }

    #[test]
    fn spoiler_bypass_requires_the_whole_message() {
        assert!(is_spoiler_bypass("||hi||"));
        assert!(is_spoiler_bypass("  ||hi there||  "));
        assert!(!is_spoiler_bypass("||hi"));
        assert!(!is_spoiler_bypass("hi||"));
        assert!(!is_spoiler_bypass("hi"));
        assert!(!is_spoiler_bypass(""));
    }

    #[test]
    fn parses_subcommands() {
        assert_eq!(parse_rp_command("start"), Some(RpCmd::Start));
        assert_eq!(parse_rp_command("STOP"), Some(RpCmd::Stop));
        assert_eq!(parse_rp_command("possess alice"), Some(RpCmd::Possess("alice".into())));
        assert_eq!(parse_rp_command("create bob"), Some(RpCmd::Create("bob".into())));
        assert_eq!(parse_rp_command("list"), Some(RpCmd::List));
        assert_eq!(parse_rp_command("help"), Some(RpCmd::Help));
        assert_eq!(parse_rp_command("delete alice"), Some(RpCmd::Delete("alice".into())));
        assert_eq!(
            parse_rp_command("alice set global=true"),
            Some(RpCmd::SetChar { name: "alice".into(), key: "global".into(), value: "true".into() })
        );
        assert_eq!(parse_rp_command("alice avatar"), Some(RpCmd::SetAvatar { name: "alice".into() }));
        assert_eq!(parse_rp_command(""), None);
        assert_eq!(parse_rp_command("alice"), None);
        assert_eq!(parse_rp_command("alice set noequals"), None);
    }

    #[test]
    fn character_keys_are_namespaced_by_owner() {
        assert_ne!(character_key("u1", "alice"), character_key("u2", "alice"));
        assert_eq!(character_key("u1", "alice"), "u1:alice");
    }

    #[test]
    fn bools_parse_both_ways() {
        assert_eq!(parse_bool("true"), Some(true));
        assert_eq!(parse_bool("FALSE"), Some(false));
        assert_eq!(parse_bool("on"), Some(true));
        assert_eq!(parse_bool("maybe"), None);
    }

    #[test]
    fn discord_meta_extracts_id_guild_and_attachment() {
        let raw = br#"{"id":"111","guild_id":"222","attachments":[{"url":"https://cdn/x.png"}]}"#;
        let (id, guild, attach) = discord_meta(raw);
        assert_eq!(id, "111");
        assert_eq!(guild, "222");
        assert_eq!(attach, "https://cdn/x.png");
        // Missing fields are empty, not a panic.
        let (id, guild, attach) = discord_meta(b"{}");
        assert!(id.is_empty() && guild.is_empty() && attach.is_empty());
        let (id, _, _) = discord_meta(b"not json");
        assert!(id.is_empty());
    }

    #[test]
    fn tilde_expands() {
        let home = std::env::var("HOME").unwrap_or_default();
        assert_eq!(expand_tilde("~/.cockatiel/rp"), format!("{home}/.cockatiel/rp"));
        assert_eq!(expand_tilde("/abs/path"), "/abs/path");
    }
}
