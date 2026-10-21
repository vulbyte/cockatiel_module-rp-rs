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
    /// Where the `state` module keeps its per-server DBs — the character sheet
    /// reads a character's stats from there (best-effort).
    state_db_dir: String,
    reconnect_base_secs: u32,
    reconnect_max_secs: u32,
}

const DEFAULT_STATE_DB_DIR: &str = "~/.cockatiel/state";

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
    let state_db_dir = ms
        .get("state_db_dir")
        .and_then(|v| v.as_str())
        .unwrap_or(DEFAULT_STATE_DB_DIR)
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
                ms.entry("state_db_dir".to_string())
                    .or_insert_with(|| serde_json::json!(state_db_dir));
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
        state_db_dir,
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
    Sheet(String),
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
        "sheet" | "info" | "show" => (!rest.is_empty()).then(|| RpCmd::Sheet(rest.to_string())),
        // Otherwise the head is a character NAME followed by a verb.
        _ => {
            let name = head.to_string();
            let (verb, tail) = match rest.split_once(char::is_whitespace) {
                Some((v, t)) => (v, t.trim()),
                None => (rest, ""),
            };
            match verb.to_ascii_lowercase().as_str() {
                "avatar" => Some(RpCmd::SetAvatar { name }),
                "sheet" | "info" | "show" => Some(RpCmd::Sheet(name)),
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
/// A character's identity + usage counters (for the sheet).
#[derive(Debug, Clone, Default)]
struct CharacterInfo {
    name: String,
    color: String,
    pronouns: String,
    bio: String,
    tags: String,
    group: String,
    created_at: i64,
    turns: i64,
    has_avatar: bool,
}

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
                 color TEXT NOT NULL DEFAULT '', pronouns TEXT NOT NULL DEFAULT '', \
                 bio TEXT NOT NULL DEFAULT '', tags TEXT NOT NULL DEFAULT '', \
                 grp TEXT NOT NULL DEFAULT '', created_at INTEGER NOT NULL DEFAULT 0, \
                 turns INTEGER NOT NULL DEFAULT 0, \
                 updated_at INTEGER NOT NULL DEFAULT 0, \
                 PRIMARY KEY (user_uuid7, name))",
                (),
            )
            .await
            .map_err(|e| e.to_string())?;
        // Older tables predate the identity columns; add only the missing ones.
        // Running an ALTER for a column that already exists makes turso 0.1.0
        // poison its schema cache so that later UPDATEs silently no-op (they
        // report rows-changed but store nothing) — so never run a doomed ALTER.
        let mut existing: HashSet<String> = HashSet::new();
        if let Ok(mut rows) = self.conn.query("PRAGMA table_info(characters)", ()).await {
            while let Ok(Some(r)) = rows.next().await {
                if let Ok(n) = r.get::<String>(1) {
                    existing.insert(n);
                }
            }
        }
        for (col, decl) in [
            ("color", "TEXT NOT NULL DEFAULT ''"),
            ("pronouns", "TEXT NOT NULL DEFAULT ''"),
            ("bio", "TEXT NOT NULL DEFAULT ''"),
            ("tags", "TEXT NOT NULL DEFAULT ''"),
            ("grp", "TEXT NOT NULL DEFAULT ''"),
            ("created_at", "INTEGER NOT NULL DEFAULT 0"),
            ("turns", "INTEGER NOT NULL DEFAULT 0"),
        ] {
            if !existing.contains(col) {
                let _ = self
                    .conn
                    .execute(&format!("ALTER TABLE characters ADD COLUMN {col} {decl}"), ())
                    .await;
            }
        }
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
        let Some(info) = self.character_info(user, name).await? else {
            return Err(format!("no character named `{name}`"));
        };
        self.save(user, name, &info, Some(avatar)).await
    }

    /// Persist a full row. turso 0.1.0's `UPDATE` silently fails to persist
    /// (it reports rows-changed but the stored value never changes), so every
    /// mutation is a read-modify-write via DELETE + INSERT — the same
    /// workaround `set_possession` uses.
    async fn save(&self, user: &str, name: &str, info: &CharacterInfo, avatar: Option<&[u8]>) -> Result<(), String> {
        self.delete_character(user, name).await?;
        let avatar = avatar.map(|b| turso::Value::Blob(b.to_vec())).unwrap_or(turso::Value::Null);
        self.conn
            .execute(
                "INSERT INTO characters \
                 (user_uuid7, name, avatar, color, pronouns, bio, tags, grp, created_at, turns, updated_at) \
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11)",
                turso::params![
                    vtext(user), vtext(name), avatar,
                    vtext(&info.color), vtext(&info.pronouns), vtext(&info.bio),
                    vtext(&info.tags), vtext(&info.group),
                    vint(info.created_at), vint(info.turns), vint(now_ms())
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

    /// A character's full identity record, or None if it doesn't exist here.
    async fn character_info(&self, user: &str, name: &str) -> Result<Option<CharacterInfo>, String> {
        let mut rows = self
            .conn
            .query(
                "SELECT color, pronouns, bio, tags, grp, created_at, turns, avatar \
                 FROM characters WHERE user_uuid7 = ?1 AND name = ?2",
                turso::params![vtext(user), vtext(name)],
            )
            .await
            .map_err(|e| e.to_string())?;
        let out = rows.next().await.map_err(|e| e.to_string())?.map(|r| CharacterInfo {
            name: name.to_string(),
            color: r.get::<String>(0).unwrap_or_default(),
            pronouns: r.get::<String>(1).unwrap_or_default(),
            bio: r.get::<String>(2).unwrap_or_default(),
            tags: r.get::<String>(3).unwrap_or_default(),
            group: r.get::<String>(4).unwrap_or_default(),
            created_at: r.get::<i64>(5).unwrap_or(0),
            turns: r.get::<i64>(6).unwrap_or(0),
            has_avatar: r.get::<Option<Vec<u8>>>(7).ok().flatten().map(|b| !b.is_empty()).unwrap_or(false),
        });
        while rows.next().await.map_err(|e| e.to_string())?.is_some() {}
        Ok(out)
    }

    /// Set one identity field; `field` is whitelisted → a column name. Returns
    /// whether the character exists in THIS store.
    async fn set_field(&self, user: &str, name: &str, field: &str, value: &str) -> Result<bool, String> {
        let col = match field {
            "color" | "colour" => "color",
            "pronouns" => "pronouns",
            "bio" => "bio",
            "tags" => "tags",
            "group" | "grp" => "grp",
            _ => return Ok(false),
        };
        let Some(mut info) = self.character_info(user, name).await? else {
            return Ok(false);
        };
        match col {
            "color" => info.color = value.to_string(),
            "pronouns" => info.pronouns = value.to_string(),
            "bio" => info.bio = value.to_string(),
            "tags" => info.tags = value.to_string(),
            _ => info.group = value.to_string(),
        }
        let avatar = self.get_avatar(user, name).await?;
        self.save(user, name, &info, avatar.as_deref()).await?;
        Ok(true)
    }

    /// Bump a character's turn counter (once per proxied message).
    async fn bump_turns(&self, user: &str, name: &str) {
        if let Ok(Some(mut info)) = self.character_info(user, name).await {
            info.turns += 1;
            let avatar = self.get_avatar(user, name).await.ok().flatten();
            let _ = self.save(user, name, &info, avatar.as_deref()).await;
        }
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
        // DELETE + INSERT rather than `INSERT OR REPLACE` (turso 0.1.0 does not
        // support the upsert clause). The primary key is (user, channel).
        self.conn
            .execute(
                "DELETE FROM possession WHERE user_uuid7 = ?1 AND channel_id = ?2",
                turso::params![vtext(user), vtext(channel)],
            )
            .await
            .map_err(|e| e.to_string())?;
        self.conn
            .execute(
                "INSERT INTO possession (user_uuid7, channel_id, char_name, started) \
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

/// A plain chat reply (command feedback) as an engine container.
fn reply_container(id: &EngineIdentity, chat: &ChatMessage, msg: String) -> ContainerForEngine {
    ContainerForEngine {
        version: 2,
        auth_token: id.auth.clone(),
        module_name: id.module.clone(),
        module_instance_uuid7: id.instance.clone(),
        payload: Some(EnginePayload::SendToPlatforms(base_send(id, chat, msg))),
    }
}

/// A PROXIED send (post `msg` as the character, then delete the trigger) as an
/// engine container.
#[allow(clippy::too_many_arguments)]
fn proxy_container(
    id: &EngineIdentity,
    chat: &ChatMessage,
    msg: String,
    char_key: String,
    display_name: String,
    avatar: Option<Vec<u8>>,
    delete_message_id: String,
) -> ContainerForEngine {
    let mut send = base_send(id, chat, msg);
    send.proxy = Some(ProxyIdentity {
        character_key: char_key,
        display_name,
        avatar: avatar.unwrap_or_default(),
    });
    send.delete_message_id = delete_message_id;
    ContainerForEngine {
        version: 2,
        auth_token: id.auth.clone(),
        module_name: id.module.clone(),
        module_instance_uuid7: id.instance.clone(),
        payload: Some(EnginePayload::SendToPlatforms(send)),
    }
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

/// Handle one `!rp` command, returning the engine messages to send (the
/// feedback replies). Async only for the store + attachment fetch, so the whole
/// command flow is drivable in a test.
/// Format epoch-ms as `YYYY-MM-DD` (UTC). 0/negative → "unknown".
fn fmt_date(ms: i64) -> String {
    if ms <= 0 {
        return "unknown".to_string();
    }
    let days = ms.div_euclid(86_400_000);
    let z = days + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = if m <= 2 { y + 1 } else { y };
    format!("{y:04}-{m:02}-{d:02}")
}

/// Read a character's stats from the `state` module's DB for this server,
/// grouped by category then key (both alphabetical). Best-effort: any error →
/// empty, so the sheet simply omits the stats section.
async fn read_state_stats(state_db_dir: &str, guild: &str, character: &str) -> Vec<(String, Vec<(String, String)>)> {
    let key = if guild.is_empty() { "_dm" } else { guild };
    let path = format!("{}/{key}.db", expand_tilde(state_db_dir).trim_end_matches('/'));
    if !std::path::Path::new(&path).exists() {
        return Vec::new();
    }
    let Ok(db) = turso::Builder::new_local(path.as_str()).build().await else {
        return Vec::new();
    };
    let Ok(conn) = db.connect() else {
        return Vec::new();
    };
    let Ok(mut rows) = conn
        .query(
            "SELECT category, skey, svalue FROM stats WHERE character = ?1 ORDER BY category ASC, skey ASC",
            turso::params![vtext(character)],
        )
        .await
    else {
        return Vec::new();
    };
    let mut out: Vec<(String, Vec<(String, String)>)> = Vec::new();
    while let Ok(Some(r)) = rows.next().await {
        let category = r.get::<String>(0).unwrap_or_default();
        let k = r.get::<String>(1).unwrap_or_default();
        let sv = r.get::<String>(2).unwrap_or_default();
        let val = serde_json::from_str::<serde_json::Value>(&sv)
            .map(|v| match v {
                serde_json::Value::String(s) => s,
                other => other.to_string(),
            })
            .unwrap_or(sv);
        match out.iter_mut().find(|(c, _)| *c == category) {
            Some((_, v)) => v.push((k, val)),
            None => out.push((category, vec![(k, val)])),
        }
    }
    out
}

/// Build a character's sheet: identity, usage counters, and its stats from the
/// `state` module (grouped by category, alphabetical).
async fn render_sheet(stores: &mut Stores, state_db_dir: &str, user: &str, name: &str, guild: &str) -> String {
    let mut info = None;
    let mut is_global = false;
    if let Ok(s) = stores.guild(guild).await {
        if let Ok(Some(i)) = s.character_info(user, name).await {
            info = Some(i);
        }
    }
    if info.is_none() {
        if let Ok(g) = stores.global_store().await {
            if let Ok(Some(i)) = g.character_info(user, name).await {
                info = Some(i);
                is_global = true;
            }
        }
    }
    let Some(info) = info else {
        return format!("No character named `{name}`.");
    };

    let mut lines: Vec<String> = Vec::new();
    let mut head = format!("**{}**", info.name);
    if !info.pronouns.is_empty() {
        head.push_str(&format!(" · {}", info.pronouns));
    }
    if !info.color.is_empty() {
        head.push_str(&format!(" · {}", info.color));
    }
    if !info.group.is_empty() {
        head.push_str(&format!(" · [{}]", info.group));
    }
    if is_global {
        head.push_str(" · global");
    }
    lines.push(head);
    if info.has_avatar {
        lines.push("avatar: set".to_string());
    }
    if !info.tags.is_empty() {
        lines.push(format!("tags: {}", info.tags));
    }
    if !info.bio.is_empty() {
        lines.push(info.bio.clone());
    }
    lines.push(format!("created {} · {} turn(s)", fmt_date(info.created_at), info.turns));

    for (category, items) in read_state_stats(state_db_dir, guild, name).await {
        let body = items.iter().map(|(k, v)| format!("`{k}`={v}")).collect::<Vec<_>>().join(" · ");
        lines.push(format!("— {category}: {body}"));
    }
    lines.join("\n")
}

async fn handle_command(
    id: &EngineIdentity,
    chat: &ChatMessage,
    stores: &mut Stores,
    sent_avatars: &mut HashSet<String>,
    guild: &str,
    state_db_dir: &str,
    cmd: RpCmd,
) -> Vec<ContainerForEngine> {
    let mut out: Vec<ContainerForEngine> = Vec::new();
    let user = chat.user_uuid7.clone();
    let channel = chat.channel_id.clone();

    // A macro (not a closure) so it does not borrow `out` for its lifetime —
    // the function returns `out` at the end.
    macro_rules! push {
        ($m:expr) => {
            out.push(reply_container(id, chat, $m))
        };
    }

    match cmd {
        RpCmd::Help => {
            push!(format!(
                "RP: `{f}rp possess <char>` · `{f}rp start`/`stop` · `{f}rp create <char>` (attach an image for the avatar) · `{f}rp <char> set color=… pronouns=… bio=… tags=… group=… global=true` · `{f}rp <char> sheet` · `{f}rp list` · `{f}rp <char> delete`",
                f = DEFAULT_FLAG
            ));
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
            push!(msg);
        }
        RpCmd::Create(name) => {
            if stores.character_exists_any(guild, &user, &name).await {
                push!(format!("You already have a character named `{name}`."));
                return out;
            }
            let (_, _, attach) = discord_meta(&chat.raw_data);
            let avatar = fetch_attachment(&attach).await;
            match stores.guild(guild).await {
                Ok(s) => match s.create_character(&user, &name, avatar.as_deref()).await {
                    Ok(()) => push!(match avatar {
                        Some(_) => format!("Created `{name}` with an avatar."),
                        None => format!("Created `{name}`. Add an avatar with `!rp {name} avatar` + an image."),
                    }),
                    Err(e) => push!(format!("Could not create `{name}`: {e}")),
                },
                Err(e) => push!(format!("Storage error: {e}")),
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
            push!(if done {
                format!("Deleted `{name}`.")
            } else {
                format!("No character named `{name}`.")
            });
        }
        RpCmd::SetAvatar { name } => {
            let (_, _, attach) = discord_meta(&chat.raw_data);
            let Some(avatar) = fetch_attachment(&attach).await else {
                push!(format!("Attach an image to set `{name}`'s avatar."));
                return out;
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
                push!(format!("Updated `{name}`'s avatar."));
            } else {
                push!(format!("No character named `{name}`."));
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
                        push!(if moved {
                            format!("`{name}` is now global.")
                        } else {
                            format!("No character named `{name}` here.")
                        });
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
                        push!(if moved {
                            format!("`{name}` is no longer global.")
                        } else {
                            format!("No global character named `{name}`.")
                        });
                    }
                    None => push!("`global` must be true or false.".to_string()),
                }
            } else if ["color", "colour", "pronouns", "bio", "tags", "group", "grp"].contains(&key.to_ascii_lowercase().as_str()) {
                let field = key.to_ascii_lowercase();
                let mut done = false;
                if let Ok(s) = stores.guild(guild).await {
                    if s.set_field(&user, &name, &field, &value).await.unwrap_or(false) {
                        done = true;
                    }
                }
                if !done {
                    if let Ok(g) = stores.global_store().await {
                        if g.set_field(&user, &name, &field, &value).await.unwrap_or(false) {
                            done = true;
                        }
                    }
                }
                push!(if done {
                    format!("Set `{field}` on `{name}`.")
                } else {
                    format!("No character named `{name}`.")
                });
            } else {
                push!(format!("Unknown setting `{key}`. Try: color, pronouns, bio, tags, group, global."));
            }
        }
        RpCmd::Sheet(name) => {
            push!(render_sheet(stores, state_db_dir, &user, &name, guild).await);
        }
        RpCmd::Possess(name) => {
            if !stores.character_exists_any(guild, &user, &name).await {
                push!(format!("No character named `{name}` — create it first."));
                return out;
            }
            // Keep the current started state; just swap the character.
            let started = possession_of(stores, guild, &user, &channel)
                .await
                .map(|(_, s)| s)
                .unwrap_or(false);
            if let Ok(s) = stores.guild(guild).await {
                let _ = s.set_possession(&user, &channel, &name, started).await;
            }
            push!(if started {
                format!("Now speaking as `{name}`.")
            } else {
                format!("Possessing `{name}`. `!rp start` to begin.")
            });
        }
        RpCmd::Start => {
            let possessed = possession_of(stores, guild, &user, &channel)
                .await
                .map(|(c, _)| c);
            let Some(name) = possessed else {
                push!("Pick a character first: `!rp possess <char>`.".to_string());
                return out;
            };
            if !stores.character_exists_any(guild, &user, &name).await {
                push!(format!("`{name}` no longer exists — pick another."));
                return out;
            }
            if let Ok(s) = stores.guild(guild).await {
                let _ = s.set_possession(&user, &channel, &name, true).await;
            }
            sent_avatars.clear();
            push!(format!("Started — your messages now post as `{name}`. `!rp stop` to end."));
        }
        RpCmd::Stop => {
            if let Some((name, _)) = possession_of(stores, guild, &user, &channel).await {
                if let Ok(s) = stores.guild(guild).await {
                    let _ = s.set_possession(&user, &channel, &name, false).await;
                }
            }
            push!("Stopped proxying. Your messages post normally again.".to_string());
        }
    }
    out
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
                            for c in handle_message(
                                &id, chat, &mut stores, &mut sent_avatars,
                                &settings.command_flag, &settings.state_db_dir,
                            )
                            .await
                            {
                                send_container(&write_for_task, c).await;
                            }
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
    id: &EngineIdentity,
    chat: &ChatMessage,
    stores: &mut Stores,
    sent_avatars: &mut HashSet<String>,
    command_flag: &str,
    state_db_dir: &str,
) -> Vec<ContainerForEngine> {
    if chat.platform != "discord" || chat.user_uuid7.is_empty() {
        return Vec::new();
    }
    let (msg_id, guild, _attach) = discord_meta(&chat.raw_data);
    let guild_key = if guild.is_empty() { DM_DB.to_string() } else { guild };
    let user = chat.user_uuid7.clone();
    let channel = chat.channel_id.clone();

    // A `!rp` command (parsed here — the module is a catch-all, not a command owner).
    if let Some(args) = parse_rp_args(&chat.raw_message, command_flag, RP_COMMAND) {
        if let Some(cmd) = parse_rp_command(&args) {
            return handle_command(id, chat, stores, sent_avatars, &guild_key, state_db_dir, cmd).await;
        }
        return Vec::new();
    }

    // Otherwise: proxy the message if the user is started here.
    let Some((char_name, started)) = possession_of(stores, &guild_key, &user, &channel).await else {
        return Vec::new();
    };
    if !started {
        return Vec::new();
    }
    let text = chat.raw_message.clone();
    if text.trim().is_empty() || is_spoiler_bypass(&text) {
        return Vec::new(); // nothing to post, or an intentional bypass
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
    // Count this as one turn the character has been used.
    if let Ok(s) = stores.guild(&guild_key).await {
        s.bump_turns(&user, &char_name).await;
    }
    vec![proxy_container(id, chat, text, key, char_name, avatar, msg_id)]
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

    #[test]
    fn fmt_date_renders_utc_ymd() {
        assert_eq!(fmt_date(0), "unknown");
        assert_eq!(fmt_date(-5), "unknown");
        assert_eq!(fmt_date(1), "1970-01-01");
        assert_eq!(fmt_date(1_609_459_200_000), "2021-01-01");
        assert_eq!(fmt_date(951_782_400_000), "2000-02-29"); // a leap day
    }

    // ── end-to-end: pretend to be a Discord chat ────────────────────────────

    /// A `ChatMessage` exactly as the discord-adapter emits it for a
    /// MESSAGE_CREATE (raw_data = the Discord JSON, which carries the id/guild).
    fn discord_msg(user: &str, channel: &str, guild: &str, msg_id: &str, text: &str) -> ChatMessage {
        let raw = format!(
            r#"{{"id":"{msg_id}","guild_id":"{guild}","channel_id":"{channel}","content":{}}}"#,
            serde_json::to_string(text).unwrap()
        );
        ChatMessage {
            platform: "discord".into(),
            raw_data: raw.into_bytes(),
            raw_message: text.into(),
            user_uuid7: user.into(),
            command: None,
            user_data: Some(UserData {
                username: user.into(),
                ..Default::default()
            }),
            channel_id: channel.into(),
        }
    }

    fn test_identity() -> EngineIdentity {
        EngineIdentity {
            auth: "t".into(),
            instance: "inst".into(),
            module: "rp".into(),
        }
    }

    /// The SendToPlatforms the module emitted (its outward effects).
    fn sends(v: &[ContainerForEngine]) -> Vec<SendToPlatforms> {
        v.iter()
            .filter_map(|c| match &c.payload {
                Some(EnginePayload::SendToPlatforms(s)) => Some(s.clone()),
                _ => None,
            })
            .collect()
    }

    /// Drive the WHOLE RP flow as a fake Discord chat would: create → possess →
    /// start → proxy → bypass → list → stop → global → delete, asserting each
    /// step's effect (the store state + the emitted sends).
    #[tokio::test]
    async fn rp_flow_start_to_finish() {
        let dir = std::env::temp_dir().join(format!("cockatiel-rp-{}", uuid::Uuid::now_v7()));
        let mut stores = Stores::new(dir.to_string_lossy().to_string());
        let mut sent: HashSet<String> = HashSet::new();
        let id = test_identity();
        let (user, channel, guild) = ("u1", "chan", "guild1");
        let mut n = 0;
        let mut say = |text: &str| {
            n += 1;
            discord_msg(user, channel, guild, &format!("m{n}"), text)
        };
        // 1. create
        let out = handle_message(&id, &say("!rp create Alice"), &mut stores, &mut sent, "!", "").await;
        assert_eq!(sends(&out).len(), 1);
        assert!(sends(&out)[0].msg.contains("Created `Alice`"), "{}", sends(&out)[0].msg);
        assert!(stores.character_exists_any(guild, user, "Alice").await);

        // 2. possess
        let out = handle_message(&id, &say("!rp possess Alice"), &mut stores, &mut sent, "!", "").await;
        assert!(sends(&out)[0].msg.contains("Possessing `Alice`"));
        assert_eq!(
            possession_of(&mut stores, guild, user, channel).await,
            Some(("Alice".to_string(), false))
        );

        // 3. a normal message while NOT started → nothing proxied
        assert!(handle_message(&id, &say("hello"), &mut stores, &mut sent, "!", "").await.is_empty());

        // 4. start
        let out = handle_message(&id, &say("!rp start"), &mut stores, &mut sent, "!", "").await;
        assert!(sends(&out)[0].msg.contains("Started"));
        assert_eq!(
            possession_of(&mut stores, guild, user, channel).await,
            Some(("Alice".to_string(), true))
        );

        // 5. a normal message while started → a PROXY send that deletes the trigger
        let out = handle_message(&id, &say("hello world"), &mut stores, &mut sent, "!", "").await;
        let s = sends(&out);
        assert_eq!(s.len(), 1);
        assert_eq!(s[0].msg, "hello world");
        let proxy = s[0].proxy.as_ref().expect("proxy identity");
        assert_eq!(proxy.display_name, "Alice");
        assert_eq!(proxy.character_key, "u1:Alice");
        assert_eq!(s[0].delete_message_id, "m5"); // the trigger message's id
        assert_eq!(s[0].platform, "discord");
        assert_eq!(s[0].channel_id, "chan");
        assert_eq!(s[0].actor_uuid7, "u1");

        // 6. a whole-message spoiler → bypassed (nothing sent)
        assert!(
            handle_message(&id, &say("||out of character||"), &mut stores, &mut sent, "!", "")
                .await
                .is_empty(),
            "a spoilered message posts normally, so we emit nothing"
        );

        // 7. list
        let out = handle_message(&id, &say("!rp list"), &mut stores, &mut sent, "!", "").await;
        assert!(sends(&out)[0].msg.contains("Alice"));

        // 8. stop → messages post normally again
        let out = handle_message(&id, &say("!rp stop"), &mut stores, &mut sent, "!", "").await;
        assert!(sends(&out)[0].msg.contains("Stopped"));
        assert!(handle_message(&id, &say("back to normal"), &mut stores, &mut sent, "!", "").await.is_empty());

        // 9. set global=true → the character moves to the global DB, still usable
        let out = handle_message(&id, &say("!rp Alice set global=true"), &mut stores, &mut sent, "!", "").await;
        assert!(sends(&out)[0].msg.contains("now global"));
        assert!(stores.character_exists_any(guild, user, "Alice").await);
        handle_message(&id, &say("!rp possess Alice"), &mut stores, &mut sent, "!", "").await;
        handle_message(&id, &say("!rp start"), &mut stores, &mut sent, "!", "").await;
        let out = handle_message(&id, &say("hi again"), &mut stores, &mut sent, "!", "").await;
        assert_eq!(sends(&out)[0].proxy.as_ref().unwrap().display_name, "Alice");

        // 10. delete
        handle_message(&id, &say("!rp stop"), &mut stores, &mut sent, "!", "").await;
        let out = handle_message(&id, &say("!rp delete Alice"), &mut stores, &mut sent, "!", "").await;
        assert!(sends(&out)[0].msg.contains("Deleted"));
        assert!(!stores.character_exists_any(guild, user, "Alice").await);

        // 11. commands are ignored for non-discord platforms
        let mut non = discord_msg(user, channel, guild, "z", "!rp start");
        non.platform = "twitch".into();
        assert!(handle_message(&id, &non, &mut stores, &mut sent, "!", "").await.is_empty());

        let _ = std::fs::remove_dir_all(&dir);
    }

    /// A fresh DB runs the migration check. If a doomed `ALTER` ran, turso
    /// 0.1.0 would poison its schema cache and this `UPDATE` would report
    /// rows-changed but store nothing.
    #[tokio::test]
    async fn migrations_do_not_poison_updates() {
        let dir = std::env::temp_dir().join(format!("cockatiel-rp-{}", uuid::Uuid::now_v7()));
        let mut stores = Stores::new(dir.to_string_lossy().to_string());
        let s = stores.guild("g").await.unwrap();
        s.create_character("u", "A", None).await.unwrap();
        s.conn
            .execute("UPDATE characters SET color='#abc' WHERE user_uuid7='u' AND name='A'", ())
            .await
            .unwrap();
        let mut rows = s
            .conn
            .query("SELECT color FROM characters WHERE user_uuid7='u' AND name='A'", ())
            .await
            .unwrap();
        let got = rows.next().await.unwrap().unwrap().get::<String>(0).unwrap();
        assert_eq!(got, "#abc", "UPDATE was poisoned by a failing ALTER");
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn set_field_roundtrip() {
        let dir = std::env::temp_dir().join(format!("cockatiel-rp-{}", uuid::Uuid::now_v7()));
        let mut stores = Stores::new(dir.to_string_lossy().to_string());
        let s = stores.guild("g").await.unwrap();
        s.create_character("u", "A", None).await.unwrap();
        let ok = s.set_field("u", "A", "color", "#f0f").await.unwrap();
        assert!(ok, "set_field returned false");
        let info = s.character_info("u", "A").await.unwrap().unwrap();
        assert_eq!(info.color, "#f0f");
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn identity_fields_and_sheet() {
        let dir = std::env::temp_dir().join(format!("cockatiel-rp-{}", uuid::Uuid::now_v7()));
        let mut stores = Stores::new(dir.to_string_lossy().to_string());
        let mut sent: HashSet<String> = HashSet::new();
        let id = test_identity();
        let (user, channel, guild) = ("u1", "chan", "guild1");
        let mut n = 0;
        let mut say = |text: &str| {
            n += 1;
            discord_msg(user, channel, guild, &format!("m{n}"), text)
        };
        handle_message(&id, &say("!rp create Alice"), &mut stores, &mut sent, "!", "").await;

        // Identity fields, including the `colour` alias.
        for line in [
            "!rp Alice set color=#f0f",
            "!rp Alice set pronouns=she/her",
            "!rp Alice set bio=A wanderer.",
            "!rp Alice set tags=brave, kind",
            "!rp Alice set group=Party",
        ] {
            let out = handle_message(&id, &say(line), &mut stores, &mut sent, "!", "").await;
            assert!(sends(&out)[0].msg.contains("Set"), "{line} -> {}", sends(&out)[0].msg);
        }

        // The sheet renders them (both `Alice sheet` and `sheet Alice` forms).
        for line in ["!rp Alice sheet", "!rp sheet Alice"] {
            let out = handle_message(&id, &say(line), &mut stores, &mut sent, "!", "").await;
            let msg = &sends(&out)[0].msg;
            assert!(
                msg.contains("Alice")
                    && msg.contains("she/her")
                    && msg.contains("#f0f")
                    && msg.contains("A wanderer.")
                    && msg.contains("Party"),
                "{line} -> {msg}"
            );
        }

        // Missing character: reported, never a panic.
        let out = handle_message(&id, &say("!rp Ghost set bio=x"), &mut stores, &mut sent, "!", "").await;
        assert!(sends(&out)[0].msg.contains("No character"));
        let out = handle_message(&id, &say("!rp Ghost sheet"), &mut stores, &mut sent, "!", "").await;
        assert!(sends(&out)[0].msg.contains("No character"));

        // An unknown setting key suggests the valid ones.
        let out = handle_message(&id, &say("!rp Alice set nope=1"), &mut stores, &mut sent, "!", "").await;
        assert!(sends(&out)[0].msg.contains("Unknown setting"), "{}", sends(&out)[0].msg);

        let _ = std::fs::remove_dir_all(&dir);
    }
}
