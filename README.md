# cockatiel_module-rp-rs

Discord **roleplay "possess"** proxying for Cockatiel — a game-like, Tupperbox-style
system where users create **characters** and post messages *as* them.

## How it works

- Users create characters (a name + avatar).
- `!rp possess <character>` chooses which character you control.
- `!rp start` enters **proxy mode**: from then on, every message you send is
  **deleted** and **re-posted as the possessed character** (via a Discord webhook
  whose name/avatar carry the character).
- `!rp possess <other>` swaps characters mid-session; `!rp stop` ends it.
- A message whose whole text is wrapped in `||spoiler||` is posted normally
  (a bypass). `>` is reserved for future status/asides.

## Commands

| Command | Effect |
|---|---|
| `!rp possess <char>` | control `<char>` |
| `!rp start` / `!rp stop` | proxy mode on/off |
| `!rp create <char>` | create a character (attach an image to set its avatar) |
| `!rp <char> avatar` | set/update `<char>`'s avatar (attach an image) |
| `!rp <char> set <key>=<value>` | e.g. `!rp alice set global=true` |
| `!rp list` | list your characters |
| `!rp <char> delete` | delete a character |
| `!rp help` | usage |

## Storage

One `turso` (SQLite) database per **server** (`<db_dir>/<guild_id>.db`) holding that
server's characters and possession state, plus a `<db_dir>/_global.db` for
characters lifted to global scope (`global=true`). IDs are **UUID7**. The DB files
are gitignored.

## Requirements

- The Discord bot needs **Manage Webhooks** (to post as the character) and
  **Manage Messages** (to delete the trigger message).
- Discord only (webhooks are the mechanism).

## Config (`module_specific`)

- `command_flag` — the command sigil (default `!`).
- `db_dir` — where the per-server DB files live (default `~/.cockatiel/rp`).
- `reconnect_base_secs` / `reconnect_max_secs` — engine reconnect backoff.
