#!/usr/bin/env bash
# delegate setup: build the CLI, install on PATH, migrate host profile, install the skill.
#   DRY_RUN=1 ./bin/setup.sh   # preview commands without making changes
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
DRY_RUN="${DRY_RUN:-0}"

run() {
  if [ "$DRY_RUN" = "1" ]; then
    echo "DRY_RUN: $*"
  else
    "$@"
  fi
}

if ! command -v cargo >/dev/null 2>&1; then
  echo "ERROR: cargo not found on PATH. Install Rust (https://rustup.rs) first." >&2
  exit 1
fi

if ! command -v cursor-agent >/dev/null 2>&1; then
  echo "WARNING: cursor-agent not on PATH. Install it and run 'cursor-agent login' before write jobs." >&2
fi

TARGET_DIR="${XDG_CACHE_HOME:-$HOME/.cache}/delegate/target"
run cargo build --release --manifest-path "$REPO_ROOT/Cargo.toml" --target-dir "$TARGET_DIR"

INSTALL_BIN="$HOME/.local/bin/delegate"
run mkdir -p "$(dirname "$INSTALL_BIN")"
# Copy then rename: overwriting a running binary in place can get its processes
# killed on macOS, and running jobs keep the old file until they exit.
run cp "$TARGET_DIR/release/delegate" "$INSTALL_BIN.new"
run mv -f "$INSTALL_BIN.new" "$INSTALL_BIN"

case ":${PATH}:" in
  *":$HOME/.local/bin:"*) ;;
  *)
    echo "WARNING: $HOME/.local/bin is not on PATH. Add it to your shell profile." >&2
    ;;
esac

CONFIG_HOME="${XDG_CONFIG_HOME:-$HOME/.config}"
OLD_PROFILE="$CONFIG_HOME/cursor-delegate/host-profile.json"
NEW_PROFILE="$CONFIG_HOME/delegate/host-profile.json"
if [ -f "$NEW_PROFILE" ] && [ -f "$OLD_PROFILE" ]; then
  echo "NOTE: host profile exists at both $OLD_PROFILE and $NEW_PROFILE; leaving both untouched."
elif [ ! -f "$NEW_PROFILE" ] && [ -f "$OLD_PROFILE" ]; then
  echo "Migrating host profile to $NEW_PROFILE"
  run mkdir -p "$(dirname "$NEW_PROFILE")"
  run mv "$OLD_PROFILE" "$NEW_PROFILE"
  run rmdir "$CONFIG_HOME/cursor-delegate" 2>/dev/null || true
fi

# cursor-agent signs commits ("Co-authored-by: Cursor") and PR bodies ("Made with
# [Cursor]") unless its user config says not to; it has no per-run flag for this.
CURSOR_CONFIG="$HOME/.cursor/cli-config.json"
if [ -f "$CURSOR_CONFIG" ]; then
  if command -v jq >/dev/null 2>&1; then
    run sh -c 'jq ".attribution = {attributeCommitsToAgent: false, attributePRsToAgent: false}" "$1" > "$1.delegate.tmp" && mv -f "$1.delegate.tmp" "$1"' _ "$CURSOR_CONFIG"
  else
    echo "WARNING: jq not found; set attribution.attributeCommitsToAgent and attributePRsToAgent to false in $CURSOR_CONFIG" >&2
  fi
fi

# Both harnesses load the skill from this checkout, so an edit is live
# without a reinstall. A plugin install would copy the whole checkout,
# untracked files included, into Claude's plugin cache.
SKILL_DIR="$REPO_ROOT/skills/delegate"
run mkdir -p "$HOME/.claude/skills"
run ln -sfn "$SKILL_DIR" "$HOME/.claude/skills/delegate"
if command -v claude >/dev/null 2>&1 && claude plugin list 2>/dev/null | grep -q 'delegate@delegate'; then
  echo "NOTE: the delegate plugin duplicates the linked skill; remove it with: claude plugin uninstall delegate@delegate && claude plugin marketplace remove delegate"
fi

if command -v pi >/dev/null 2>&1; then
  run pi install "$REPO_ROOT"
else
  echo "NOTE: 'pi' not found. For pi, run: pi install $REPO_ROOT"
fi

echo "Done."
