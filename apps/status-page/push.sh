#!/bin/sh
# Publish status.json as the single commit of branch "data" (force push).
set -eu

REMOTE_URL=${REMOTE_URL:-git@ssh.github.com:The-EPISERVE-Consortium/episerve-status.git}
WORK_DIR=${WORK_DIR:-/tmp/push}
STATUS_FILE=${STATUS_FILE:-/work/status.json}

if [ ! -s "$STATUS_FILE" ]; then
  echo "push.sh: $STATUS_FILE is missing or empty" >&2
  exit 1
fi

if [ -n "${SSH_KEY_SOURCE:-}" ]; then
  mkdir -p "$HOME/.ssh"
  chmod 700 "$HOME/.ssh"
  cp "$SSH_KEY_SOURCE" "$HOME/.ssh/id_ed25519"
  chmod 600 "$HOME/.ssh/id_ed25519"
fi

mkdir -p "$WORK_DIR"
rm -rf "$WORK_DIR/.git"
cd "$WORK_DIR"
git init -q
git checkout -q --orphan data
cp "$STATUS_FILE" status.json
git add status.json
git -c user.name="EPISERVE status bot" -c user.email=status-bot@users.noreply.github.com \
  commit -q -m "Update status"
git push -q -f "$REMOTE_URL" data:data
