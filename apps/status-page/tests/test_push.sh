#!/bin/sh
# Test for push.sh. Run: sh tests/test_push.sh
set -eu
here=$(cd "$(dirname "$0")/.." && pwd)
T=$(mktemp -d)
trap 'rm -rf "$T"' EXIT
fail() { echo "FAIL: $*" >&2; exit 1; }

git init -q --bare "$T/remote.git"
export HOME="$T/home"; mkdir -p "$HOME"
export REMOTE_URL="file://$T/remote.git"
export WORK_DIR="$T/work"
export STATUS_FILE="$T/status.json"

echo '{"v":1}' > "$STATUS_FILE"
sh "$here/push.sh"
echo '{"v":2}' > "$STATUS_FILE"
sh "$here/push.sh"

[ "$(git -C "$T/remote.git" rev-list --count data)" = 1 ] || fail "commit count"
[ "$(git -C "$T/remote.git" show data:status.json)" = '{"v":2}' ] || fail "content"
[ "$(git -C "$T/remote.git" log -1 --format='%an <%ae>|%s' data)" = \
  'EPISERVE status bot <status-bot@users.noreply.github.com>|Update status' ] || fail "author/subject"
msg=$(git -C "$T/remote.git" log -1 --format=%B data)
case "$msg" in *Co-Authored*|*Claude*) fail "message has attribution";; esac

# SSH_KEY_SOURCE copy
echo "KEY" > "$T/key"; chmod 640 "$T/key"
SSH_KEY_SOURCE="$T/key" sh "$here/push.sh"
[ -f "$HOME/.ssh/id_ed25519" ] || fail "key not copied"
[ "$(stat -c %a "$HOME/.ssh/id_ed25519")" = 600 ] || fail "key mode"
[ "$(stat -c %a "$HOME/.ssh")" = 700 ] || fail ".ssh mode"

# missing / empty status file: non-zero, nothing pushed
before=$(git -C "$T/remote.git" rev-parse data)
rm "$STATUS_FILE"
if sh "$here/push.sh" 2>/dev/null; then fail "missing status file succeeded"; fi
: > "$STATUS_FILE"
if sh "$here/push.sh" 2>/dev/null; then fail "empty status file succeeded"; fi
[ "$(git -C "$T/remote.git" rev-parse data)" = "$before" ] || fail "remote changed"

echo "push ok"
