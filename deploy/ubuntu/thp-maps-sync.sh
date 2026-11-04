#!/usr/bin/env bash
# Daily Google Maps list sync: update the repo, run sync-lists under Xvfb, alert on exit 3 or 4.
# Extra arguments are passed to `sync-lists` (e.g. --dry-run, --only "臺北市 中山區").
#
# Everything this wrapper does is also appended to sync.log as JSON lines (event "wrapper":
# start, pull result, child stderr, sync exit code, alert), with the same run_id as the sync-lists
# lines, so sync.log alone tells the whole story of a run. The child's stderr (Xvfb start failure,
# import error, bad argument) goes to $STATE_DIR/last_stderr.log, overwritten each run and also copied
# to this script's stderr for the journal; when the child fails, its last 5 lines are logged as stage "stderr".
#
# THP_STATE_DIR (optional): directory of sync.log and the state file, default ~/.local/state/thp-maps.
# Both the wrapper and sync-lists use it (the wrapper passes --state); a --state argument is refused.
# THP_ALERT_CMD (optional): a command that takes one message argument, for example
# "/path/to/notify-script". Unset or empty = no alert. It is split on spaces, not run through a shell.
set -u -o pipefail

REPO="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
STATE_DIR="${THP_STATE_DIR:-$HOME/.local/state/thp-maps}"
mkdir -p "$STATE_DIR" 2>/dev/null || true
# absolute before the first log line, so a relative THP_STATE_DIR means the same directory before and after the cd below
STATE_DIR="$(cd "$STATE_DIR" 2>/dev/null && pwd)" || { echo "thp-maps-sync: cannot use state dir ${THP_STATE_DIR:-}" >&2; exit 1; }
LOG="$STATE_DIR/sync.log"
RUN_ID="$(date -u +%Y%m%dT%H%M%SZ)-$$"

# wlog STAGE RC DETAIL  -> one JSON line in sync.log (and on stderr for the journal)
wlog() {
    local detail
    detail="$(printf '%s' "$3" | tr '\n\t' '  ')"
    local line
    # cut to 500 characters before JSON encoding, so a record is always valid JSON (UTF-8 safe)
    line="$(python3 -I -c 'import json, sys; ts, run, st, rc, d = sys.argv[1:]
d = d.encode("utf-8", "surrogateescape").decode("utf-8", "replace")   # a byte cut (tail -c) may split a character
print(json.dumps({"ts": ts, "run_id": run, "event": "wrapper", "stage": st, "rc": int(rc), "detail": d[:500]}, ensure_ascii=False))' \
        "$(date -u +%Y-%m-%dT%H:%M:%S+00:00)" "$RUN_ID" "$1" "$2" "$detail" 2>/dev/null)" ||
        line="$(printf '{"ts": "%s", "run_id": "%s", "event": "wrapper", "stage": "%s", "rc": %s, "detail": "python3 failed to encode detail"}' \
            "$(date -u +%Y-%m-%dT%H:%M:%S+00:00)" "$RUN_ID" "$1" "$2")"
    printf '%s\n' "$line" >> "$LOG" 2>/dev/null || echo "thp-maps-sync: cannot write $LOG" >&2
    printf '%s\n' "$line" >&2
}

wlog start 0 "repo=$REPO host=$(hostname) args=$*"

# The state file must sit next to this sync.log, so its location comes only from THP_STATE_DIR.
for a in "$@"; do
    case "$a" in
        --state|--state=*) wlog args 1 "--state is not accepted here; set THP_STATE_DIR"; wlog exit 1 "aborted before sync"; exit 1 ;;
    esac
done

cd "$REPO" || { wlog cd 1 "cannot cd to $REPO"; exit 1; }

pull_out="$(git -C "$REPO" pull --ff-only 2>&1)"
pull_rc=$?
if [ "$pull_rc" -ne 0 ]; then
    wlog pull "$pull_rc" "git pull --ff-only failed, not syncing: $(printf '%s' "$pull_out" | tail -n 3)"
    wlog exit 1 "aborted before sync"
    exit 1
fi
wlog pull 0 "$(printf '%s' "$pull_out" | tail -n 1) head=$(git -C "$REPO" rev-parse --short HEAD 2>/dev/null)"

ERR_FILE="$STATE_DIR/last_stderr.log"
if ! : > "$ERR_FILE" 2>/dev/null; then
    wlog capture 1 "cannot write $ERR_FILE, not syncing"
    wlog exit 1 "aborted before sync"
    exit 1
fi
xvfb-run -a -s "-screen 0 1440x900x24" "$REPO/.venv/bin/python" "$REPO/scripts/main.py" sync-lists \
    --state "$STATE_DIR/sync_state.json" --run-id "$RUN_ID" "$@" 2> "$ERR_FILE"
rc=$?
cat "$ERR_FILE" >&2 2>/dev/null
# only a failing child with something on stderr gets a line; exit 3 and 4 normally print nothing there
if [ "$rc" -ne 0 ] && [ -s "$ERR_FILE" ]; then
    wlog stderr "$rc" "$(tail -n 5 "$ERR_FILE" | tail -c 480)"   # the end holds the exception line; wlog cuts at 500 chars
fi
wlog exit "$rc" "sync-lists finished"

if [ "$rc" -eq 3 ] || [ "$rc" -eq 4 ]; then
    case "$rc" in
        3) why="relogin needed or blocked (redo the Chrome login, see deploy/ubuntu/README.md)" ;;
        4) why="3 consecutive op failures (selectors may have changed)" ;;
    esac
    msg="thp-maps-sync exit $rc on $(hostname): $why. Log: $LOG"
    if [ -n "${THP_ALERT_CMD:-}" ]; then
        read -r -a alert_cmd <<< "$THP_ALERT_CMD"
        if "${alert_cmd[@]}" "$msg"; then
            wlog alert 0 "alert sent"
        else
            wlog alert 1 "alert command failed"
        fi
    else
        wlog alert 0 "THP_ALERT_CMD unset, no alert sent"
    fi
fi
exit "$rc"
