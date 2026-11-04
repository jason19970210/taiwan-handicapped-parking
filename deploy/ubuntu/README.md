# Google Maps list sync on ubuntu

`scripts/main.py sync-lists` keeps 33 Google Maps saved lists (one per district, names in
`data/lists/index.json`) in line with `data/lists`. It drives Google Chrome on a dedicated profile that
was logged in by hand. That profile directory is the only credential: there is no email, password,
secret or token in this repo, in CI, or in any file the sync reads.

What it will and will not do:

- Adds and removes single places on the 33 lists named in `index.json`. Nothing else.
- Never deletes a list. Never opens, edits or deletes any other list (the old big list, Favorites, ...).
- Stops on a sign-in page, captcha, "unusual traffic" or consent page (exit 3) and writes nothing then.
- State and log live outside the repo: `~/.local/state/thp-maps/sync_state.json` and `sync.log`.

## Install (once)

```bash
sudo apt install xvfb                        # the sync runs headed, under a virtual display
# Google Chrome (not Chromium) must be installed: the sync launches channel="chrome"

git clone https://github.com/<owner>/taiwan-handicapped-parking.git ~/taiwan-handicapped-parking
cd ~/taiwan-handicapped-parking
python3 -m venv .venv
.venv/bin/pip install -r requirements.txt    # no "playwright install": Chrome is used, not a downloaded browser
```

The clone uses https and no credentials (public repo); the timer pulls with `git pull --ff-only`.

Check the plan without a browser (nothing is written):

```bash
.venv/bin/python scripts/main.py sync-lists --dry-run
# sync-lists dry-run: 33 lists / 2609 adds / 0 removes (0 unchanged)   (numbers follow the data)
```

Log in once with the relogin procedure below, then:

```bash
mkdir -p ~/.config/systemd/user ~/.config/thp-maps
cp deploy/ubuntu/thp-maps-sync.service deploy/ubuntu/thp-maps-sync.timer ~/.config/systemd/user/
# optional alert on exit 3 or 4: one line, a command that takes the message as its last argument
#   echo 'THP_ALERT_CMD=/path/to/notify-script' > ~/.config/thp-maps/env
systemctl --user daemon-reload
systemctl --user enable --now thp-maps-sync.timer
loginctl enable-linger "$USER"              # without linger, user timers do not fire while logged out
systemctl --user list-timers thp-maps-sync.timer
```

The service assumes the clone is at `~/taiwan-handicapped-parking`; edit `WorkingDirectory=` and
`ExecStart=` in the copied unit if it is elsewhere. Schedule: daily 10:17 Asia/Taipei,
`Persistent=true` (a missed run starts when the host is back).

Chrome must not be running on the profile (`~/.config/thp-maps-profile`) when the sync starts, and the
sync must not run while you do the relogin: Chrome locks the profile.

## Capacity test (first live run, CTO or CEO at the keyboard)

```bash
cd ~/taiwan-handicapped-parking
xvfb-run -a -s "-screen 0 1440x900x24" .venv/bin/python scripts/main.py sync-lists \
    --only "臺北市 中山區" --max-ops 230
xvfb-run -a .venv/bin/python scripts/main.py sync-lists --count "臺北市 中山區"
```

`--count` opens the Save menu of the list's first place and reads the count Google shows in the list's
row ("Private · 220 places"); it ticks nothing. It prints that number next to the number of **places** in the
state file (and the number of ids). Ids that share one lat/lon are one Google place, so compare with
places, not ids. A Google number below the places number means two different coordinates resolved to
the same Google place.

Failed ops leave a screenshot in `~/.local/state/thp-maps/fail/` (first 5 per run) and a `fail` line in
`sync.log`. Selectors that need a fix are named in the log reason, for example
`save button not found (SAVE_BUTTON)`; all of them are constants in
`scripts/google_maps/selectors.py`.

## Daily run

`thp-maps-sync.sh` does `git pull --ff-only` (aborts with exit 1 if it fails), then
`xvfb-run -a .venv/bin/python scripts/main.py sync-lists` (default `--max-ops 300`; the rest continues
the next day), then on exit 3 or 4 runs `$THP_ALERT_CMD "<one line>"` if that variable is set.

| Exit | Meaning | Timer state |
| --- | --- | --- |
| 0 | done, or nothing to do | success |
| 1 | other error (git pull failed, profile in use, bad arguments) | failed |
| 2 | partial: `--max-ops` reached or some ops failed; they are planned again next run | success |
| 3 | relogin needed or blocked; nothing was written by that run beyond ops already verified | failed + alert |
| 4 | 3 consecutive op failures (selectors changed?) | failed + alert |

```bash
systemctl --user start thp-maps-sync.service   # run now
journalctl --user -u thp-maps-sync.service -n 100
tail -n 20 ~/.local/state/thp-maps/sync.log    # JSON lines: wrapper, intent, op, summary
```

`sync.log` alone reconstructs a run. Every line has `ts` and `run_id`; the wrapper passes its own
run id so its lines and the sync lines match. Events:

- `wrapper`: `stage` (`start`, `args`, `pull`, `exit`, `alert`), `rc`, `detail`. A failed `git pull` shows here;
  `args` rc 1 means a `--state` argument was refused (the state directory comes only from `THP_STATE_DIR`).
- `start`: planned Google ops and ids, places to add, how many place groups hold more than one id.
- `intent`: written before an op is sent to the browser: `list`, `place` (lat,lon at 6 decimals),
  `op` (`ensure`, `add`, `remove`, `forget`), `ids`.
- `op`: the outcome: `list`, `place`, `point_id`, `op` (`session`, `ensure`, `add`, `remove`,
  `forget`, `count`), `result`, `ms`, `reason`.
- `skipped`: entities not processed, with the reason (list that could not be created, max-ops
  reached, state point without coordinates).
- `state_write_failed`: a verified Google op whose state write failed (the run stops, exit 1).
- `summary`: `planned, done, failed, remaining` (Google ops), `ids_planned, ids_done`, `exit, reason`.

Several point ids can share one Google place (same lat/lon at 6 decimals). The sync works per place:
it ticks the place once, and unticks it only when no id of the list is left there. `--count` therefore
compares Google with the number of **places** in the state, not ids; `--dry-run` prints how many
groups hold more than one id.

## Relogin procedure (exit 3, or first login)

Google asks for a new login now and then; 2-step verification is done by you, in a real Chrome. Run
every command yourself; nothing here is automated.

1. On ubuntu, stop the timer so no sync starts meanwhile, and start Chrome on the profile in a virtual
   display with the DevTools port on loopback only:

   ```bash
   systemctl --user stop thp-maps-sync.timer
   Xvfb :99 -screen 0 1440x900x24 &
   DISPLAY=:99 google-chrome --user-data-dir="$HOME/.config/thp-maps-profile" \
       --password-store=basic --remote-debugging-port=9222 --remote-debugging-address=127.0.0.1 \
       https://accounts.google.com/ &
   ```

   `--password-store=basic` matters: there is no keyring on this host, and cookies written with another
   store would be unreadable by the sync (which starts Chrome with the same flag).

2. From your Mac, forward the port and open the page:

   ```bash
   ssh -N -L 9222:127.0.0.1:9222 <your-ubuntu-host>
   ```

   In Chrome on the Mac open `chrome://inspect/#devices`, "Configure...", add `localhost:9222`, then
   "inspect" the Google sign-in page. The DevTools window shows the remote page; click and type in it.

3. Sign in with your account (password and 2-step verification). Open `https://www.google.com/maps` in
   that tab and check your account picture shows in the top right.

4. Close Chrome so the profile is written to disk. On ubuntu:

   ```bash
   cd ~/taiwan-handicapped-parking
   .venv/bin/python -c "from playwright.sync_api import sync_playwright as s; p=s().start(); \
   b=p.chromium.connect_over_cdp('http://127.0.0.1:9222'); b.new_browser_cdp_session().send('Browser.close')"
   pkill Xvfb; pgrep -fa thp-maps-profile || echo "chrome is gone"
   ```

   Stop the `ssh -N -L` on the Mac (Ctrl-C).

5. Test and restart the timer:

   ```bash
   xvfb-run -a .venv/bin/python scripts/main.py sync-lists --dry-run     # plan only
   xvfb-run -a .venv/bin/python scripts/main.py sync-lists --count "臺北市 中山區"   # session check + read
   systemctl --user start thp-maps-sync.timer
   ```

   Exit 3 on the count means the login did not take; repeat from step 1.
