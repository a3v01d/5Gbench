# 5gbench — 5G Tri-Carrier Drive-Test Benchmarking Tool

GPS-tagged drive testing that simultaneously compares signal strength, throughput,
latency, and 5G availability across three cellular carriers (AT&T, T-Mobile,
Verizon) using three Quectel RM520N-GL 5G modems connected via USB to a single
Linux laptop.

**Intended operators:** City of Fontana IT staff.  
**Output consumers:** IT division, GIS team (ArcGIS/QGIS), Police Department, Fire Department, city leadership.

---

## Hardware Requirements

| Item | Qty | Notes |
|------|-----|-------|
| Quectel RM520N-GL M.2 5G modem in USB enclosure | 3 | One per carrier |
| Taoglas MA352 antenna (2×2 MIMO + GNSS) | 3 | Roof-mounted |
| Powered USB hub | 1 | All three modems connect through it |
| Linux laptop | 1 | Debian 13+ or Fedora 40+ |
| SIM cards | 3 | AT&T, T-Mobile, Verizon |

Each modem's USB hub exposes multiple `/dev/ttyUSBx` AT-command ports and one
network interface (`wwan0`, `wwan1`, `wwan2`).

---

## System Dependencies

Install before running 5gbench:

**Debian/Ubuntu:**
```bash
sudo apt install modemmanager iperf3 iproute2 python3 python3-pip python3-venv
```

**Fedora:**
```bash
sudo dnf install ModemManager iperf3 iproute python3 python3-pip
```

> **Root required.** Network namespace operations (`ip netns`) require root or
> `CAP_NET_ADMIN`. Run as root or configure systemd with the appropriate
> capabilities.

---

## Installation

```bash
git clone https://github.com/a3v01d/5gbench.git
cd 5gbench

python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
pip install -e .

# Create config directory and copy example config
mkdir -p ~/.config/5gbench/logs
cp config.example.toml ~/.config/5gbench/config.toml
```

---

## Configuration

Edit `~/.config/5gbench/config.toml`. **Required before first run:**

1. Set each modem's `imei` — find with: `mmcli -m 0 | grep imei` (repeat for `-m 1`, `-m 2`)
2. Verify each modem's `apn` for your carrier SIM
3. Set `throughput.iperf3_server` to a reachable iperf3 server

```toml
[[modems]]
imei = "860123456789001"   # ← your actual IMEI
carrier = "att"
label = "AT&T"
apn = "broadband"

[[modems]]
imei = "860123456789002"
carrier = "tmobile"
label = "T-Mobile"
apn = "fast.t-mobile.com"

[[modems]]
imei = "860123456789003"
carrier = "verizon"
label = "Verizon"
apn = "vzwinternet"

[throughput]
iperf3_server = "your.iperf3.server.com"   # ← required
```

All other settings have sensible defaults. See `config.example.toml` for the
full annotated reference.

---

## Usage

### Pre-flight check

Run this before every field deployment to validate hardware:

```bash
sudo 5gbench preflight
```

Output example:
```
5gbench preflight check
────────────────────────────────────────────────────────────
  ✅ PASS  Config file valid                        3 modem(s) defined
  ✅ PASS  Disk space in db_path                   47.3 GB free
  ✅ PASS  System tools available                  All tools found
  ✅ PASS  Modem USB detection                     Found 3/3 modems
  ✅ PASS  AT port access                          All 3 AT ports respond OK
  ✅ PASS  IMEI match                              All IMEIs match config
  ✅ PASS  SIM cards present                       ICCIDs: ...1234, ...5678, ...9012
  ✅ PASS  Network registration                    att:'AT&T'; tmobile:'T-Mobile'; verizon:'Verizon'
  ✅ PASS  ModemManager connected                  att:connected; tmobile:connected; verizon:connected
  ✅ PASS  Namespace operations                    Namespace create/delete OK
  ✅ PASS  GNSS fix status                         Fix: 3d, sats=12
  ✅ PASS  iperf3 server reachable                 Reachable from: att, tmobile, verizon
────────────────────────────────────────────────────────────
  Results: 12/12 checks passed
  All checks passed. Ready to run 5gbench.
```

### Start a collection session (TUI mode)

```bash
sudo 5gbench start
```

You will be prompted for session metadata (operator, vehicle ID, route). The
live TUI dashboard then shows all three carriers updating in real time.

**TUI hotkeys:**

| Key | Action |
|-----|--------|
| `S` | Start new session |
| `P` | Pause current session |
| `R` | Resume paused session |
| `T` | Trigger on-demand throughput test |
| `L` | Trigger on-demand latency test |
| `Q` | Stop session and quit |

### Headless / quiet mode

```bash
sudo 5gbench start --quiet
```

Uses config defaults without prompting. Suitable for systemd operation.

### Stop a running instance

```bash
sudo 5gbench stop
```

Sends SIGTERM to the running process (via PID file).

---

## Data Output

All data is stored in SQLite databases at `~/.config/5gbench/` (configurable):

```
~/.config/5gbench/
├── 5gbench_20260410.db      ← today's data
├── 5gbench_20260411.db      ← tomorrow (created automatically)
└── logs/
    └── 5gbench_20260410.log
```

**File naming:** `5gbench_YYYYMMDD.db` — one file per calendar day.  
**Sessions:** Multiple sessions per file, distinguished by `session_id`.

### Database tables

| Table | Contents |
|-------|----------|
| `sessions` | Session metadata, start/end times, config snapshot |
| `rf_telemetry` | Per-modem RF metrics: RSRP, RSRQ, SINR, band, tech |
| `gnss` | GPS fixes: lat/lon, altitude, speed, fix quality |
| `neighbor_cells` | Neighboring cell scan results |
| `throughput_results` | iperf3 / HTTP download and upload speeds |
| `latency_results` | ICMP, TCP, and HTTP HEAD latency measurements |
| `modem_events` | Connect/disconnect/GNSS failover events |

### GIS export

Use a separate post-processing tool to join RF telemetry with GPS coordinates
and export to GeoJSON/KML/CSV for ArcGIS/QGIS. The join query:

```sql
SELECT rf.*, g.latitude, g.longitude, g.altitude, g.hdop, g.fix_type
FROM rf_telemetry rf
LEFT JOIN gnss g ON g.session_id = rf.session_id
  AND g.timestamp = (
    SELECT MAX(g2.timestamp) FROM gnss g2
    WHERE g2.session_id = rf.session_id
      AND g2.timestamp <= rf.timestamp
  )
WHERE rf.session_id = '20260410_143022';
```

---

## Network Architecture

Each carrier runs in its own **Linux network namespace** (`ns_att`,
`ns_tmobile`, `ns_verizon`). This guarantees that iperf3, ping, and curl
traffic egresses through the correct modem interface — zero cross-carrier
bleed.

```
AT&T modem  → wwan0 → ns_att    → 8.8.8.8, 1.1.1.1 (DNS)
T-Mobile    → wwan1 → ns_tmobile → 8.8.8.8, 1.1.1.1
Verizon     → wwan2 → ns_verizon → 8.8.8.8, 1.1.1.1
```

**ModemManager** owns the cellular data connection (APN, registration,
reconnection). The tool takes over the interface after MM establishes it.

---

## Systemd (Headless)

Install the service unit for persistent headless operation:

```bash
# Edit the ExecStart path to match your installation
sudo cp systemd/5gbench.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable 5gbench
sudo systemctl start 5gbench

# Check status
sudo systemctl status 5gbench
sudo journalctl -u 5gbench -f
```

---

## Phased Implementation

| Phase | Status | Contents |
|-------|--------|----------|
| 1 — Foundation | ✅ Complete | Config, event bus, modem discovery, AT serial, AT parsers, pre-flight |
| 2 — Data Storage | ✅ Complete | SQLite writer, session management |
| 3 — Network & RF | ✅ Complete | Namespace management, ModemManager, RF and GNSS collectors |
| 4 — Active Testing | ✅ Complete | Throughput (iperf3/HTTP) and latency (ICMP/TCP/HTTP HEAD) collectors |
| 5 — User Interface | ✅ Complete | CLI (start/stop/preflight), Rich TUI dashboard |
| 6 — Hardening | Planned | Disconnect recovery, modem health monitor, error recovery |
| 7 — API | Planned | FastAPI REST + WebSocket (stub in place) |

---

## Configuration Reference

See `config.example.toml` for the full annotated configuration reference with
all available options and their defaults.

Key sections:

| Section | Key settings |
|---------|-------------|
| `[general]` | `db_path`, `log_path`, `log_level` |
| `[gnss]` | `primary_modem`, `failover_order`, `movement_threshold_meters` |
| `[polling]` | `rf_interval_seconds` (min 0.1s), `throughput_interval_seconds` |
| `[throughput]` | `method` (iperf3/http), `iperf3_server`, `iperf3_port` |
| `[latency]` | `methods` (icmp/tcp/http_head), `targets`, `icmp_count` |
| `[[modems]]` | `imei`, `carrier`, `label`, `apn` |

---

## License

See [LICENSE](LICENSE).
