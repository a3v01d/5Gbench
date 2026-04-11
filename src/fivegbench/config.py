"""Configuration loading, validation, and defaults for 5gbench.

Config file: ~/.config/5gbench/config.toml (TOML format, Python 3.11+ tomllib)
Example:     config.example.toml in repository root

All fields have sensible defaults so the tool runs with minimal config (just
modem IMEI mappings required).
"""

from __future__ import annotations

import os
import re
import tomllib
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

CONFIG_DIR = Path("~/.config/5gbench").expanduser()
CONFIG_PATH = CONFIG_DIR / "config.toml"

# ---------------------------------------------------------------------------
# Dataclasses
# ---------------------------------------------------------------------------

@dataclass
class DnsConfig:
    nameservers: list[str] = field(default_factory=lambda: ["8.8.8.8", "1.1.1.1"])


@dataclass
class GeneralConfig:
    db_path: Path = CONFIG_DIR
    log_path: Path = CONFIG_DIR / "logs"
    log_level: str = "warning"
    dns: DnsConfig = field(default_factory=DnsConfig)


@dataclass
class SessionDefaults:
    operator: str = ""
    vehicle_id: str = ""
    route_description: str = ""
    notes: str = ""


@dataclass
class GnssConfig:
    primary_modem: str = "att"
    failover_order: list[str] = field(default_factory=lambda: ["tmobile", "verizon"])
    movement_threshold_meters: float = 5.0
    interpolate_on_fix_loss: bool = True


@dataclass
class PollingConfig:
    rf_interval_seconds: float = 1.0
    neighbor_cells: bool = True
    throughput_interval_seconds: float = 60.0
    gnss_interval_seconds: float = 1.0


@dataclass
class ThroughputConfig:
    method: str = "iperf3"
    iperf3_server: str = ""
    iperf3_port: int = 5201
    iperf3_duration_seconds: int = 10
    http_fallback_url: str = "https://speed.cloudflare.com/__down?bytes=25000000"


@dataclass
class LatencyConfig:
    methods: list[str] = field(default_factory=lambda: ["icmp", "tcp", "http_head"])
    targets: list[str] = field(default_factory=lambda: ["8.8.8.8", "1.1.1.1"])
    icmp_count: int = 10
    tcp_port: int = 443
    jitter: bool = True


@dataclass
class ApiConfig:
    enabled: bool = False
    bind: str = "127.0.0.1"
    port: int = 8080
    auth: str = "none"
    api_key: str = ""


@dataclass
class TuiConfig:
    enabled: bool = True


@dataclass
class ModemConfig:
    imei: str
    carrier: str        # machine-readable label: "att", "tmobile", "verizon"
    label: str          # human-readable: "AT&T", "T-Mobile", "Verizon"
    apn: str

    # Resolved at runtime by discovery.py / ModemManager
    at_port: str = ""           # /dev/ttyUSBx for AT commands
    net_interface: str = ""     # wwanX
    namespace: str = ""         # ns_att, ns_tmobile, etc.
    mm_index: str = ""          # ModemManager index ("0", "1", "2")


@dataclass
class Config:
    general: GeneralConfig = field(default_factory=GeneralConfig)
    session_defaults: SessionDefaults = field(default_factory=SessionDefaults)
    gnss: GnssConfig = field(default_factory=GnssConfig)
    polling: PollingConfig = field(default_factory=PollingConfig)
    throughput: ThroughputConfig = field(default_factory=ThroughputConfig)
    latency: LatencyConfig = field(default_factory=LatencyConfig)
    api: ApiConfig = field(default_factory=ApiConfig)
    tui: TuiConfig = field(default_factory=TuiConfig)
    modems: list[ModemConfig] = field(default_factory=list)


# ---------------------------------------------------------------------------
# Loaders
# ---------------------------------------------------------------------------

def _expand_path(value: str | None, default: Path) -> Path:
    if not value:
        return default
    return Path(value).expanduser()


def _load_dns(raw: dict) -> DnsConfig:
    return DnsConfig(
        nameservers=raw.get("nameservers", ["8.8.8.8", "1.1.1.1"])
    )


def _load_general(raw: dict) -> GeneralConfig:
    dns_raw = raw.get("dns", {})
    return GeneralConfig(
        db_path=_expand_path(raw.get("db_path"), CONFIG_DIR),
        log_path=_expand_path(raw.get("log_path"), CONFIG_DIR / "logs"),
        log_level=raw.get("log_level", "warning").lower(),
        dns=_load_dns(dns_raw),
    )


def _load_session_defaults(raw: dict) -> SessionDefaults:
    return SessionDefaults(
        operator=raw.get("operator", ""),
        vehicle_id=raw.get("vehicle_id", ""),
        route_description=raw.get("route_description", ""),
        notes=raw.get("notes", ""),
    )


def _load_gnss(raw: dict) -> GnssConfig:
    return GnssConfig(
        primary_modem=raw.get("primary_modem", "att"),
        failover_order=raw.get("failover_order", ["tmobile", "verizon"]),
        movement_threshold_meters=float(raw.get("movement_threshold_meters", 5.0)),
        interpolate_on_fix_loss=bool(raw.get("interpolate_on_fix_loss", True)),
    )


def _load_polling(raw: dict) -> PollingConfig:
    interval = float(raw.get("rf_interval_seconds", 1.0))
    if interval < 0.1:
        raise ValueError(
            f"polling.rf_interval_seconds must be >= 0.1, got {interval}"
        )
    return PollingConfig(
        rf_interval_seconds=interval,
        neighbor_cells=bool(raw.get("neighbor_cells", True)),
        throughput_interval_seconds=float(raw.get("throughput_interval_seconds", 60.0)),
        gnss_interval_seconds=float(raw.get("gnss_interval_seconds", 1.0)),
    )


def _load_throughput(raw: dict) -> ThroughputConfig:
    method = raw.get("method", "iperf3")
    if method not in ("iperf3", "http"):
        raise ValueError(f"throughput.method must be 'iperf3' or 'http', got '{method}'")
    return ThroughputConfig(
        method=method,
        iperf3_server=raw.get("iperf3_server", ""),
        iperf3_port=int(raw.get("iperf3_port", 5201)),
        iperf3_duration_seconds=int(raw.get("iperf3_duration_seconds", 10)),
        http_fallback_url=raw.get(
            "http_fallback_url",
            "https://speed.cloudflare.com/__down?bytes=25000000",
        ),
    )


def _load_latency(raw: dict) -> LatencyConfig:
    valid_methods = {"icmp", "tcp", "http_head"}
    methods = raw.get("methods", ["icmp", "tcp", "http_head"])
    for m in methods:
        if m not in valid_methods:
            raise ValueError(
                f"latency.methods contains unknown method '{m}'. "
                f"Valid: {sorted(valid_methods)}"
            )
    return LatencyConfig(
        methods=methods,
        targets=raw.get("targets", ["8.8.8.8", "1.1.1.1"]),
        icmp_count=int(raw.get("icmp_count", 10)),
        tcp_port=int(raw.get("tcp_port", 443)),
        jitter=bool(raw.get("jitter", True)),
    )


def _load_api(raw: dict) -> ApiConfig:
    auth = raw.get("auth", "none")
    if auth not in ("none", "apikey"):
        raise ValueError(f"api.auth must be 'none' or 'apikey', got '{auth}'")
    return ApiConfig(
        enabled=bool(raw.get("enabled", False)),
        bind=raw.get("bind", "127.0.0.1"),
        port=int(raw.get("port", 8080)),
        auth=auth,
        api_key=raw.get("api_key", ""),
    )


def _load_modems(raw_list: list[dict]) -> list[ModemConfig]:
    if not raw_list:
        raise ValueError("Config must define at least one [[modems]] entry.")
    modems = []
    for i, raw in enumerate(raw_list):
        for required in ("imei", "carrier", "label", "apn"):
            if required not in raw:
                raise ValueError(
                    f"[[modems]][{i}] is missing required field '{required}'"
                )
        imei = str(raw["imei"]).strip()
        if not re.fullmatch(r"\d{15}", imei):
            raise ValueError(
                f"[[modems]][{i}] imei '{imei}' is not a valid 15-digit IMEI"
            )
        carrier = str(raw["carrier"]).strip().lower()
        ns = f"ns_{carrier}"
        modems.append(
            ModemConfig(
                imei=imei,
                carrier=carrier,
                label=str(raw["label"]).strip(),
                apn=str(raw["apn"]).strip(),
                namespace=ns,
            )
        )
    # Check for duplicate IMEIs or carriers
    imeis = [m.imei for m in modems]
    if len(imeis) != len(set(imeis)):
        raise ValueError("Duplicate modem IMEI in config.")
    carriers = [m.carrier for m in modems]
    if len(carriers) != len(set(carriers)):
        raise ValueError("Duplicate modem carrier label in config.")
    return modems


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def load(path: Path | None = None) -> Config:
    """Load and validate config from *path* (default: ~/.config/5gbench/config.toml).

    Raises FileNotFoundError if the file doesn't exist.
    Raises ValueError on validation failures.
    Raises tomllib.TOMLDecodeError on parse errors.
    """
    config_path = path or CONFIG_PATH
    config_path = Path(config_path).expanduser()
    if not config_path.exists():
        raise FileNotFoundError(
            f"Config file not found: {config_path}\n"
            f"Run: cp config.example.toml {config_path}"
        )
    with open(config_path, "rb") as fh:
        raw: dict[str, Any] = tomllib.load(fh)

    cfg = Config(
        general=_load_general(raw.get("general", {})),
        session_defaults=_load_session_defaults(raw.get("session_defaults", {})),
        gnss=_load_gnss(raw.get("gnss", {})),
        polling=_load_polling(raw.get("polling", {})),
        throughput=_load_throughput(raw.get("throughput", {})),
        latency=_load_latency(raw.get("latency", {})),
        api=_load_api(raw.get("api", {})),
        tui=TuiConfig(enabled=bool(raw.get("tui", {}).get("enabled", True))),
        modems=_load_modems(raw.get("modems", [])),
    )
    return cfg


def ensure_dirs(cfg: Config) -> None:
    """Create db_path and log_path directories if they don't exist."""
    cfg.general.db_path.mkdir(parents=True, exist_ok=True)
    cfg.general.log_path.mkdir(parents=True, exist_ok=True)
