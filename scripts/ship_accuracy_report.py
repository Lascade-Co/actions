"""Ship Position Accuracy Report — VesselFinder vs Lascade Ship API.

Fetches ship positions from VesselFinder's map API (source of truth),
compares them against Lascade's ship API, and generates a self-contained
HTML report with per-ship deviations, time delays, and aggregate stats.

Usage:
    python3 scripts/ship_accuracy_report.py --bbox 39.72,-11.69,42.28,-7.93
    python3 scripts/ship_accuracy_report.py --bbox 39.72,-11.69,42.28,-7.93 --zoom 8 --output report.html
"""

import argparse
import concurrent.futures
import html as html_mod
import json
import math
import statistics
import struct
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timedelta, timezone

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

VF_BASE_URL = "https://www.vesselfinder.com/api/pub/mp2"
VF_USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/134.0.0.0 Safari/537.36"
)
LASCADE_SHIP_URL = "https://ship.lascade.com/ships/{mmsi}"
EARTH_RADIUS_KM = 6371.0
COORD_FACTOR = 600_000

AN = 6e5  # Coordinate divisor (confirmed from JS: An = 6e5)


# ---------------------------------------------------------------------------
# VesselFinder API
# ---------------------------------------------------------------------------
def decode_timestamp(td: int) -> str:
    """Decode the timestamp delta field (Int8).
    Positive = minutes ago, negative = hours/days encoded."""
    if td == -1:
        return "unknown"
    if td == 0:
        return "just now"
    if td > 0:
        return f"{td} min ago"
    # Negative values: lower 7 bits encode hours
    hours = td & 0x7F
    if hours == 0:
        return "just now"
    if hours >= 24:
        days = round(hours / 24)
        return f"~{days} day{'s' if days > 1 else ''} ago"
    return f"~{hours} hour{'s' if hours > 1 else ''} ago"


def decode_heading(w_field: int) -> float | None:
    """Decode heading from W field (6-bit, bits 8-13 of flags Int16).
    0-31 = valid heading (multiply by 11.25 for degrees), >=32 = unknown."""
    if w_field < 32:
        return w_field * 11.25
    return None


def read_name(data: bytes, offset: int, length: int) -> str:
    """Read ship name from binary data (same as JS _g function)."""
    return ''.join(chr(data[offset + i]) for i in range(length))


def decode_response(data: bytes, zoom: int = 8, fleet_mode: bool = False) -> dict:
    """Decode a VesselFinder /api/pub/mp2 binary response.

    Args:
        data: raw binary response bytes
        zoom: zoom level from the request URL
        fleet_mode: whether fleet mode was active

    Returns:
        dict with 'header' and 'ships' keys
    """
    total_len = len(data)

    if total_len < 4:
        return {"header": {}, "ships": []}

    # --- Header ---
    format_marker = data[0]  # 0x43 = 'C'
    y = struct.unpack_from('>H', data, 1)[0]  # Header extension size

    header = {
        "format_marker": f"0x{format_marker:02X}",
        "extension_size": y,
    }

    ref_mmsi = None

    if y >= 8 and total_len >= 12:
        flags_word = struct.unpack_from('>i', data, 4)[0]
        mcb_flags = {
            "p1": bool(flags_word & 1),
            "p2": bool(flags_word & 2),
            "p3": bool(flags_word & 4),
            "p4": bool(flags_word & 8),
            "p7": bool(flags_word & 64),
        }
        header["mcb_flags"] = mcb_flags

        if mcb_flags.get("p7") and total_len >= 12:
            total_ships = struct.unpack_from('>i', data, 8)[0]
            header["total_ships_global"] = total_ships

    record_start = 4 + y
    if record_start >= 4:
        ref_mmsi = struct.unpack_from('>i', data, record_start - 4)[0]
        header["reference_mmsi"] = ref_mmsi

    header["record_start_offset"] = record_start

    # --- Ship Records ---
    ships = []
    i = record_start
    high_zoom = zoom > 13  # 'b' flag in JS (yg = 13)

    while i < total_len:
        if i + 16 > total_len:
            break

        # Flags Int16 (2 bytes, big-endian)
        w = struct.unpack_from('>h', data, i)[0]
        ship_type_idx = (w & 0xF0) >> 4       # bits 4-7: ship type (0-8)
        heading_idx = (w & 0x3F00) >> 8        # bits 8-13: heading (0-63)
        size_bits = w & 0xC000                 # bits 14-15: marker size

        if zoom > 6:
            if size_bits == -16384:    # 0xC000 as signed = -16384
                marker_size = 2
            elif size_bits == -32768:  # 0x8000 as signed
                marker_size = 0
            else:
                marker_size = 1
        else:
            marker_size = 1

        i += 2

        # MMSI (Int32)
        mmsi = struct.unpack_from('>i', data, i)[0]
        i += 4
        is_reference = (mmsi == ref_mmsi)

        # Latitude and Longitude (Int32 each, ÷ 600000 = degrees)
        lat_raw = struct.unpack_from('>i', data, i)[0]
        i += 4
        lon_raw = struct.unpack_from('>i', data, i)[0]
        i += 4

        lat = lat_raw / AN
        lon = lon_raw / AN

        # Speed and course (only for reference ship or fleet mode)
        speed = None
        course = None
        if is_reference:
            if i + 6 <= total_len:
                speed = struct.unpack_from('>h', data, i)[0] / 10.0
                i += 2
                course = struct.unpack_from('>h', data, i)[0] / 10.0
                i += 2
                i += 2  # skip 2 bytes (heading or reserved)
        elif fleet_mode:
            if i + 4 <= total_len:
                speed = struct.unpack_from('>h', data, i)[0] / 10.0
                i += 2
                course = struct.unpack_from('>h', data, i)[0] / 10.0
                i += 2

        # Timestamp delta (Int8)
        if i >= total_len:
            break
        td = struct.unpack_from('>b', data, i)[0]
        i += 1

        # Name length (Int8) and name
        if i >= total_len:
            break
        name_len = struct.unpack_from('>b', data, i)[0]
        i += 1

        if i + name_len > total_len:
            break

        name = read_name(data, i, name_len) if name_len > 0 else str(mmsi)
        i += name_len

        # Reference ship: extra Int32 after name (IMO or timestamp)
        imo = None
        if is_reference:
            if i + 4 <= total_len:
                imo = struct.unpack_from('>i', data, i)[0]
                i += 4

        # High zoom (>13): extra 10 bytes for ship dimensions
        ship_dims = None
        if high_zoom:
            if i + 10 <= total_len:
                dims = struct.unpack_from('>hhhhh', data, i)
                i += 10
                ship_dims = {
                    "a": dims[0], "b": dims[1],
                    "c": dims[2], "d": dims[3],
                    "heading_precise": dims[4],
                }

        heading = decode_heading(heading_idx)

        ship = {
            "name": name,
            "mmsi": mmsi,
            "lat": round(lat, 5),
            "lon": round(lon, 5),
            "ship_type_id": ship_type_idx,
            "heading": heading,
            "heading_index": heading_idx,
            "marker_size": marker_size,
            "last_update": decode_timestamp(td),
            "td_raw": td,
            "is_reference": is_reference,
        }

        if speed is not None:
            ship["speed_knots"] = speed
        if course is not None:
            ship["course"] = course
        if imo:
            ship["imo"] = imo
        if ship_dims:
            ship["dimensions"] = ship_dims

        ships.append(ship)

    return {"header": header, "ships": ships}


def build_vf_url(lat_min, lon_min, lat_max, lon_max, zoom):
    bbox = ",".join(str(int(v * COORD_FACTOR)) for v in [lon_min, lat_min, lon_max, lat_max])
    params = urllib.parse.urlencode({
        "bbox": bbox,
        "zoom": zoom,
        "ref": f"{time.time():.5f}",
        "mcbe": 1,
    })
    return f"{VF_BASE_URL}?{params}"


def fetch_vf_ships(lat_min, lon_min, lat_max, lon_max, zoom):
    url = build_vf_url(lat_min, lon_min, lat_max, lon_max, zoom)
    req = urllib.request.Request(url, headers={
        "User-Agent": VF_USER_AGENT,
        "Referer": "",
    })
    fetch_time = datetime.now(timezone.utc)
    with urllib.request.urlopen(req, timeout=10) as resp:
        data = resp.read()
    result = decode_response(data, zoom=zoom)
    return result.get("ships", []), fetch_time


# ---------------------------------------------------------------------------
# Lascade API
# ---------------------------------------------------------------------------

def fetch_lascade_ship(mmsi):
    url = LASCADE_SHIP_URL.format(mmsi=mmsi)
    req = urllib.request.Request(url, headers={"User-Agent": VF_USER_AGENT})
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            return json.loads(resp.read()), None
    except urllib.error.HTTPError as e:
        if e.code == 404:
            return None, "not_found"
        return None, f"http_{e.code}"
    except (urllib.error.URLError, TimeoutError, OSError):
        return None, "network_error"


# ---------------------------------------------------------------------------
# Math / comparison helpers
# ---------------------------------------------------------------------------

def haversine_km(lat1, lon1, lat2, lon2):
    lat1, lon1, lat2, lon2 = (math.radians(v) for v in (lat1, lon1, lat2, lon2))
    dlat = lat2 - lat1
    dlon = lon2 - lon1
    a = math.sin(dlat / 2) ** 2 + math.cos(lat1) * math.cos(lat2) * math.sin(dlon / 2) ** 2
    return EARTH_RADIUS_KM * 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))


def td_raw_to_seconds(td_raw):
    if td_raw == -1:
        return None
    if td_raw == 0:
        return 0
    if td_raw > 0:
        return td_raw * 60
    hours = td_raw & 0x7F
    return hours * 3600 if hours > 0 else 0


def compare_ship(vf_ship, lascade_data, fetch_time):
    loc = lascade_data.get("location")
    if not loc or not loc.get("coordinates"):
        return None

    coords = loc["coordinates"]  # [lon, lat]
    l_lon, l_lat = coords[0], coords[1]
    v_lat, v_lon = vf_ship["lat"], vf_ship["lon"]

    distance_km = haversine_km(v_lat, v_lon, l_lat, l_lon)

    # Time delay
    td_raw = vf_ship.get("td_raw")
    time_delay_seconds = None
    vf_estimated_time = None
    lascade_time = None

    if td_raw is not None:
        offset = td_raw_to_seconds(td_raw)
        if offset is not None:
            vf_estimated_time = fetch_time - timedelta(seconds=offset)

    lp = lascade_data.get("last_position")
    if lp:
        try:
            lascade_time = datetime.fromisoformat(lp.replace("Z", "+00:00"))
        except (ValueError, TypeError):
            pass

    if vf_estimated_time and lascade_time:
        time_delay_seconds = (lascade_time - vf_estimated_time).total_seconds()

    return {
        "mmsi": vf_ship["mmsi"],
        "name": vf_ship.get("name", str(vf_ship["mmsi"])),
        "ship_type": vf_ship.get("ship_type", "Unknown"),
        "vf_lat": v_lat,
        "vf_lon": v_lon,
        "lascade_lat": round(l_lat, 5),
        "lascade_lon": round(l_lon, 5),
        "distance_km": round(distance_km, 3),
        "time_delay_seconds": round(time_delay_seconds) if time_delay_seconds is not None else None,
        "vf_estimated_time": vf_estimated_time.isoformat() if vf_estimated_time else None,
        "lascade_last_position": lascade_time.isoformat() if lascade_time else None,
    }


def safe_p95(data):
    if not data:
        return None
    if len(data) == 1:
        return data[0]
    return statistics.quantiles(data, n=20, method="inclusive")[18]


def compute_stats(results):
    distances = [r["distance_km"] for r in results]
    abs_delays = [abs(r["time_delay_seconds"]) for r in results if r["time_delay_seconds"] is not None]

    stats = {}
    if distances:
        stats["mean_distance"] = round(statistics.mean(distances), 3)
        stats["median_distance"] = round(statistics.median(distances), 3)
        stats["p95_distance"] = round(safe_p95(distances), 3)
        stats["max_distance"] = round(max(distances), 3)
    if abs_delays:
        stats["mean_delay"] = round(statistics.mean(abs_delays))
        stats["median_delay"] = round(statistics.median(abs_delays))
        stats["p95_delay"] = round(safe_p95(abs_delays))
        stats["max_delay"] = round(max(abs_delays))
    return stats


def format_delay(seconds):
    if seconds is None:
        return "N/A"
    abs_s = abs(seconds)
    if abs_s < 60:
        label = f"{abs_s}s"
    elif abs_s < 3600:
        label = f"{abs_s // 60}m {abs_s % 60}s"
    else:
        h = abs_s // 3600
        m = (abs_s % 3600) // 60
        label = f"{h}h {m}m"
    return f"{label} {'ahead' if seconds > 0 else 'behind'}" if seconds != 0 else "in sync"


def format_seconds_short(seconds):
    if seconds is None:
        return "N/A"
    if seconds < 60:
        return f"{seconds}s"
    if seconds < 3600:
        return f"{seconds // 60}m {seconds % 60}s"
    h = seconds // 3600
    m = (seconds % 3600) // 60
    return f"{h}h {m}m"


def distance_color(km):
    if km < 1:
        return "#059669"   # green
    if km < 10:
        return "#d97706"   # amber
    if km < 50:
        return "#ea580c"   # orange
    return "#dc2626"       # red


def distance_bg(km):
    if km < 1:
        return "#ecfdf5"
    if km < 10:
        return "#fffbeb"
    if km < 50:
        return "#fff7ed"
    return "#fef2f2"


# ---------------------------------------------------------------------------
# HTML report generation
# ---------------------------------------------------------------------------

def generate_report(results, not_found, fetch_errors, skipped_count, total_vf, bboxes, stats, gen_time):
    e = html_mod.escape

    bbox_strs = [f"({b[0]}, {b[1]}) → ({b[2]}, {b[3]})" for b in bboxes]

    # Distribution buckets
    buckets = [
        ("<0.1 km", 0, 0.1),
        ("0.1–1 km", 0.1, 1),
        ("1–5 km", 1, 5),
        ("5–10 km", 5, 10),
        ("10–50 km", 10, 50),
        (">50 km", 50, float("inf")),
    ]
    bucket_colors = ["#059669", "#10b981", "#d97706", "#ea580c", "#dc2626", "#991b1b"]
    bucket_counts = [0] * len(buckets)
    for r in results:
        for i, (_, lo, hi) in enumerate(buckets):
            if lo <= r["distance_km"] < hi:
                bucket_counts[i] += 1
                break

    max_bucket = max(bucket_counts) if bucket_counts else 1

    # Summary values
    matched = len(results)
    nf = len(not_found)
    fe = len(fetch_errors)
    unknown_time_count = sum(1 for r in results if r["time_delay_seconds"] is None)

    def stat_card(label, value, unit=""):
        return f'<div class="card"><div class="card-value">{e(str(value))}<span class="card-unit">{e(unit)}</span></div><div class="card-label">{e(label)}</div></div>'

    dist_cards = ""
    if stats.get("mean_distance") is not None:
        dist_cards = (
            stat_card("Mean Deviation", stats["mean_distance"], " km")
            + stat_card("P95 Deviation", stats["p95_distance"], " km")
            + stat_card("Max Deviation", stats["max_distance"], " km")
            + stat_card("Median Deviation", stats["median_distance"], " km")
        )

    delay_cards = ""
    if stats.get("mean_delay") is not None:
        delay_cards = (
            stat_card("Mean Delay", format_seconds_short(stats["mean_delay"]))
            + stat_card("P95 Delay", format_seconds_short(stats["p95_delay"]))
            + stat_card("Max Delay", format_seconds_short(stats["max_delay"]))
        )

    # Ship table rows
    sorted_results = sorted(results, key=lambda x: x["distance_km"], reverse=True)
    ship_rows = ""
    for r in sorted_results:
        bg = distance_bg(r["distance_km"])
        clr = distance_color(r["distance_km"])
        ship_rows += f"""<tr style="background:{bg}">
<td>{e(r['name'])}</td>
<td><code>{r['mmsi']}</code></td>
<td>{e(r['ship_type'])}</td>
<td>{r['vf_lat']}, {r['vf_lon']}</td>
<td>{r['lascade_lat']}, {r['lascade_lon']}</td>
<td style="color:{clr};font-weight:600">{r['distance_km']}</td>
<td>{e(format_delay(r['time_delay_seconds']))}</td>
</tr>"""

    # Not found rows
    nf_rows = ""
    for ship in not_found:
        nf_rows += f"""<tr>
<td>{e(ship.get('name', str(ship['mmsi'])))}</td>
<td><code>{ship['mmsi']}</code></td>
<td>{e(ship.get('ship_type', 'Unknown'))}</td>
<td>{ship.get('lat', '')}, {ship.get('lon', '')}</td>
</tr>"""

    # Fetch error rows
    fe_rows = ""
    for ship, err in fetch_errors:
        fe_rows += f"""<tr>
<td>{e(ship.get('name', str(ship['mmsi'])))}</td>
<td><code>{ship['mmsi']}</code></td>
<td>{e(err)}</td>
</tr>"""

    # Distribution bars
    dist_bars = ""
    for i, (label, _, _) in enumerate(buckets):
        pct = (bucket_counts[i] / max_bucket * 100) if max_bucket > 0 else 0
        dist_bars += f"""<div class="bar-row">
<div class="bar-label">{e(label)}</div>
<div class="bar-track"><div class="bar-fill" style="width:{pct}%;background:{bucket_colors[i]}"></div></div>
<div class="bar-count">{bucket_counts[i]}</div>
</div>"""

    nf_section = ""
    if not_found:
        nf_section = f"""
<section>
<h2>Ships Not Found in Lascade</h2>
<table>
<thead><tr><th>Name</th><th>MMSI</th><th>Type</th><th>VF Position</th></tr></thead>
<tbody>{nf_rows}</tbody>
</table>
</section>"""

    fe_section = ""
    if fetch_errors:
        fe_section = f"""
<section>
<h2>Fetch Errors</h2>
<table>
<thead><tr><th>Name</th><th>MMSI</th><th>Error</th></tr></thead>
<tbody>{fe_rows}</tbody>
</table>
</section>"""

    skipped_card = ""
    if skipped_count > 0:
        skipped_card = stat_card("Skipped (cap)", skipped_count)

    return f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>Ship Accuracy Report — {e(gen_time.strftime('%Y-%m-%d %H:%M UTC'))}</title>
<style>
*,*::before,*::after{{box-sizing:border-box}}
body{{margin:0;font-family:-apple-system,BlinkMacSystemFont,"Segoe UI",Roboto,"Helvetica Neue",Arial,sans-serif;background:#f8fafc;color:#1e293b;line-height:1.5}}
.container{{max-width:1200px;margin:0 auto;padding:24px 20px}}
header{{background:linear-gradient(135deg,#0f172a 0%,#1e3a5f 100%);color:#fff;padding:40px 0;margin-bottom:32px}}
header .container{{display:flex;flex-direction:column;gap:8px}}
h1{{margin:0;font-size:28px;font-weight:700;letter-spacing:-0.5px}}
.subtitle{{opacity:0.85;font-size:15px}}
.meta{{display:flex;gap:24px;flex-wrap:wrap;margin-top:8px;font-size:13px;opacity:0.7}}
.cards{{display:grid;grid-template-columns:repeat(auto-fit,minmax(180px,1fr));gap:16px;margin-bottom:32px}}
.card{{background:#fff;border:1px solid #e2e8f0;border-radius:12px;padding:20px;text-align:center}}
.card-value{{font-size:28px;font-weight:700;color:#0f172a}}
.card-unit{{font-size:14px;font-weight:400;color:#64748b}}
.card-label{{font-size:13px;color:#64748b;margin-top:4px}}
section{{background:#fff;border:1px solid #e2e8f0;border-radius:12px;padding:24px;margin-bottom:24px}}
h2{{margin:0 0 16px;font-size:18px;font-weight:600;color:#0f172a}}
table{{width:100%;border-collapse:collapse;font-size:14px}}
thead th{{text-align:left;padding:10px 12px;background:#f1f5f9;border-bottom:2px solid #e2e8f0;font-weight:600;color:#475569;font-size:12px;text-transform:uppercase;letter-spacing:0.5px}}
tbody td{{padding:10px 12px;border-bottom:1px solid #f1f5f9}}
tbody tr:hover{{filter:brightness(0.97)}}
code{{font-size:13px;background:#f1f5f9;padding:2px 6px;border-radius:4px}}
.bar-row{{display:flex;align-items:center;gap:12px;margin-bottom:8px}}
.bar-label{{width:80px;font-size:13px;color:#64748b;text-align:right;flex-shrink:0}}
.bar-track{{flex:1;height:24px;background:#f1f5f9;border-radius:6px;overflow:hidden}}
.bar-fill{{height:100%;border-radius:6px;transition:width 0.3s ease}}
.bar-count{{width:40px;font-size:13px;font-weight:600;color:#334155}}
footer{{text-align:center;padding:24px;font-size:12px;color:#94a3b8}}
@media print{{
  body{{background:#fff}}
  header{{background:#0f172a !important;-webkit-print-color-adjust:exact;print-color-adjust:exact}}
  .card,.section{{break-inside:avoid}}
  table{{font-size:11px}}
}}
</style>
</head>
<body>
<header>
<div class="container">
<h1>Ship Position Accuracy Report</h1>
<div class="subtitle">VesselFinder vs Lascade Ship API</div>
<div class="meta">
<span>Generated: {e(gen_time.strftime('%Y-%m-%d %H:%M:%S UTC'))}</span>
<span>Region(s): {e(' | '.join(bbox_strs))}</span>
</div>
</div>
</header>

<div class="container">

<div class="cards">
{stat_card("Total Ships (VF)", total_vf)}
{stat_card("Matched", matched)}
{stat_card("Not Found", nf)}
{stat_card("Fetch Errors", fe)}
{skipped_card}
</div>

<div class="cards">
{dist_cards}
</div>

<div class="cards">
{delay_cards}
</div>

<section>
<h2>Deviation Distribution</h2>
{dist_bars}
</section>

<section>
<h2>Ship Details</h2>
<table>
<thead><tr>
<th>Name</th><th>MMSI</th><th>Type</th><th>VF Position</th><th>Lascade Position</th><th>Distance (km)</th><th>Time Delta</th>
</tr></thead>
<tbody>{ship_rows}</tbody>
</table>
</section>

{nf_section}
{fe_section}

</div>

<footer>
VesselFinder timestamps are approximate (minute-level for recent, hour-level for older positions).
Ships with unknown VF timestamps ({unknown_time_count}) are excluded from time-delay statistics.
Distances computed using the Haversine formula (WGS84).
</footer>
</body>
</html>"""


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def parse_bbox(s):
    parts = s.split(",")
    if len(parts) != 4:
        raise argparse.ArgumentTypeError(f"bbox must be LAT_MIN,LON_MIN,LAT_MAX,LON_MAX, got: {s}")
    try:
        return tuple(float(p) for p in parts)
    except ValueError:
        raise argparse.ArgumentTypeError(f"bbox values must be numeric, got: {s}")


def main():
    parser = argparse.ArgumentParser(description="Ship position accuracy report: VesselFinder vs Lascade")
    parser.add_argument("--bbox", type=parse_bbox, action="append", required=True,
                        help="LAT_MIN,LON_MIN,LAT_MAX,LON_MAX (repeatable)")
    parser.add_argument("--zoom", type=int, default=8, help="VesselFinder zoom level (default: 8)")
    parser.add_argument("--output", default=None, help="Output HTML file path")
    parser.add_argument("--max-ships", type=int, default=200, help="Max Lascade API calls (default: 200)")
    args = parser.parse_args()

    gen_time = datetime.now(timezone.utc)
    if args.output is None:
        args.output = f"ship_report_{gen_time.strftime('%Y%m%d_%H%M%S')}.html"

    # Fetch VesselFinder ships for each bbox
    all_ships = {}
    fetch_time = None
    for bbox in args.bbox:
        lat_min, lon_min, lat_max, lon_max = bbox
        print(f"Fetching VesselFinder ships for bbox ({lat_min}, {lon_min}) → ({lat_max}, {lon_max}) ...")
        ships, ft = fetch_vf_ships(lat_min, lon_min, lat_max, lon_max, args.zoom)
        fetch_time = ft
        for s in ships:
            all_ships[s["mmsi"]] = s
        print(f"  → {len(ships)} ships ({len(all_ships)} unique total)")

    total_vf = len(all_ships)
    if total_vf == 0:
        print("No ships found. Generating empty report.")

    # Cap ships for Lascade calls
    ship_list = list(all_ships.values())
    skipped_count = max(0, len(ship_list) - args.max_ships)
    ship_list = ship_list[:args.max_ships]

    # Parallel fetch from Lascade
    print(f"Fetching Lascade data for {len(ship_list)} ships ...")
    lascade_results = {}

    def _fetch(ship):
        return ship["mmsi"], fetch_lascade_ship(ship["mmsi"])

    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as pool:
        futures = {}
        for s in ship_list:
            futures[pool.submit(_fetch, s)] = s
            time.sleep(0.05)  # 50ms stagger to avoid rate limits
        for future in concurrent.futures.as_completed(futures):
            mmsi, (data, err) = future.result()
            lascade_results[mmsi] = (data, err)

    # Compare
    results = []
    not_found = []
    fetch_errors = []

    for ship in ship_list:
        mmsi = ship["mmsi"]
        data, err = lascade_results.get(mmsi, (None, "missing"))
        if err == "not_found":
            not_found.append(ship)
        elif err:
            fetch_errors.append((ship, err))
        elif data:
            comparison = compare_ship(ship, data, fetch_time)
            if comparison:
                results.append(comparison)
            else:
                not_found.append(ship)

    matched = len(results)
    print(f"  → Matched: {matched}, Not found: {len(not_found)}, Errors: {len(fetch_errors)}, Skipped: {skipped_count}")

    # Stats
    stats = compute_stats(results)

    # Generate report
    html = generate_report(results, not_found, fetch_errors, skipped_count, total_vf, args.bbox, stats, gen_time)

    with open(args.output, "w", encoding="utf-8") as f:
        f.write(html)

    print(f"Report written to {args.output}")


if __name__ == "__main__":
    main()
