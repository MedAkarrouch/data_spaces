#!/usr/bin/env python3
"""
Generate 3 heterogeneous datasets for a Data Space project:
  1) traffic_data.csv (CSV) - road traffic per zone and 5-min windows
  2) bus_gps.geojson (GeoJSON) - bus GPS points with area_code (zone synonym)
  3) planning.txt (TXT) - planning reference with service_zone (zone synonym)

Design choices (maxed but safe):
  - 8 zones
  - Traffic: 8 zones x 69 windows (07:00 -> 12:40, step 5 min) = 552 rows (~550)
  - Bus GeoJSON: 15 buses x 120 points = 1800 features
  - Planning TXT: 180 lines
  - Imperfections (moderate):
      * Traffic: ~3% missing values, ~1% outliers (fixable)
      * Bus: small % missing delay, and synonym for zone (area_code)
      * Planning: synonym for zone (service_zone) + slight formatting variability

Outputs are reproducible thanks to a fixed seed.
"""

import csv
import json
import math
import random
from datetime import datetime, timedelta
from pathlib import Path

# ----------------------------
# Global config (feel free to tweak)
# ----------------------------
SEED = 42
OUTDIR = Path("generated_sources")

# Common "semantic zones" (true concept)
ZONES = [f"Z{i}" for i in range(1, 9)]  # Z1..Z8

# Synonyms used by different actors (semantic interoperability challenge)
ZONE_TO_AREA_CODE = {z: f"A{idx:02d}" for idx, z in enumerate(ZONES, start=1)}         # e.g., Z1 -> A01
ZONE_TO_SERVICE_ZONE = {z: f"ServiceZone-{z}" for z in ZONES}                           # e.g., Z1 -> ServiceZone-Z1

# Traffic (CSV)
TRAFFIC_START = datetime(2025, 3, 10, 7, 0)
TRAFFIC_END   = datetime(2025, 3, 10, 12, 40)  # chosen to reach ~550 rows with 8 zones
TRAFFIC_STEP_MIN = 5

TRAFFIC_MISSING_RATE = 0.03   # 3%
TRAFFIC_OUTLIER_RATE = 0.01   # 1%

# Bus (GeoJSON)
BUS_COUNT = 15
BUS_POINTS_PER_BUS = 120
BUS_TOTAL_FEATURES = BUS_COUNT * BUS_POINTS_PER_BUS  # 1800
BUS_TIME_STEP_SEC = 60  # 1 minute steps (higher frequency than traffic)
BUS_MISSING_DELAY_RATE = 0.02  # 2% missing delays (cleaning case)

# Planning (TXT)
PLANNING_LINES = 180

# ----------------------------
# Helpers
# ----------------------------
def clamp(x, lo, hi):
    return max(lo, min(hi, x))

def dt_range(start: datetime, end: datetime, step: timedelta):
    cur = start
    while cur <= end:
        yield cur
        cur += step

def write_csv(path: Path, header, rows):
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(header)
        w.writerows(rows)

# ----------------------------
# Generate Source 1: Traffic CSV
# ----------------------------
def generate_traffic_csv(outdir: Path):
    """
    Schema:
      zone_id,timestamp,average_speed_kmh,traffic_volume,occupancy_rate

    Patterns:
      - morning peak: strongest congestion around ~07:45-09:00 for some zones
      - recovery afterwards
      - moderate imperfections: missing values + outliers
    """
    random.seed(SEED)

    timestamps = list(dt_range(TRAFFIC_START, TRAFFIC_END, timedelta(minutes=TRAFFIC_STEP_MIN)))

    # Zone profiles (different base speeds/volumes)
    # Higher congestion in Z1, Z6; low congestion in Z7; mixed in Z4, Z8
    zone_profile = {
        "Z1": {"base_speed": 52, "base_vol": 140, "peak_strength": 0.55},
        "Z2": {"base_speed": 56, "base_vol": 120, "peak_strength": 0.40},
        "Z3": {"base_speed": 58, "base_vol": 110, "peak_strength": 0.30},
        "Z4": {"base_speed": 54, "base_vol": 130, "peak_strength": 0.35},
        "Z5": {"base_speed": 60, "base_vol": 90,  "peak_strength": 0.20},
        "Z6": {"base_speed": 50, "base_vol": 150, "peak_strength": 0.60},
        "Z7": {"base_speed": 62, "base_vol": 75,  "peak_strength": 0.10},
        "Z8": {"base_speed": 55, "base_vol": 115, "peak_strength": 0.33},
    }

    # Peak center time for a smooth congestion curve
    peak_center = datetime(2025, 3, 10, 8, 20)
    peak_sigma_minutes = 50  # controls width of peak

    rows = []
    for z in ZONES:
        prof = zone_profile[z]
        for ts in timestamps:
            # Gaussian-like peak factor (0..1)
            diff_min = abs((ts - peak_center).total_seconds()) / 60.0
            peak_factor = math.exp(-(diff_min ** 2) / (2 * (peak_sigma_minutes ** 2)))

            # Speed drops during peak; volume and occupancy increase
            speed = prof["base_speed"] * (1.0 - prof["peak_strength"] * peak_factor)
            speed += random.uniform(-1.5, 1.5)  # small noise
            speed = clamp(speed, 8, 80)

            volume = prof["base_vol"] * (1.0 + 0.9 * prof["peak_strength"] * peak_factor)
            volume += random.uniform(-8, 8)
            volume = int(clamp(volume, 20, 450))

            # Occupancy correlated with volume and peak
            occ = 0.18 + (volume / 500.0) + 0.25 * prof["peak_strength"] * peak_factor
            occ += random.uniform(-0.02, 0.02)
            occ = clamp(occ, 0.05, 0.99)

            rows.append([z, ts.strftime("%Y-%m-%d %H:%M"), round(speed, 1), volume, round(occ, 2)])

    # Inject moderate imperfections (missing + outliers), but still fixable
    total = len(rows)
    miss_count = int(total * TRAFFIC_MISSING_RATE)
    out_count = int(total * TRAFFIC_OUTLIER_RATE)

    # Missing: blank out some average_speed_kmh OR occupancy_rate
    miss_indices = random.sample(range(total), miss_count)
    for i in miss_indices:
        # choose one field to blank
        if random.random() < 0.6:
            rows[i][2] = ""  # missing speed
        else:
            rows[i][4] = ""  # missing occupancy

    # Outliers: rare unrealistic speed or occupancy slightly > 1.0
    out_indices = random.sample([i for i in range(total) if i not in miss_indices], out_count)
    for i in out_indices:
        if random.random() < 0.5:
            rows[i][2] = random.choice([2.0, 4.5, 120.0])  # extreme speed outlier
        else:
            rows[i][4] = round(random.uniform(1.01, 1.10), 2)  # occupancy > 1 (cleaning case)

    header = ["zone_id", "timestamp", "average_speed_kmh", "traffic_volume", "occupancy_rate"]
    path = outdir / "traffic_data.csv"
    write_csv(path, header, rows)
    return path, total

# ----------------------------
# Generate Source 2: Bus GeoJSON
# ----------------------------
def generate_bus_geojson(outdir: Path):
    """
    GeoJSON FeatureCollection of Point features.
    Uses 'area_code' (synonym) instead of zone_id.

    Each feature properties:
      - bus_id
      - line_id
      - area_code   (semantic synonym of zone_id)
      - timestamp
      - delay_minutes
      - speed_kmh
    """
    random.seed(SEED + 1)

    # Define lines and which zones they typically cross
    lines = [f"L{i}" for i in range(1, 9)]  # 8 lines
    line_zones = {
        "L1": ["Z1", "Z2", "Z4"],
        "L2": ["Z6", "Z1", "Z8"],
        "L3": ["Z3", "Z4", "Z5"],
        "L4": ["Z7", "Z5", "Z8"],
        "L5": ["Z2", "Z3", "Z6"],
        "L6": ["Z8", "Z4", "Z1"],
        "L7": ["Z5", "Z2", "Z7"],
        "L8": ["Z6", "Z3", "Z8"],
    }

    # Rough bounding boxes per zone (fake city coordinates)
    # (lat, lon) centers
    zone_center = {
        "Z1": (33.590, -7.620),
        "Z2": (33.600, -7.610),
        "Z3": (33.585, -7.605),
        "Z4": (33.575, -7.615),
        "Z5": (33.565, -7.600),
        "Z6": (33.610, -7.630),
        "Z7": (33.555, -7.625),
        "Z8": (33.595, -7.595),
    }

    start_time = TRAFFIC_START  # align day
    features = []

    # Create buses
    buses = []
    for b in range(1, BUS_COUNT + 1):
        bus_id = f"BUS-{b:02d}"
        line_id = random.choice(lines)
        buses.append((bus_id, line_id))

    # Generate points per bus
    for bus_id, line_id in buses:
        zones_path = line_zones[line_id]
        # spread points over time
        t0 = start_time + timedelta(minutes=random.randint(0, 30))
        # initial delay baseline influenced by whether route hits congested zones
        base_delay = 2
        if "Z1" in zones_path or "Z6" in zones_path:
            base_delay = 6

        for k in range(BUS_POINTS_PER_BUS):
            ts = t0 + timedelta(seconds=BUS_TIME_STEP_SEC * k)

            # choose zone segment (simulate movement along path)
            seg = (k // max(1, (BUS_POINTS_PER_BUS // len(zones_path)))) % len(zones_path)
            z = zones_path[seg]

            # Create coordinates near zone center
            lat0, lon0 = zone_center[z]
            lat = lat0 + random.uniform(-0.0025, 0.0025)
            lon = lon0 + random.uniform(-0.0025, 0.0025)

            # speed lower in congested zones
            speed = random.uniform(18, 35)
            if z in ("Z1", "Z6"):
                speed = random.uniform(8, 20)

            # delay increases through congested zones and peak time
            peak_bonus = 0
            if datetime(2025, 3, 10, 7, 45) <= ts <= datetime(2025, 3, 10, 9, 0):
                peak_bonus = 3

            delay = base_delay + peak_bonus
            if z in ("Z1", "Z6"):
                delay += random.randint(2, 7)
            else:
                delay += random.randint(-1, 3)

            delay = int(clamp(delay, 0, 35))

            # introduce some missing delay (cleaning case)
            if random.random() < BUS_MISSING_DELAY_RATE:
                delay_value = None
            else:
                delay_value = delay

            area_code = ZONE_TO_AREA_CODE[z]  # synonym instead of zone_id

            feat = {
                "type": "Feature",
                "geometry": {
                    "type": "Point",
                    "coordinates": [round(lon, 6), round(lat, 6)]  # GeoJSON = [lon, lat]
                },
                "properties": {
                    "bus_id": bus_id,
                    "line_id": line_id,
                    "area_code": area_code,
                    "timestamp": ts.strftime("%Y-%m-%d %H:%M:%S"),
                    "delay_minutes": delay_value,
                    "speed_kmh": round(speed, 1),
                }
            }
            features.append(feat)

    geojson = {"type": "FeatureCollection", "features": features}

    outdir.mkdir(parents=True, exist_ok=True)
    path = outdir / "bus_gps.geojson"
    with path.open("w", encoding="utf-8") as f:
        json.dump(geojson, f, ensure_ascii=False, indent=2)

    return path, len(features)

# ----------------------------
# Generate Source 3: Planning TXT
# ----------------------------
def generate_planning_txt(outdir: Path):
    """
    Semi-structured TXT (one record per line), uses 'service_zone' synonym.
    Each line example:
      line_id=L2 | service_zone=ServiceZone-Z1 | day_type=weekday | scheduled_time=07:30 | frequency_min=10

    We'll generate ~180 lines with:
      - 8 lines (L1..L8)
      - day_type weekday/weekend
      - times across morning range
      - multiple zones per line (using synonym term)
    """
    random.seed(SEED + 2)

    lines = [f"L{i}" for i in range(1, 9)]
    day_types = ["weekday", "weekend"]

    # Candidate scheduled times (every 10 minutes from 07:00 to 12:50)
    base = datetime(2025, 3, 10, 7, 0)
    times = [(base + timedelta(minutes=10 * i)).strftime("%H:%M") for i in range(0, 36)]  # 6 hours => 36 times

    # Frequency options
    freq_options = [6, 8, 10, 12, 15]

    records = []
    # Build records until PLANNING_LINES
    while len(records) < PLANNING_LINES:
        line_id = random.choice(lines)
        z = random.choice(ZONES)
        service_zone = ZONE_TO_SERVICE_ZONE[z]  # synonym instead of zone_id
        day_type = random.choice(day_types)
        scheduled_time = random.choice(times)

        # Make frequencies more frequent on weekdays and peak-ish times
        hh = int(scheduled_time.split(":")[0])
        peak = (7 <= hh <= 9)
        if day_type == "weekday" and peak:
            frequency = random.choice([6, 8, 10])
        else:
            frequency = random.choice(freq_options)

        # Slight formatting variability (still parseable)
        sep = " | " if random.random() < 0.85 else " ; "

        line = (
            f"line_id={line_id}{sep}"
            f"service_zone={service_zone}{sep}"
            f"day_type={day_type}{sep}"
            f"scheduled_time={scheduled_time}{sep}"
            f"frequency_min={frequency}"
        )
        records.append(line)

    outdir.mkdir(parents=True, exist_ok=True)
    path = outdir / "planning.txt"
    with path.open("w", encoding="utf-8") as f:
        for line in records:
            f.write(line + "\n")

    return path, len(records)

# ----------------------------
# Also export a mapping file (helps Part 3 / cleaning)
# ----------------------------
def generate_zone_mapping(outdir: Path):
    """
    Optional but very useful for your report:
    A mapping table showing synonym alignment between actors.
    """
    header = ["zone_id", "area_code", "service_zone"]
    rows = []
    for z in ZONES:
        rows.append([z, ZONE_TO_AREA_CODE[z], ZONE_TO_SERVICE_ZONE[z]])
    path = outdir / "zone_mapping.csv"
    write_csv(path, header, rows)
    return path, len(rows)

# ----------------------------
# Main
# ----------------------------
def main():
    OUTDIR.mkdir(parents=True, exist_ok=True)

    t_path, t_rows = generate_traffic_csv(OUTDIR)
    b_path, b_feats = generate_bus_geojson(OUTDIR)
    p_path, p_lines = generate_planning_txt(OUTDIR)
    m_path, m_rows = generate_zone_mapping(OUTDIR)

    print("✅ Generated files:")
    print(f" - {t_path}  (rows: {t_rows})")
    print(f" - {b_path}  (features: {b_feats})")
    print(f" - {p_path}  (lines: {p_lines})")
    print(f" - {m_path}  (rows: {m_rows})")
    print("\nNotes:")
    print(" - Traffic includes ~3% missing values and ~1% outliers (intentional, fixable).")
    print(" - Bus GeoJSON uses 'area_code' instead of 'zone_id' (semantic synonym).")
    print(" - Planning TXT uses 'service_zone' instead of 'zone_id' (semantic synonym).")

if __name__ == "__main__":
    main()
