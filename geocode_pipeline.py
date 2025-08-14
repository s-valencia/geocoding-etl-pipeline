"""
Geocoding + Reverse Geocoding ETL (Portfolio-Ready)
- Auto-detects forward or reverse mode based on columns present.
- Uses geopy's ArcGIS geocoder by default; falls back to Nominatim.
- Adds locator provenance and raw payload columns.
- Writes CSV and GeoJSON outputs.

Usage:
    python geocode_pipeline.py input.csv output_basename

Outputs:
    output_basename.csv
    output_basename.geojson
"""

import sys, time, json, math, csv, os
from typing import Optional, Tuple
import pandas as pd
from geopy.geocoders import ArcGIS, Nominatim
from geopy.extra.rate_limiter import RateLimiter
from geopy.exc import GeocoderTimedOut, GeocoderServiceError

# ---------- Config ----------
# Requests per second (be polite to public geocoders)
RPS = 1.0
RETRY_TIMES = 3
RETRY_SLEEP = 2.0

# Columns (case-insensitive match will be applied)
COL_ADDRESS = "address"
COL_LAT = "latitude"
COL_LON = "longitude"

# ---------- Helpers ----------
def has_columns(df: pd.DataFrame, cols) -> bool:
    low = {c.lower() for c in df.columns}
    return all(c.lower() in low for c in cols)

def to_float(v) -> Optional[float]:
    try:
        if v is None or (isinstance(v, float) and math.isnan(v)):
            return None
        return float(v)
    except Exception:
        return None

def build_geojson(df: pd.DataFrame, lat_col: str, lon_col: str, props_cols: list) -> dict:
    features = []
    for _, r in df.iterrows():
        lat = to_float(r.get(lat_col))
        lon = to_float(r.get(lon_col))
        if lat is None or lon is None:
            continue
        props = {c: (None if pd.isna(r.get(c)) else r.get(c)) for c in props_cols}
        features.append({
            "type": "Feature",
            "geometry": {"type": "Point", "coordinates": [lon, lat]},
            "properties": props
        })
    return {"type": "FeatureCollection", "features": features}

def safe_json(obj) -> str:
    try:
        return json.dumps(obj, ensure_ascii=False)
    except Exception:
        return json.dumps({"_raw_str": str(obj)})

# ---------- Geocoders ----------
def get_geocoders():
    arc = ArcGIS(timeout=10)
    nom = Nominatim(user_agent="sv-portfolio-geocoder", timeout=10)
    # Wrap in rate limiters
    geocode_arc = RateLimiter(arc.geocode, min_delay_seconds=1.0 / RPS)
    reverse_arc = RateLimiter(arc.reverse, min_delay_seconds=1.0 / RPS)
    geocode_nom = RateLimiter(nom.geocode, min_delay_seconds=1.0 / RPS)
    reverse_nom = RateLimiter(nom.reverse, min_delay_seconds=1.0 / RPS)
    return (geocode_arc, reverse_arc, geocode_nom, reverse_nom)

def try_forward(geocode_fn, q) -> Optional[Tuple[float, float, str, dict]]:
    loc = geocode_fn(q)
    if not loc:
        return None
    return (loc.latitude, loc.longitude, getattr(loc, "address", None), getattr(loc, "raw", {}))

def try_reverse(reverse_fn, lat, lon) -> Optional[Tuple[str, dict]]:
    loc = reverse_fn((lat, lon), exactly_one=True)
    if not loc:
        return None
    return (getattr(loc, "address", None), getattr(loc, "raw", {}))

# ---------- Pipeline ----------
def geocode_forward(df: pd.DataFrame) -> pd.DataFrame:
    geocode_arc, _, geocode_nom, _ = get_geocoders()
    out_rows = []
    for idx, row in df.iterrows():
        q = str(row[COL_ADDRESS])
        lat = lon = matched = None
        raw = {}
        locator = None

        for attempt in range(RETRY_TIMES):
            try:
                hit = try_forward(geocode_arc, q)
                if hit:
                    lat, lon, matched, raw = hit
                    locator = "ArcGIS"
                    break
            except (GeocoderTimedOut, GeocoderServiceError):
                time.sleep(RETRY_SLEEP)

        if lat is None:
            # fallback to Nominatim
            for attempt in range(RETRY_TIMES):
                try:
                    hit = try_forward(geocode_nom, q)
                    if hit:
                        lat, lon, matched, raw = hit
                        locator = "Nominatim"
                        break
                except (GeocoderTimedOut, GeocoderServiceError):
                    time.sleep(RETRY_SLEEP)

        out_rows.append({
            **row.to_dict(),
            "matched_address": matched,
            "latitude": lat,
            "longitude": lon,
            "locator": locator,
            "raw_payload": safe_json(raw)
        })
    return pd.DataFrame(out_rows)

def geocode_reverse(df: pd.DataFrame) -> pd.DataFrame:
    _, reverse_arc, _, reverse_nom = get_geocoders()
    out_rows = []
    for idx, row in df.iterrows():
        lat = to_float(row[COL_LAT])
        lon = to_float(row[COL_LON])
        matched = None
        raw = {}
        locator = None

        # Skip invalid coords
        if lat is None or lon is None:
            out_rows.append({**row.to_dict(),
                             "matched_address": None,
                             "locator": None,
                             "raw_payload": None})
            continue

        for attempt in range(RETRY_TIMES):
            try:
                hit = try_reverse(reverse_arc, lat, lon)
                if hit:
                    matched, raw = hit
                    locator = "ArcGIS"
                    break
            except (GeocoderTimedOut, GeocoderServiceError):
                time.sleep(RETRY_SLEEP)

        if matched is None:
            for attempt in range(RETRY_TIMES):
                try:
                    hit = try_reverse(reverse_nom, lat, lon)
                    if hit:
                        matched, raw = hit
                        locator = "Nominatim"
                        break
                except (GeocoderTimedOut, GeocoderServiceError):
                    time.sleep(RETRY_SLEEP)

        out_rows.append({
            **row.to_dict(),
            "matched_address": matched,
            "locator": locator,
            "raw_payload": safe_json(raw)
        })
    return pd.DataFrame(out_rows)

def main():
    if len(sys.argv) != 3:
        print("Usage: python geocode_pipeline.py <input.csv> <output_basename>")
        sys.exit(1)

    in_path = sys.argv[1]
    out_base = os.path.splitext(sys.argv[2])[0]
    df = pd.read_csv(in_path)

    # Normalize headers (case-insensitive handling)
    df.columns = [c.strip() for c in df.columns]

    forward_mode = COL_ADDRESS in [c.lower() for c in df.columns]
    reverse_mode = (COL_LAT in [c.lower() for c in df.columns]) and (COL_LON in [c.lower() for c in df.columns])

    # Map lower->actual names
    lower_map = {c.lower(): c for c in df.columns}

    if forward_mode and reverse_mode:
        print("Found both address and lat/lon columns. Using forward geocoding (address).")
    elif not forward_mode and not reverse_mode:
        print("Input must contain 'address' OR 'latitude' + 'longitude' columns.")
        sys.exit(1)

    if forward_mode:
        # Ensure we use the exact column name casing from file
        global COL_ADDRESS
        COL_ADDRESS = lower_map[COL_ADDRESS]
        result = geocode_forward(df)
        lat_col, lon_col = "latitude", "longitude"
    else:
        # Ensure exact casing
        global COL_LAT, COL_LON
        COL_LAT, COL_LON = lower_map[COL_LAT], lower_map[COL_LON]
        result = geocode_reverse(df)
        lat_col, lon_col = COL_LAT, COL_LON

    # Write CSV
    csv_out = f"{out_base}.csv"
    result.to_csv(csv_out, index=False)

    # Write GeoJSON (only if we have lat/lon)
    if lat_col in result.columns and lon_col in result.columns:
        gj = build_geojson(result, lat_col, lon_col,
                           [c for c in result.columns if c not in (lat_col, lon_col)])
        gj_out = f"{out_base}.geojson"
        with open(gj_out, "w", encoding="utf-8") as f:
            json.dump(gj, f, ensure_ascii=False)
        print(f"Wrote: {csv_out} and {gj_out}")
    else:
        print(f"Wrote: {csv_out} (no coordinates present, GeoJSON skipped)")

if __name__ == "__main__":
    main()