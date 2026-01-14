# generate_ttl.py
import pandas as pd
from rdflib import Graph, Namespace, Literal, RDF, RDFS, XSD
from rdflib.namespace import SKOS, GEO
import os
import urllib.parse

# === NAMESPACES ===
MOB = Namespace("https://purl.org/mobility/ontology#")
EX = Namespace("https://example.org/mobility/data/")
SKOS_NS = SKOS
GEO_NS = Namespace("http://www.opengis.net/ont/geosparql#")
UNIT = Namespace("http://qudt.org/vocab/unit/")

# === UTILS ===
def to_uri(base, value):
    if pd.isna(value) or value == "":
        return None
    safe_value = str(value).replace(" ", "_").replace(":", "-").replace(".", "-")
    safe_value = urllib.parse.quote(safe_value, safe="")
    return EX[f"{base}/{safe_value}"]

def safe_float(x):
    try:
        return float(x) if pd.notna(x) and x != '' else None
    except (ValueError, TypeError):
        return None

def safe_int(x):
    try:
        return int(float(x)) if pd.notna(x) and x != '' else None
    except (ValueError, TypeError):
        return None

def safe_bool(x):
    if pd.isna(x):
        return None
    x_str = str(x).lower().strip()
    return x_str in ("true", "1", "yes", "t")

def init_graph():
    g = Graph()
    g.bind("mob", MOB)
    g.bind("ex", EX)
    g.bind("skos", SKOS_NS)
    g.bind("geo", GEO_NS)
    g.bind("unit", UNIT)
    return g

# === 1. CONCEPTS SKOS (vocabulaire partagé) ===
def build_skos_concepts():
    g = init_graph()
    delay_categories = ["NO_DELAY", "MINOR_DELAY", "MODERATE_DELAY", "SEVERE_DELAY"]
    congestion_levels = ["FREE_FLOW", "MODERATE", "HEAVY", "CONGESTED"]
    
    for cat in delay_categories:
        uri = MOB[cat]
        g.add((uri, RDF.type, SKOS_NS.Concept))
        g.add((uri, SKOS_NS.prefLabel, Literal(cat.replace("_", " ").title(), lang="en")))
    
    for level in congestion_levels:
        uri = MOB[level]
        g.add((uri, RDF.type, SKOS_NS.Concept))
        g.add((uri, SKOS_NS.prefLabel, Literal(level.replace("_", " ").title(), lang="en")))
    
    return g

# === 2. ZONE_MAPPING → ServiceZone ===
def process_zone_mapping():
    print("Processing ZONE_MAPPING → zone_mapping.ttl")
    g = init_graph()
    df = pd.read_csv("data/ZONE_MAPPING.csv")
    for _, row in df.iterrows():
        zone_uri = to_uri("zone", row["ZONE_ID"])
        if not zone_uri:
            continue
        g.add((zone_uri, RDF.type, MOB.ServiceZone))
        g.add((zone_uri, MOB.hasAreaCode, Literal(row["AREA_CODE"])))
        g.add((zone_uri, RDFS.label, Literal(row["SERVICE_ZONE"])))
    return g

# === 3. PLANNING_CLEAN → Schedule ===
def process_planning_clean():
    print("Processing PLANNING_CLEAN → planning_clean.ttl")
    g = init_graph()
    df = pd.read_csv("data/PLANNING_CLEAN.csv")
    for _, row in df.iterrows():
        schedule_id = f"{row['LINE_ID']}_{row['ZONE_ID']}_{row['DAY_TYPE']}_{row['SCHEDULED_TIME']}"
        schedule_uri = to_uri("schedule", schedule_id)
        route_uri = to_uri("route", row["LINE_ID"])
        zone_uri = to_uri("zone", row["ZONE_ID"])
        if not all([schedule_uri, route_uri, zone_uri]):
            continue
        g.add((schedule_uri, RDF.type, MOB.Schedule))
        g.add((schedule_uri, MOB.belongsToRoute, route_uri))
        g.add((schedule_uri, MOB.appliesToZone, zone_uri))
        g.add((schedule_uri, MOB.dayType, Literal(row["DAY_TYPE"])))
        g.add((schedule_uri, MOB.scheduledTime, Literal(row["SCHEDULED_TIME"], datatype=XSD.time)))
        freq = safe_int(row["FREQUENCY_MIN"])
        if freq is not None:
            g.add((schedule_uri, MOB.frequencyMinutes, Literal(freq, datatype=XSD.integer)))
        is_peak = safe_bool(row["IS_PEAK_SCHEDULE"])
        if is_peak is not None:
            g.add((schedule_uri, MOB.isPeakSchedule, Literal(is_peak, datatype=XSD.boolean)))
    return g

# === 4. TRAFFIC_CLEAN → TrafficObservation ===
def process_traffic_clean():
    print("Processing TRAFFIC_CLEAN → traffic_clean.ttl")
    g = init_graph()
    df = pd.read_csv("data/TRAFFIC_CLEAN.csv")
    for _, row in df.iterrows():
        obs_id = f"{row['ZONE_ID']}_{row['TIMESTAMP']}"
        obs_uri = to_uri("traffic_obs", obs_id)
        zone_uri = to_uri("zone", row["ZONE_ID"])
        if not obs_uri or not zone_uri:
            continue
        g.add((obs_uri, RDF.type, MOB.TrafficObservation))
        g.add((obs_uri, MOB.observedInZone, zone_uri))
        g.add((obs_uri, MOB.observedAt, Literal(row["TIMESTAMP"], datatype=XSD.dateTime)))
        speed = safe_float(row["AVERAGE_SPEED_KMH"])
        if speed is not None:
            g.add((obs_uri, MOB.averageSpeed, Literal(speed, datatype=XSD.float)))
            g.add((obs_uri, MOB.speedUnit, UNIT["KiloM-PER-HR"]))
        vol = safe_int(row["TRAFFIC_VOLUME"])
        if vol is not None:
            g.add((obs_uri, MOB.trafficVolume, Literal(vol, datatype=XSD.integer)))
        occ = safe_float(row["OCCUPANCY_RATE"])
        if occ is not None:
            g.add((obs_uri, MOB.occupancyRate, Literal(occ, datatype=XSD.float)))
        if pd.notna(row["CONGESTION_LEVEL"]) and row["CONGESTION_LEVEL"] != "":
            level_uri = MOB[row["CONGESTION_LEVEL"]]
            g.add((obs_uri, MOB.congestionLevel, level_uri))
        is_cong = safe_bool(row["IS_CONGESTED"])
        if is_cong is not None:
            g.add((obs_uri, MOB.isCongested, Literal(is_cong, datatype=XSD.boolean)))
    return g

# === 5. BUS_GPS_CLEAN → Trip + Vehicle + Location ===
def process_bus_gps_clean():
    print("Processing BUS_GPS_CLEAN → bus_gps_clean.ttl")
    g = init_graph()
    df = pd.read_csv("data/BUS_GPS_CLEAN.csv")
    for _, row in df.iterrows():
        event_time_iso = row["EVENT_TIME"].replace(" ", "T") + "Z"
        trip_id = f"{row['BUS_ID']}_{event_time_iso}"
        trip_uri = to_uri("trip", trip_id)
        vehicle_uri = to_uri("vehicle", row["BUS_ID"])
        route_uri = to_uri("route", row["LINE_ID"])
        zone_uri = to_uri("zone", row["ZONE_ID"])
        if not all([trip_uri, vehicle_uri, route_uri, zone_uri]):
            continue
        g.add((trip_uri, RDF.type, MOB.Trip))
        g.add((trip_uri, MOB.hasVehicle, vehicle_uri))
        g.add((trip_uri, MOB.operatesOnRoute, route_uri))
        g.add((trip_uri, MOB.observedInZone, zone_uri))
        g.add((trip_uri, MOB.recordedAt, Literal(event_time_iso, datatype=XSD.dateTime)))
        g.add((vehicle_uri, RDF.type, MOB.Vehicle))
        g.add((vehicle_uri, RDFS.label, Literal(row["BUS_ID"])))
        lat = safe_float(row["LATITUDE"])
        lon = safe_float(row["LONGITUDE"])
        if lat is not None and lon is not None:
            point_uri = to_uri("point", trip_id)
            g.add((trip_uri, MOB.hasLocation, point_uri))
            g.add((point_uri, RDF.type, GEO_NS.Point))
            wkt = f"POINT({lon} {lat})"
            g.add((point_uri, GEO_NS.asWKT, Literal(wkt, datatype=GEO_NS.wktLiteral)))
        speed = safe_float(row["SPEED_KMH"])
        if speed is not None:
            g.add((trip_uri, MOB.instantaneousSpeed, Literal(speed, datatype=XSD.float)))
            g.add((trip_uri, MOB.speedUnit, UNIT["KiloM-PER-HR"]))
        is_delayed = safe_bool(row["IS_DELAYED"])
        if is_delayed is not None:
            g.add((trip_uri, MOB.isDelayed, Literal(is_delayed, datatype=XSD.boolean)))
        delay_min = safe_float(row["DELAY_MINUTES"])
        if delay_min is not None and delay_min > 0:
            event_uri = to_uri("delay_event", trip_id)
            g.add((trip_uri, MOB.hasDelayEvent, event_uri))
            g.add((event_uri, RDF.type, MOB.DelayEvent))
            g.add((event_uri, MOB.delayMinutes, Literal(delay_min, datatype=XSD.float)))
            g.add((event_uri, MOB.delayUnit, UNIT["MIN"]))
            if pd.notna(row["DELAY_CATEGORY"]) and row["DELAY_CATEGORY"] != "":
                cat_uri = MOB[row["DELAY_CATEGORY"]]
                g.add((event_uri, MOB.delayCategory, cat_uri))
    return g

# === 6. BUS_PERFORMANCE_HOURLY → AggregatedPerformance ===
def process_bus_performance_hourly():
    print("Processing BUS_PERFORMANCE_HOURLY → bus_performance_hourly.ttl")
    g = init_graph()
    df = pd.read_csv("data/BUS_PERFORMANCE_HOURLY.csv")
    for _, row in df.iterrows():
        perf_id = f"{row['LINE_ID']}_{row['ZONE_ID']}_{row['EVENT_DATE']}_{row['EVENT_HOUR']}"
        perf_uri = to_uri("performance", perf_id)
        route_uri = to_uri("route", row["LINE_ID"])
        zone_uri = to_uri("zone", row["ZONE_ID"])
        if not all([perf_uri, route_uri, zone_uri]):
            continue
        g.add((perf_uri, RDF.type, MOB.AggregatedPerformance))
        g.add((perf_uri, MOB.route, route_uri))
        g.add((perf_uri, MOB.zone, zone_uri))
        g.add((perf_uri, MOB.performanceDate, Literal(row["EVENT_DATE"], datatype=XSD.date)))
        hour = safe_int(row["EVENT_HOUR"])
        if hour is not None:
            g.add((perf_uri, MOB.performanceHour, Literal(hour, datatype=XSD.integer)))
        avg_delay = safe_float(row["AVG_DELAY_MINUTES"])
        if avg_delay is not None:
            g.add((perf_uri, MOB.averageDelayMinutes, Literal(avg_delay, datatype=XSD.float)))
        max_delay = safe_float(row["MAX_DELAY_MINUTES"])
        if max_delay is not None:
            g.add((perf_uri, MOB.maxDelayMinutes, Literal(max_delay, datatype=XSD.float)))
        avg_speed = safe_float(row["AVG_SPEED_KMH"])
        if avg_speed is not None:
            g.add((perf_uri, MOB.averageSpeedKmh, Literal(avg_speed, datatype=XSD.float)))
        delay_rate = safe_float(row["DELAY_RATE_PCT"])
        if delay_rate is not None:
            g.add((perf_uri, MOB.delayRatePercent, Literal(delay_rate, datatype=XSD.float)))
    return g

# === EXECUTION PRINCIPALE ===
if __name__ == "__main__":
    os.makedirs("ttl", exist_ok=True)
    
    # 1. Générer le vocabulaire SKOS (commun à tous)
    skos_graph = build_skos_concepts()
    skos_graph.serialize(destination="ttl/skos_concepts.ttl", format="turtle", encoding="utf-8")
    print(" Generated: ttl/skos_concepts.ttl")
    
    # 2. Générer chaque fichier TTL
    graphs = {
        "zone_mapping.ttl": process_zone_mapping(),
        "planning_clean.ttl": process_planning_clean(),
        "traffic_clean.ttl": process_traffic_clean(),
        "bus_gps_clean.ttl": process_bus_gps_clean(),
        "bus_performance_hourly.ttl": process_bus_performance_hourly(),
    }
    
    total_triples = 0
    for filename, graph in graphs.items():
        path = f"ttl/{filename}"
        graph.serialize(destination=path, format="turtle", encoding="utf-8")
        count = len(graph)
        total_triples += count
        print(f" Generated: {path} ({count} triplets)")
    
    print(f"\n Total triplets générés : {total_triples}")
    print(" Fichiers TTL prêts dans le dossier 'ttl/'")