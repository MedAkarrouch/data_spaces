# Urban Mobility Data Space – Synthetic Dataset

## 1. Project Overview

This repository contains **synthetic datasets** generated for an **academic Data Space project** focused on **urban mobility**.

### Objective
To study **road congestion** and its **impact on public transport (buses)** by integrating **heterogeneous data sources** produced by **independent actors**, following **Data Space principles**.

The datasets are **intentionally imperfect** to:
- demonstrate **semantic interoperability**
- justify **data cleaning and alignment**
- reflect **real‑world data heterogeneity**

---

## 2. Data Space Context

This project simulates a **Mobility Data Space** where:
- data is produced by **different organizations**
- each actor uses **its own format and vocabulary**
- interoperability is achieved through **semantic alignment**, not centralization

### Independent data providers:
1. **Traffic Authority** → road traffic conditions  
2. **Public Transport Operator** → bus GPS data  
3. **Transport Planning Authority** → planned schedules  

---

## 3. Dataset Structure

The dataset consists of **four files**, each representing a different actor and perspective.
generated_sources/
├── traffic_data.csv
├── bus_gps.geojson
├── planning.txt
└── zone_mapping.csv

---

## 4. File Descriptions

---

### 1. `traffic_data.csv` — Road Traffic Data

**Producer:** Traffic authority  
**Format:** CSV  
**Granularity:** 5‑minute time windows  
**Size:** ~550 rows  

#### Description
Represents **road traffic conditions** per geographic zone over time.

#### Attributes
| Column | Description |
|------|------------|
| `zone_id` | Geographic zone identifier |
| `timestamp` | Time of measurement |
| `average_speed_kmh` | Average vehicle speed |
| `traffic_volume` | Number of vehicles |
| `occupancy_rate` | Road occupancy ratio |

#### Notes
- Includes **peak congestion periods**
- Contains **missing values** and **outliers** (intentional)
- Used to identify **where and when congestion occurs**

---

### 2. `bus_gps.geojson` — Bus GPS Data

**Producer:** Public transport operator  
**Format:** GeoJSON  
**Geometry:** `Point`  
**Size:** ~1,800 features  

#### Description
Represents **actual bus movements** and operational conditions.

#### Feature Properties
| Property | Description |
|--------|------------|
| `bus_id` | Bus identifier |
| `line_id` | Bus line |
| `area_code` | Zone identifier (**synonym of `zone_id`**) |
| `timestamp` | Observation time (1‑minute frequency) |
| `delay_minutes` | Bus delay |
| `speed_kmh` | Bus speed |

#### Notes
- Uses **`area_code` instead of `zone_id`** (semantic mismatch)
- Higher temporal resolution than traffic data
- Some missing delay values (intentional)

---

### 3. `planning.txt` — Transport Planning Data

**Producer:** Transport planning authority  
**Format:** TXT (semi‑structured)  
**Size:** ~180 lines  

#### Description
Represents **how public transport is supposed to operate** under normal conditions.

#### Example Line
line_id=L2 | service_zone=ServiceZone-Z1 | day_type=weekday | scheduled_time=07:30 | frequency_min=10

#### Attributes
- `line_id`
- `service_zone` (**synonym of `zone_id`**)
- `day_type` (weekday / weekend)
- `scheduled_time`
- `frequency_min`

#### Notes
- Reference dataset (no real‑time timestamps)
- Different vocabulary and format
- Used to compare **planned vs actual** behavior

---

### 4. `zone_mapping.csv` — Semantic Mapping File

**Purpose:** Semantic interoperability support  
**Format:** CSV  

#### Description
Maps **different zone vocabularies** used by the actors to the same semantic concept.

| zone_id | area_code | service_zone |
|-------|----------|--------------|
| Z1 | A01 | ServiceZone‑Z1 |
| Z2 | A02 | ServiceZone‑Z2 |
| … | … | … |

#### Importance
- Resolves **synonyms**
- Enables **ontology definition**
- Supports **semantic alignment and RDF/SPARQL reasoning**

---

## 5. How the Files Relate

### Shared semantic concept
- **Zone**
  - `zone_id` (traffic)
  - `area_code` (bus)
  - `service_zone` (planning)

### Logical relationship
Planning data → defines expected behavior
Bus GPS data → shows real behavior
Traffic data → explains deviations

---

## 6. Data Quality & Imperfections (Intentional)

The datasets include:
- Missing values
- Outliers
- Different temporal granularities
- Different vocabularies
- Different file formats

These imperfections are **intentional** and are used to:
- demonstrate **data cleaning**
- justify **semantic interoperability**
- apply **Data Space concepts**

---

## 7. Intended Use

These datasets are designed for:
- Academic projects
- Data Space demonstrations
- Semantic interoperability analysis
- Data integration pipelines
- Ontology and RDF/SPARQL modeling (conceptual or practical)

They are **not real data** and should not be used for operational decision‑making.

---

## 8. Key Takeaway

> This dataset illustrates how heterogeneous, imperfect, and independently produced data can be integrated using Data Space principles to analyze the impact of road congestion on public transport.


