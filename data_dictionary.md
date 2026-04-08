# Data Dictionary - OSCARS Regional State of the Climate

This data dictionary defines the structure, meaning, and format of the climate data and indices produced by the OSCARS Ingestion Pipeline.

## 1. Dataset Overview

The dataset provides harmonized regional climate data and climate-extreme indices derived from the **ERA5 reanalysis**. The data includes core meteorological variables and derived climate indices, spatially aggregated over European regions (NUTS classification) or provided in their original gridded format.

## 2. Naming Convention

Filenames for the Zarr data stores follow this convention:
`{variable}_{pressure_level}_{dataset}_{region_set}.zarr`

- **variable**: The short name of the variable or index (e.g., `tas`, `tx35`).
- **pressure_level**: For surface variables, this is always `None`.
- **dataset**: The foundational dataset (e.g., `ERA5`).
- **region_set**: The spatial grouping. Options include:
  - `gridded`: Original 0.25° x 0.25° grid.
  - `NUTS-0`: Country level.
  - `NUTS-1`: Major socio-economic regions.
  - `NUTS-2`: Basic regions for regional policies.
  - `NUTS-3`: Small regions for specific diagnoses.

______________________________________________________________________

## 3. Core Variables

Core variables represent fundamental meteorological quantities. They are provided at a **daily** temporal resolution.

| Variable Name | Long Name | Description | Units | Data Type | Range/Constraints |
| :--- | :--- | :--- | :--- | :--- | :--- |
| **tas** | Mean temperature | Daily mean air temperature at 2 m | Celsius | float32 | Typically -50 to 50 |
| **tasmin** | Minimum temperature | Daily minimum air temperature at 2 m | Celsius | float32 | Typically -60 to 40 |
| **tasmax** | Maximum temperature | Daily maximum air temperature at 2 m | Celsius | float32 | Typically -40 to 60 |
| **pr** | Total precipitation | Daily accumulated liquid and frozen precipitation | mm | float32 | ≥ 0 |
| **sfcWind** | Surface Wind Speed | Magnitude of the horizontal wind vector at 10 m | m s-1 | float32 | ≥ 0 |

______________________________________________________________________

## 4. Derived Indices

Climate indices are calculated from core variables and represent counts of events or specific indicators. They are provided for multiple temporal aggregations (Annual, Monthly, Seasonal).

| Variable Name | Long Name | Description | Units | Data Type | Calculation Basis |
| :--- | :--- | :--- | :--- | :--- | :--- |
| **fd** | Frost days | Number of days when `tasmin` \< 0 °C | days | float32 | tasmin |
| **tr20** | Tropical nights | Number of nights when `tasmin` > 20 °C | days | float32 | tasmin |
| **tr25** | Equatorial nights | Number of nights when `tasmin` > 25 °C | days | float32 | tasmin |
| **tx30** | Hot days (30°C) | Number of days when `tasmax` > 30 °C | days | float32 | tasmax |
| **tx35** | Hot days (35°C) | Number of days when `tasmax` > 35 °C | days | float32 | tasmax |
| **tx40** | Hot days (40°C) | Number of days when `tasmax` > 40 °C | days | float32 | tasmax |
| **r1mm** | Wet days | Number of days when `pr` ≥ 1 mm | days | float32 | pr |
| **r20mm** | Heavy precipitation days | Number of days when `pr` ≥ 20 mm | days | float32 | pr |
| **r95ptot** | Extreme precipitation fraction | Precipitation from days > 95th percentile relative to total | 1 (fraction) | float32 | pr |

______________________________________________________________________

## 5. Dimensions and Coordinates

### Gridded Datasets (`gridded`)

- **time**: Date of observation (Daily).
- **lat**: Latitude in decimal degrees (North positive).
- **lon**: Longitude in decimal degrees (East positive).

### Regional Datasets (`NUTS-x`)

- **time**: Start date of the aggregation period.
- **time_filter**: (For Indices only) The aggregation period identifier (e.g., `Annual`, `Jan`, `MAM`).
- **region**: NUTS code (e.g., `CZ020`, `ES300`).

______________________________________________________________________

## 6. Relationships and Origin

- **Source Data**: ECMWF ERA5 Reanalysis (0.25° resolution).
- **Relationship**: Regional datasets (`NUTS-x`) are derived from gridded data through area-weighted averaging within the administrative boundaries.
- **Consistency**: All variables and indices follow the CF (Climate and Forecast) conventions and Dublin Core metadata standards for improved interoperability.
- **Governance**: Managed by the OSCARS - RSOTC project research group.
