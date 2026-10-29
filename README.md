# Energy air quality analysis

This repository contains an end-to-end analysis on the impact of power plant fuel usage on air quality readings. We want to find out if readings of sulfur oxides (SOx), nitrogen oxides (NOx) and particulate matter (PM) differ considerably depending on which power plants are close by.


## Data sources

This project uses the following datasets for its analysis:

**Dataset:** Global Power Plant Database (Version 1.3.0) <br>
**License:** [Creative Commons Attribution 4.0 (CC BY 4.0)](https://creativecommons.org/licenses/by/4.0/). Data was filtered and aggregated for this project. <br>
**Citation:** Global Energy Observatory, Google, KTH Royal Institute of Technology in Stockholm, Enipedia, World Resources Institute. 2021. Global Power Plant Database version 1.3.0. Accessed through https://datasets.wri.org/datasets/global-power-plant-database on September 4th, 2026.

**Dataset:** Global Volcano Locations Database <br>
**Citation:** NCEI Volcano Location Database. NOAA National Centers for Environmental Information.

**Dataset:** [OpenAQ](https://openaq.org) (retrieved via the OpenAQ API (v3) and the OpenAQ AWS Open Data archive on October 2026) <br>
**License:** The providers and licenses of the stations used are listed in the table below. Data was filtered, cleaned and aggregated for this project. <br>
**Citation:** OpenAQ and data providers (see table below). Air quality data. Retrieved from https://openaq.org, October 2026.

The ELT and analysis was built using the following OpenAQ data providers
<details>
    <summary>Data providers and licenses</summary>
    <table>
        <tr>
            <td><b>Provider (as listed by OpenAQ)</b></td>
            <td><b>License</b></td>
            <td><b>Stations used (Location Id)</b></td>
        </tr>
        <tr>
            <td>Unknown Governmental Organization</td>
            <td><a href="https://opendatacommons.org/licenses/by/1-0/">ODC-BY</a></td>
            <td>2669, 2935, 3068, 3078, 3093, 3099, 3158, 3180, 3187, 3202, 3915, 4557, 4701, 4751, 4752</td>
        </tr>
    </table>
</details>

<br>

_OpenAQ's metadata for these stations names the license (ODC-BY) but gives no license URL, so the link above is to the standard ODC-BY 1.0 text._


## Initial analysis

Before air quality readings are obtained, a list of areas to obtain data from must be established first.

`power_plant_analysis.ipynb` contains an extensive analysis on Power Plant distributions and uses clustering metrics to detect where different fuel pollution levels are separated, thus reducing the likelihood of interference in the final obtained data.


## Architecture

Data ingestion is made through Airflow DAGs. These are:
 - `import_power_plants.py`: Reads the Global Power Plant Database csv file in `./data` and imports it to DuckDB;
 - `openaq_location_info.py`: Obtains OpenAQ station info from the API. Skips entries already obtained;
 - `openaq_s3_ingest.py`: Obtains historical OpenAQ readings for a given station and year. Skips days already obtained;
 - `ingest_controller.py`: Calls S3 ingest for every specified (station, year) combination on `.env`.

Afterwards, dbt will handle the transformation step. There are 3 layers:
 - `staging`: almost direct copy of every raw table (some columns are renamed);
 - `intermediate`: performs data cleaning and time aggregation (hourly to daily);
 - `mart`: final tables for the dashboard, including a monthly aggregation.


## Configuring and running the ELT

OpenAQ locations and years of data can be set with the `LOCATIONS` and `YEARS` environment values. Afterwards the extract and load steps can be run with docker compose:

    docker compose up

Afterwards, go to Airflow on localhost (port 8080), and login (`admin` user, and the password will be on the docker compose logs). The 3 ingestion DAGs (`import_power_plants`, `openaq_location_info`, `ingest_controller`) can be run in no particular order.

An OpenAQ API key is needed for the location information ingestion step, as that data is obtained from the API, and should be added to `.env` under `OPEN_AQ_API_KEY`. The historical data, obtained from S3, should not require anything.

dbt's `profiles.yml` is not included in this repository, as well as any data. The Global Power Plant Database csv file must be placed in `./data/` so it can be detected by Airflow. dbt's profile must be configured to point to the local DuckDB file created during ingestion (`/data/aq_raw.db` by default), and a PostgreSQL connection under `attach` (for `prod` only) as this is the mart database that the dashboard reads from. Builds under `dev`, the default target, will write back to the same DuckDB database for easier debugging, and so do not need references to PostgreSQL.

After setting up dbt, connections can be tested with `dbt debug`:

    dbt debug --target dev
    dbt debug --target prod

**Note:** Since `prod` writes to PostgreSQL, please ensure docker compose is running. For `dev`, this is not needed.

Finally, to build the pipeline, run:

    dbt build --target dev
    dbt build --target prod
