# Energy air quality analysis

This repository contains an ongoing analysis on the impact of power plant fuel usage on air quality readings. We want to find out if readings of sulfur oxides (SOx), nitrogen oxides (NOx) and particulate matter (PM) differ considerably depending on which power plants are close by.


## Data sources

This project uses the following datasets for its analysis:

**Dataset:** Global Power Plant Database (Version 1.3.0) <br>
**License:** [Creative Commons Attribution 4.0 (CC BY 4.0)](https://creativecommons.org/licenses/by/4.0/) <br>
**Citation:** Global Energy Observatory, Google, KTH Royal Institute of Technology in Stockholm, Enipedia, World Resources Institute. 2021. Global Power Plant Database version 1.3.0. Accessed through https://datasets.wri.org/datasets/global-power-plant-database on September 4th, 2026.

**Dataset:** Global Volcano Locations Database <br>
**Citation:** NCEI Volcano Location Database. NOAA National Centers for Environmental Information.


## Initial analysis

Before air quality readings are obtained, a list of areas to obtain data from must be established first.

`power_plant_analysis.ipynb` contains an extensive analysis on Power Plant distributions and uses clustering metrics to detect where different fuel pollution levels are separated, thus reducing the likelihood of interference in the final obtained data.

