# Data engineer Capstone Project

This repo contains the code for the capstone project of the data engineer nanodegree from udacity. In the scope of this project we investigate a dataset called US I94 about immigration under consideration of further data about demographics and temperature for a more comprehensive analysis of causes of immigration. We extract transform the data from different sources and load it into a data model using a star schema with a central fact table. The purpose of the generated data model is to allow an comprehensive analysis of the immigration to the United States under consideration of demographics and temperature. The model should furthermore be extenable with additional data. In the scope of the project we following the steps listed below:

1. Loading of data
2. Exploration and cleaning of data
3. Definition of data model
4. Loading of data into model
5. Quality checks


## Folder and file description
.
├── airport-codes_csv.csv - Dataset as csv of airport data
├── capstone_notebook.ipynb - Notebook containig code for data processing and modelling
├── check_quality.py - Contains code for quality check of sql tables
├── clean_data.py - Contains code for the cleaning of data
├── create_tables.py - Contains code for the creation of sql tables
├── I94_SAS_Labels_Descriptions.SAS - Dataset as parquet of immigrations data
├── immigration_data_sample.csv - Dataset sample as csv of immigrations data
├── README.md - README
└── us-cities-demographics.csv - Dataset as csv of us city demographics

## Datasets

We use data from four different datasets:

- I94 Immigration Data: This data comes from the [US National Tourism and Trade Office](https://www.trade.gov/national-travel-and-tourism-office); it includes information about immigrants and their departure airport. 
- World Temperature Data: This dataset came from [Kaggle](https://www.kaggle.com/berkeleyearth/climate-change-earth-surface-temperature-data); contains information about surface temperature of countries in the world.
- U.S. City Demographic Data: This data comes from [OpenSoft](https://public.opendatasoft.com/explore/dataset/us-cities-demographics/export/); contains information about the demographics of all US cities and census-designated places with a population greater or equal to 65,000. 
- Airport Code Table: This is a simple table of IATA airport codes and corresponding cities. Comes from [datahub.io](https://datahub.io/core/airport-codes#data)

Table | Colums | Descriptions
--- | --- | ---
immigrations | 'cicid', 'i94yr', 'i94mon', 'i94cit', 'i94res', 'i94port', 'arrdate', 'i94mode', 'i94addr', 'depdate', 'i94bir', 'i94visa', 'count', 'dtadfile', 'visapost', 'occup', 'entdepa', 'entdepd', 'entdepu', 'matflag', 'biryear', 'dtaddto', 'gender', 'insnum', 'airline', 'admnum', 'fltno', 'visatype'| Immigration data
airports | 'ident', 'type', 'name', 'elevation_ft', 'continent', 'iso_country', 'iso_region', 'municipality', 'gps_code', 'iata_code', 'local_code', 'coordinates'| Airport description and codes
us_demographics | 'City;State;Median Age;Male Population;Female Population;Total Population;Number of Veterans;Foreign-born;Average Household Size;State Code;Race;Count'| Airport description and codes
city_temperatures | 'dt', 'AverageTemperature', 'AverageTemperatureUncertainty', 'City', 'Country', 'Latitude', 'Longitude'| Temperatures of cities

## Complete Project Write Up

  - Spark is used for data processing and PostgresSQL for the model
  - Spark was chosen because for intuitive data processing and ability to handle and process large datasets
  - PostgreSQL was chosen because its a widely used database that allows fast queries in the intuitive SQL language

## Data cleaning and preprocessing

- For city_temperatures we get all rows for data form the United States that is not null, convert the string date to datetime and drop the dublicates and drop rows with any nulls
- For us_demographics we drop dublicates and rows with nulls, we then pivot the Race column and add 0 for missing race counts
- For airports we get only US airports with a IATA code, again dublicates and rows with Null are droped
- We convert arrival and departure date in dates, rename and select meaningful columns and drop dublicates and rows with Null values

## Conceptual data model

We use 4 tables with a star schema 3 dimension tables (city_temperature, airports, us_demographics) and the immigrations as fact table, see table below

|Table |Columns  | Description|
--- | --- | ---
|city_temperature|City, Country, Latitude, Longitude, AverageTemperature, AverageTemperatureUncertainty, dt|temperature information from cities in different countries|
|airports|ident, name, City, State, iata_code|airport information|
|us_demographics|State, City, Median_Age, Male_Population, Female_Population, Total_Population, Number_of_Veterans, Foreign_born,
 Average_Household_Size, State_Code, American_Indian_and_Alaska Native, Asian, Black_or_African_American, Hispanic_or_Latino, White|demographics information|
|immigratins|city_id, year,month, iata_code, city_code, res_id, State, matflag, birthyear, gender, dtaddto,  airline, visatype, visapost, flight_number, arrival_date, departure_date|immigrations informations|

## Data Quality Check

We review the data quality by counting the number of rows for each table and ensure that the cout is >1.

## Data Dictionary

#### city_temperatures
    |-- City: string (nullable = false) - city name from city_temperatures
    |-- Country: string (nullable = false) - country name from city_temperatures
    |-- Latitude: string (nullable = false) - latitude of city name from city_temperatures
    |-- Longitude: string (nullable = false) - longitude of city name from city_temperatures
    |-- AverageTemperature: float (nullable = false) - average temperature in city name from city_temperatures
    |-- AverageTemperatureUncertainty: float (nullable = false) - average temperature uncertainty in the city name from city_temperatures
    |-- dt: date (nullable = false) - date the temperature was recorded name from city_temperatures
    
#### airports
    |-- ident: string (nullable = false) - identifier for airport from airports
    |-- name: string (nullable = false) - name of airport from airports
    |-- City: string (nullable = false) - city of the airport from airports
    |-- State: string (nullable = false) - state the airport is located in from airports
    |-- iata_code: string (nullable = false) - IATA code of the airport from airports
    
    
#### us_demographics
    |-- State: string (nullable = false) - state name of the US state from us_demographics
    |-- City: string (nullable = false) - city name from us_demographics
    |-- Median_Age: float (nullable = false) - median age in city from us_demographics
    |-- Male_Population: integer (nullable = false) - Male population in city from us_demographics
    |-- Female_Population: integer (nullable = false) - Female population in city from us_demographics
    |-- Total_Population: integer (nullable = false) - Total population in city from us_demographics
    |-- Number_of_Veterans: integer (nullable = false) - Number of veterans in city from us_demographics
    |-- Foregin_born: integer (nullable = false) - Number of foregin born in city from us_demographics
    |-- Average_Household_Size: float (nullable = false) - Average household size in city from us_demographics
    |-- State_Code: string (nullable = false) - state code of city from us_demographics
    |-- American_Indian_and_Alaska_Native: integer (nullable = false) - Count of people from native american or alskan native race in city from us_demographics
    |-- Asian: integer (nullable = false) - Count of people from asian race in city from us_demographics
    |-- Black_or_African_American: integer (nullable = false) - Count of people from black or afican-american race in city from us_demographics
    |-- Hispanic_or_Latino: integer (nullable = false) - Count of people from hispanic or latino race in city from us_demographics
    |-- White: integer (nullable = false) - Count of people from white race in city from us_demographics
    
#### immigrations
    |-- city_id: double (nullable = false) - Id of city from immigrations
    |-- year: double (nullable = false) - Year the data was recorded from immigrations
    |-- month: double (nullable = false) - Month the data was recorded from immigrations
    |-- iata_code: string (nullable = false) - IATA code of airport from immigrations
    |-- city_code: double (nullable = false) - city code from immigrations
    |-- res_id: double (nullable = false) - id of residence from immigrations
    |-- State: string (nullable = false) - State from immigrations
    |-- matflag: string (nullable = false) - matchflag between arrival and departure from immigrations
    |-- birthyear: double (nullable = false) - birthyear of passager from immigrations
    |-- gender: string (nullable = false) - gender of the passager from immigtions
    |-- dtaddto: string (nullable = false) - allowed days to stay from immigrations
    |-- airline: string (nullable = false) - airline from immigrations
    |-- visatype: string (nullable = false) - type of visa from immigrations
    |-- visapost: string (nullable = false) - visa post from immigrations
    |-- flight_number: string (nullable = false) - flight number from immigrations
    |-- arrival_date: date (nullable = false) - arrival date from immigrations
    |-- departure_date: date (nullable = false) - departure date from immigrations

## Additional Questions
* Clearly state the rationale for the choice of tools and technologies for the project.
  - Spark is used for data processing and PostgresSQL for the model
  - Spark was chosen because for intuitive data processing and ability to handle and process large datasets
  - PostgreSQL was chosen because its a widely used database that allows fast queries in the intuitive SQL language
* Propose how often the data should be updated and why.
   -  Since the data records daily events (arrival and departure dates in immigrations and temperatues in city_temperatures) daily updates are recommended
* Write a description of how you would approach the problem differently under the following scenarios:
 * The data was increased by 100x.
     - Spark can handle the increase
 * The data populates a dashboard that must be updated on a daily basis by 7am every day.
     - Implement a Airflow pipeline that fetches the data and displays it to the dashbord
 * The database needed to be accessed by 100+ people.
     - The transition of the data from a local PostgreSQL database to a Cloud based option like Amazon Redshift would be recommended
