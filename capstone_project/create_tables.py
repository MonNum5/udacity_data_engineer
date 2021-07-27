"""
Contains code for the creation of the data model
"""

# DROP TABLES

city_temperatures_table_drop = "DROP TABLE IF EXISTS city_temperatures"
airports_table_drop = "DROP TABLE IF EXISTS airports"
us_demographics_table_drop = "DROP TABLE IF EXISTS us_demographics"
immigrations_table_drop = "DROP TABLE IF EXISTS immigrations"

# CREATE TABLES

city_temperatures_table_create = ("""CREATE TABLE IF NOT EXISTS city_temperatures
        (City varchar not null,
        Country varchar not null,
        Latitude varchar not null,
        Longitude varchar not null,
        AverageTemperature float not null,
        AverageTemperatureUncertainty float not null,
        dt date not null)
""")

airports_table_create = ("""CREATE TABLE IF NOT EXISTS airports
    (ident varchar not null,
    name varchar not null,
    State varchar not null,
    iata_code varchar not null)
""")

us_demographics_table_create = ("""CREATE TABLE IF NOT EXISTS us_demographics
    (State varchar not null,
    City varchar not null,
    Median_Age float not null,
    Male_Population int not null,
    Female_Population int not null,
    Total_Population int not null,
    Number_of_Veterans int not null,
    Foregin_born int not null,
    Average_Household_Size float not null,
    State_Code varchar not null,
    American_Indian_and_Alaska_Native int not null,
    Asian int not null,
    Black_or_African_American int not null,
    Hispanic_or_Latino int not null,
    White varchar not null)
""")

immigrations_table_create = ("""CREATE TABLE IF NOT EXISTS immigrations
    (city_id int not null,
    year int not null,
    month int not null,
    iata_code varchar not null,
    city_code int not null,
    res_id int not null,
    State varchar not null,
    matflag varchar not null,
    birthyear int not null,
    gender varchar not null,
    dtaddto date not null,
    visatype varchar not null,
    visapost varchar not null,
    flight_number varchar not null,
    airline varchar not null,
    arrival_date date not null, 
    departure_date date not null,
    City varchar not null)
""")

delete_tables = [city_temperatures_table_drop, airports_table_drop, us_demographics_table_drop, immigrations_table_drop]
create_tables = [city_temperatures_table_create, airports_table_create, us_demographics_table_create, immigrations_table_create]