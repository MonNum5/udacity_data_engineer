"""
Contains functions for the processing and cleaning of the considered data
"""

#Imports
from pyspark.sql.types import DateType
from pyspark.sql import functions as F

def check_for_null(df, columns=None):
    """
    Check Pyspark dataframe columns for null
    
    Args:
        df (object): Pyspark dataframe object
        columns (list or None): List of str of column names
    """
    if not columns:
        columns = df.schema.names
      
    df_count = df.count()
    for col in columns:
        count_nulls = df.where(df[col].isNull()).count()
        if count_nulls > 0:
            print(ValueError(f'Data in {col} has {count_nulls} NULLs'))
        
        
def check_for_dublicates(df, columns = None):
    """
    Check Pyspark dataframe columns for dublicates
    
    Args:
        df (object): Pyspark dataframe object
        columns (list or None): List of str of column names
    """
    if not columns:
        columns = df.schema.names
      
    df_count = df.count()
    for col in columns:
        col_count = df.dropDuplicates([col]).count()
        if df_count > col_count:
            diff = df_count - col_count
            print(ValueError(f'Data in {col} has duplicates {diff} of {df_count}'))          

            
def clean_city_temperatures(city_temperatures_spark, spark_session):
    """
    Clean data from city temperatures
    
    Args:
        city_temperatures_spark (object): Pyspark dataframe object
        spark_session (object): Pyspark session
        
    Returns:
        (object): Pyspark dataframe with cleaned data
    """
    spark = spark_session
    city_temperatures_spark.createOrReplaceTempView('city_temperatures')
    
    city_temperatures_spark_cleaned = spark.sql("""
        SELECT City, Country, Latitude, Longitude, cast(AverageTemperature as float), cast(AverageTemperatureUncertainty as float), dt
        FROM city_temperatures
        WHERE AverageTemperature IS NOT NULL 
        AND AverageTemperatureUncertainty IS NOT NULL
        """)

    city_temperatures_spark_cleaned = city_temperatures_spark_cleaned.withColumn('dt',city_temperatures_spark_cleaned['dt'].cast(DateType()))
    city_temperatures_spark_cleaned = city_temperatures_spark_cleaned.dropDuplicates()
    city_temperatures_spark_cleaned = city_temperatures_spark_cleaned.na.drop()
    
    return city_temperatures_spark_cleaned


def clean_us_demographics(us_demographics_spark, spark_session):
    """
    Clean data from us_demographics
    
    Args:
        us_demographics (object): Pyspark dataframe object
        spark_session (object): Pyspark session
        
    Returns:
        (object): Pyspark dataframe with cleaned data
    """
    spark = spark_session
    us_demographics_spark.createOrReplaceTempView('us_demographics')
    
    dum = spark.sql("""
    SELECT City, State, cast(`Median Age` as float) as Median_Age, cast(`Male Population` as int) as Male_Population,
    cast(`Female Population` as int) as Female_Population, cast(`Total Population` as int) as Total_Population,
    cast(`Number of Veterans` as int) as Number_of_Veterans, cast(`Foreign-born` as int) as Foregin_born,
    cast(`Average Household Size` as float) as Average_Household_Size, `State Code` as State_Code,Race, cast(Count as int)
    FROM us_demographics
    """)
    us_demographics_spark_cleaned = dum.dropDuplicates()
    us_demographics_spark_cleaned = us_demographics_spark_cleaned.na.drop()
    us_demographics_spark_race = us_demographics_spark_cleaned.groupBy(['City','State']).pivot("Race").agg(F.first("Count"))
    us_demographics_spark_race = us_demographics_spark_race.select('City', 'State', F.col('American Indian and Alaska Native').alias('American_Indian_and_Alaska_Native'),
                        'Asian', F.col('Black or African-American').alias('Black_or_African_American'), F.col('Hispanic or Latino').alias('Hispanic_or_Latino'), 'White')
    us_demographics_spark_cleaned = us_demographics_spark_cleaned.drop('Race', 'Count') 
    us_demographics_spark_cleaned = us_demographics_spark_cleaned.dropDuplicates()
    us_demographics_spark_cleaned = us_demographics_spark_cleaned.join(us_demographics_spark_race, ['State', 'City'])
    us_demographics_spark_cleaned = us_demographics_spark_cleaned.fillna(
                    {'American_Indian_and_Alaska_Native':0, 
                      'Asian':0, 
                      'Black_or_African_American':0,
                      'Hispanic_or_Latino':0,
                      'White':0})
    us_demographics_spark_cleaned = us_demographics_spark_cleaned.orderBy(['City','State'])
    
    return us_demographics_spark_cleaned


def clean_airports(airports_spark, spark_session):
    """
    Clean data from airports
    
    Args:
        airports_spark (object): Pyspark dataframe object
        spark_session (object): Pyspark session
        
    Returns:
        (object): Pyspark dataframe with cleaned data
    """
    spark = spark_session
    airports_spark.createOrReplaceTempView('airports')
    
    airports_spark_cleaned = spark.sql("""
    SELECT ident, name, municipality as City, SUBSTRING(iso_region, 4, 5) as State, iata_code
    FROM airports
    WHERE iata_code IS NOT NULL
    """)

    airports_spark_cleaned = airports_spark_cleaned.dropDuplicates()
    airports_spark_cleaned = airports_spark_cleaned.na.drop()
    
    return airports_spark_cleaned
                                           

def clean_immigrations(immigrations_spark, spark_session):
    """
    Clean data from immigrations
    
    Args:
        immigrations_spark (object): Pyspark dataframe object
        spark_session (object): Pyspark session
        
    Returns:
        (object): Pyspark dataframe with cleaned data
    """
    spark = spark_session
    immigrations_spark.createOrReplaceTempView('immigrations')
    
    immigrations_spark_cleaned = immigrations_spark.withColumn('arrival_date', F.expr('date_add("1960-01-01", arrdate)'))
    immigrations_spark_cleaned = immigrations_spark_cleaned.withColumn('departure_date', F.expr('date_add("1960-01-01", depdate)'))
    immigrations_spark_cleaned = immigrations_spark_cleaned.withColumn('dtaddto', F.to_timestamp(immigrations_spark_cleaned.dtaddto,'ddMMyyyy'))
    immigrations_spark_cleaned = immigrations_spark_cleaned.withColumn('dtaddto', F.to_date(immigrations_spark_cleaned.dtaddto,'MMddyyyy'))

    immigrations_spark_cleaned.createOrReplaceTempView('immigrations')

    immigrations_spark_cleaned = spark.sql("""
        SELECT cast(cicid as int) as city_id, cast(i94yr as int) as year, cast(i94mon as int) as month, 
            i94port as iata_code, cast(i94cit as int) as city_code, cast(i94res as int) res_id, i94addr as State, 
            matflag, cast(biryear as int) as birthyear, gender, dtaddto, airline, 
            visatype, visapost, fltno as flight_number, arrival_date,
            departure_date
        FROM immigrations
        """)

    immigrations_spark_cleaned = immigrations_spark_cleaned.dropDuplicates()
    immigrations_spark_cleaned = immigrations_spark_cleaned.na.drop()


    return immigrations_spark_cleaned                                          