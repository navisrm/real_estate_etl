from pyspark.sql.functions import udf, to_date, substring
from pyspark.sql import functions as func
from utils.spark import start_spark
from pyspark.sql.types import DoubleType, ArrayType
import pyproj
import datetime
from math import radians, cos, sin, asin, sqrt


def main():
    spark, log, config = start_spark(
        app_name='etl_job',
        files='configs/etl_config.json')

    distance_consideration = int(config["transit_consideration_distance_in_kms"])

    # read house data
    try:
        house_data = read_data(spark,config["house_data_input_path"])
    except Exception as e:
        log.error("house_data file cannot be read. " + str(e))

    # read transit classification data
    try:
        transit_classification = read_data(spark,config["transit_classification_data_input_path"])
    except Exception as e:
        log.error("transit_classification file cannot be read. " + str(e))

    """
    Raw Zone
    """
    # Persisting the input data as-is in the raw history zone
    write_output_to_csv_files(house_data,config["raw_zone_house_data"])
    write_output_to_csv_files(transit_classification,config["raw_zone_transit_data"])

    """
    Trusted Zone
    """
    ##############
    # House Data #
    ##############
    # Change the date type in house_data and convert column names to upper case and
    # write results to trusted zone
    house_data_date_reformatted = house_data.withColumn("new_date",to_date(substring(func.col("date"),1,8),"yyyyMMdd")).\
        drop("date").withColumnRenamed("new_date","date")

    house_data_column_names_upper_case = house_data_date_reformatted.\
        select([func.col(x).alias(x.upper()) for x in house_data_date_reformatted.columns])

    write_output_to_csv_files(house_data_column_names_upper_case, config["trusted_zone_house_data_columns_upper_case"])

    ###############################
    # Transit Classification Data #
    ###############################
    # Change date type in transit classification data and convert geo coordinates from SPCS to Lat Long
    # write results to trusted zone
    transit_classification_date_reformatted = transit_classification.\
        withColumn("new_date",to_date(func.col("SURFACEDATE_1"),"yyyy/MM/dd HH:mm:ss+SS")).drop("SURFACEDATE_1").\
        withColumnRenamed("new_date","SURFACEDATE")

    df_transit_with_lat_lon = convert_spcs_to_lat_long(transit_classification_date_reformatted).\
        drop("GIS_MID_X", "GIS_MID_Y")

    write_output_to_csv_files(df_transit_with_lat_lon, config["trusted_zone_transit_with_lat_long"])

    """
    Refined Zone
    """
    ####################
    # Aggregated Rules #
    ####################

    # Cross Join house data and transit classification data
    df_cross_joined = house_data_column_names_upper_case.crossJoin(df_transit_with_lat_lon)

    # Calculate the distance between each unit in the house data and each object in the transit classification data
    df_distance_calculated = calculated_distance(df_cross_joined)

    write_output_to_csv_files(df_distance_calculated,config["refined_zone_house_transit_distance_calculated"])

    # Identify the houses that has transit in less than 5 kms
    houses_with_transit_in_5kms = df_distance_calculated.filter(func.col("DISTANCE")<distance_consideration).\
        select("ID","DISTANCE").distinct()

    # Combine this with house_data and create a new column TRANSIT_NEARBY(5KMS) that specifies if the house
    # is close to transit
    houses_with_transit_nearby = house_data_column_names_upper_case.join(houses_with_transit_in_5kms, "ID","left").\
        withColumn("TRANSIT_NEARBY(5KMS)", func.when(func.col("DISTANCE").isNotNull(),"Yes").\
                   otherwise("No")).drop("DISTANCE")
    write_output_to_csv_files(houses_with_transit_nearby,config["refined_zone_houses_with_transit_nearby"])

    """
    Note:
    The final tables are denormalized tables which serves Machine Learning teams in general. The data visualization team
    would require some additional normalized tables.
    """
    log.info("Process completed")
    spark.stop()


def read_data(spark,path_to_file):
    """
    This function reads data from the path provided
    :param spark: Spark Session
    :param path_to_file: Path to the file
    :return: dataframe with data from the file
    """
    df = spark.read.load(path_to_file, format="csv", sep=",", inferSchema="true", header="true")
    return df


def write_output_to_csv_files(df, path):
    """
    This function writes the dataframe output to csv files
    :param spark:
    :param df: Dataframe results to be written
    :param path: The path where results should be written
    :return: None
    """
    processing_date = datetime.datetime.now()
    string_date = processing_date.strftime("%Y") + "-" + processing_date.strftime("%m") + "-" + processing_date.strftime("%d")
    df.coalesce(1).write.mode("overwrite").option("header","true").format("csv").save(path + string_date)


def convert_spcs_to_lat_long(df):
    """
    This function converts location spcs geo coordinates to latitude and longitude degrees.
    :param df: Dataframe with spcs coordinates
    :return: Dataframe with latitude and longitude values
    """
    def conversion(X_coord, Y_coord):
        p = pyproj.Proj("+proj=lcc +lat_1=47.5 +lat_2=49.73333333333333 +lat_0=47 +lon_0=-120.8333333333333 +x_0=500000.0000000001 +y_0=0 +ellps=GRS80 +to_meter=0.3048006096012192 +no_defs")
        lon, lat = p(X_coord, Y_coord, inverse=True)
        return [lat,lon]

    conversion_udf = udf(conversion, ArrayType(DoubleType()))

    df_lat_lon = df.withColumn("converted_coord", conversion_udf(df["GIS_MID_X"], df["GIS_MID_Y"])).\
        withColumn("LATITUDE", func.col("converted_coord").getItem(0)).\
        withColumn("LONGITUDE", func.col("converted_coord").getItem(1)).drop(func.col("converted_coord"))

    return df_lat_lon


def get_distance(longit_a, latit_a, longit_b, latit_b):
    """
    This python functions calculates the distance between two location coordinates
    :param longit_a: Origin's longtide
    :param latit_a: Origin's latitude
    :param longit_b: Destination's Longitude
    :param latit_b: Destination's Latitude
    :return: Calculated distance value
    """
    # Transform to radians
    longit_a, latit_a, longit_b, latit_b = map(radians, [longit_a,  latit_a, longit_b, latit_b])
    dist_longit = longit_b - longit_a
    dist_latit = latit_b - latit_a

    # Calculate area
    area = sin(dist_latit/2)**2 + cos(latit_a) * cos(latit_b) * sin(dist_longit/2)**2
    # Calculate the central angle
    central_angle = 2 * asin(sqrt(area))
    radius = 6371
    # Calculate Distance
    distance = central_angle * radius
    return abs(round(distance, 2))


# UDF for get_distance function
udf_get_distance = func.udf(get_distance)


def calculated_distance(df):
    """
    This function creates a new column for dataframe with distance calculated using the udf_get_distance function
    :param df: Dataframe with spcs coordinates
    :return: Dataframe with Latitude and Longitude values
    """
    return df.withColumn("DISTANCE", udf_get_distance(func.col("LONG"), func.col("LAT"), func.col("LONGITUDE"), func.col("LATITUDE")))


# entry point for PySpark ETL application
if __name__ == '__main__':
    main()
