"""
spark.py
~~~~~~~~
Module containing helper function for use with Apache Spark
"""

from pyspark.sql import SparkSession
from utils import logging
from os import listdir, path
import json
from pyspark import SparkFiles


def start_spark(app_name='etl_app', master='local[*]',
                files=[], spark_config={}):

    spark_builder = (
        SparkSession
            .builder
            .master("local")
            .appName(app_name))

    spark_builder.config('spark.files', files)
    spark_builder.config("spark.sql.autoBroadcastJoinThreshold", -1)

    for key, val in spark_config.items():
        spark_builder.config(key, val)

    # create session and retrieve Spark logger object
    spark_sess = spark_builder.getOrCreate()
    spark_logger = logging.Log4j(spark_sess)

    # get config file if sent to cluster with --files
    spark_files_dir = SparkFiles.getRootDirectory()
    config_files = [filename
                    for filename in listdir(spark_files_dir)
                    if filename.endswith('config.json')]

    try:
        path_to_config_file = path.join(spark_files_dir, config_files[0])
        with open(path_to_config_file, 'r') as config_file:
            config_dict = json.load(config_file)
    except Exception as e:
        spark_logger.warn("no config file found" + str(e))

    return spark_sess, spark_logger, config_dict