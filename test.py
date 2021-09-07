"""
test.py
"""
import unittest

from utils.spark import start_spark
import main


class SparkETLTests(unittest.TestCase):
    """
    Test suite for main.py
    NOTE: More test cases can be written.
    """

    def setUp(self):
        """Start Spark, define config and path to config data
        """
        self.spark, self.log, self.config = start_spark(app_name = "test_etl_job",
                                                        files='configs/etl_config.json')

    def tearDown(self):
        """Stop Spark
        """
        self.spark.stop()

    def test_read_data(self):
        """
        Test read_data function
        :return:
        """
        house_data_test = main.read_data(self.spark, self.config["test_house_data_path"])
        house_data_test_count = 0

        house_data_test_original = main.read_data(self.spark, self.config["test_house_data_test1_path"])
        house_data_test_original_count = 2

        self.assertEqual(house_data_test.count(),house_data_test_count)
        self.assertEqual(house_data_test_original.count(),house_data_test_original_count)

    def test_convert_spcs_to_lat_long(self):
        """
        Test convert_spcs_to_lat_long function
        :return:
        """
        columns = ["GIS_MID_X", "GIS_MID_Y"]
        data = [(1269293.53489655,224852.7909383),(1272296.04824363,265566.28685199)]

        expected_columns = ["GIS_MID_X", "GIS_MID_Y","LATITUDE","LONGITUDE"]
        expected_data = [(1269293.53489655,224852.7909383,47.606382464787444,-122.33789343983992),
                         (1272296.04824363,265566.28685199,47.71814079130721,-122.32895665401779)]

        df_data = self.spark.createDataFrame(data).toDF(*columns)
        df_expected_data = self.spark.createDataFrame(expected_data).toDF(*expected_columns)

        resultant_data = main.convert_spcs_to_lat_long(df_data)
        df_difference = resultant_data.exceptAll(df_expected_data)

        self.assertEqual(df_difference.count(),0)


if __name__ == '__main__':
    unittest.main()





