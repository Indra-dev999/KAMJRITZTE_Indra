import unittest
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

class TestDataPipeline(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = SparkSession.builder.master("local[*]").appName("TestDataPipeline").getOrCreate()

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def test_transformation(self):
        data = [("John", "Doe"), ("Jane", "Smith")]
        columns = ["first_name", "last_name"]
        df = self.spark.createDataFrame(data, columns)
        df_transformed = df.withColumn("fullname", concat(col("first_name"), lit(" "), col("last_name")))
        self.assertEqual(df_transformed.filter(col("fullname") == "John Doe").count(), 1)

if __name__ == "__main__":
    unittest.main()