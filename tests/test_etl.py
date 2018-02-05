from pyspark.sql import SparkSession, HiveContext
import pytest
import etl


def test_etl_1():
    df1 = spark.createDataFrame(

        [
            (101, 'Rishi'),
            (102, 'Manoj'),
            (103, 'Prateek'),
            (104, 'Praveen')
        ], ["roll_no", "name"])

    # print (df1.show())

    df2 = spark.createDataFrame(
        [

            (101, 47),
            (102, 58),
            (103, 53),
            (104, 84),
            (105, 72)

        ], ["roll_no", "marks"])

    # print(df2.show())

    actual = etl.etl_script1(df1, df2).collect()

    print(actual)

    expected = spark.createDataFrame(
        [
            ('Prateek', 53),
            ('Praveen', 84),
            ('Manoj', 58),
        ], ["name", "marks"]
    ).collect()

    print(expected)

    assert actual == expected


spark = SparkSession.builder.enableHiveSupport() \
    .config("hive.exec.dynamic.partition", "true") \
    .config("hive.exec.dynamic.partition.mode", "nonstrict") \
    .appName("omniture_guid_load").getOrCreate()
