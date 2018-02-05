from pyspark.sql import SparkSession, HiveContext

spark = SparkSession.builder.enableHiveSupport() \
    .config("hive.exec.dynamic.partition", "true") \
    .config("hive.exec.dynamic.partition.mode", "nonstrict") \
    .appName("omniture_guid_load").getOrCreate()

spark.sparkContext.setLogLevel("WARN")

 # df1 have roll_no and student_name    ||  df2 have roll_no and agg_marks 
 # final df will have student_name and marks for the students having agg_marks more than 50%

def etl_script1 (df1, df2):
    df1.createOrReplaceTempView('df1')
    df2.createOrReplaceTempView('df2')

    sql="""SELECT df1.name
      , df2.marks
    FROM df1 INNER JOIN df2
      ON df1.roll_no = df2.roll_no
  WHERE df2.marks > 50"""

    return spark.sql(sql)


