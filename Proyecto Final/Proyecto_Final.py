from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("Proyecto_Final") \
    .getOrCreate()

df_construccion = spark.read.csv('Base_Anonimizada2022.csv',header=True,inferSchema=True)

df_construccion.show()
