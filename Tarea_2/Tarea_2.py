from pyspark.sql import SparkSession
from pyspark.sql.types import (DateType, IntegerType, FloatType, StringType,
                               StructField, StructType, TimestampType)

spark = SparkSession.builder.appName("Tarea_2").getOrCreate()

# schema = StructType([
#     StructField("nombre", StringType(), True),
#     StructField("cantidad", IntegerType(), True),
#     StructField("precio_unitario", FloatType(), True)
# ])

df = spark.read.option("multiline","true").json("compras_1.json")

# Show the DataFrame schema
df.printSchema()

# Show the first few rows of the DataFrame
df.show()