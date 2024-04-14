from pyspark.sql import SparkSession
from pyspark.sql.types import (DateType, IntegerType, FloatType, StringType,
                               StructField, StructType, TimestampType)
from pyspark.sql.functions import col, explode, arrays_zip

spark = SparkSession.builder.appName("Tarea_2").getOrCreate()

# schema = StructType([
#     StructField("nombre", StringType(), True),
#     StructField("cantidad", IntegerType(), True),
#     StructField("precio_unitario", FloatType(), True)
# ])

df = spark.read.option("multiline","true").json("compras_1.json")

df_exploded = df.select(explode("compras").alias("compra"))

# Select columns "nombre", "cantidad", and "precio_unitario"
df_final = df_exploded.select(
    df_exploded["compra.nombre"].alias("nombre"),
    df_exploded["compra.cantidad"].alias("cantidad"),
    df_exploded["compra.precio_unitario"].alias("precio_unitario")
)


# Show the DataFrame schema
df.printSchema()
df_final.printSchema()


df_exploded_2 = df_final.withColumn("new", arrays_zip("nombre", "cantidad","precio_unitario"))\
.withColumn("new", explode("new"))\
.select(col("new.nombre").alias("Nombre"), col("new.cantidad").alias("cantidad"),col("new.precio_unitario").alias("Precio_Unitario"))

df_exploded_2.printSchema()
df_exploded_2.show()