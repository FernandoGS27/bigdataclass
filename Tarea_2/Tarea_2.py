from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import (DateType, IntegerType, FloatType, StringType,
                               StructField, StructType, TimestampType)
from pyspark.sql.functions import col, explode, arrays_zip
from functools import reduce

spark = SparkSession.builder.appName("Tarea_2").getOrCreate()

# schema = StructType([
#     StructField("nombre", StringType(), True),
#     StructField("cantidad", IntegerType(), True),
#     StructField("precio_unitario", FloatType(), True)
# ])

archivos = ["compras_1.json","compras_2.json","compras_3.json","compras_4.json","compras_5.json"]

dfs = [spark.read.option("multiline","true").json(archivo_json) for archivo_json in archivos]

dfs_unidos = reduce(DataFrame.union,dfs)

dfs_unidos.show()

df = spark.read.option("multiline","true").json("compras_1.json")

df_exploded = df.select("numero_caja",explode("compras").alias("compra"))

# Select columns "nombre", "cantidad", and "precio_unitario"
df_final = df_exploded.select("numero_caja",
    df_exploded["compra.nombre"].alias("nombre"),
    df_exploded["compra.cantidad"].alias("cantidad"),
    df_exploded["compra.precio_unitario"].alias("precio_unitario")
)


# Show the DataFrame schema
df.printSchema()
df_final.printSchema()


df_exploded_2 = df_final.withColumn("nueva", arrays_zip("nombre", "cantidad","precio_unitario"))\
.withColumn("nueva", explode("nueva"))\
.select("numero_caja",col("nueva.nombre").alias("Nombre"), col("nueva.cantidad").alias("Cantidad"),col("nueva.precio_unitario").alias("Precio_Unitario"))

df_exploded_2.printSchema()
df_exploded_2.show()