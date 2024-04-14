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

#df = spark.read.option("multiline","true").json("compras_1.json")

dfs_exploded = dfs_unidos.select("numero_caja",explode("compras").alias("compra"))

# Select columns "nombre", "cantidad", and "precio_unitario"
df_final = dfs_exploded.select("numero_caja",
    dfs_exploded["compra.nombre"].alias("nombre"),
    dfs_exploded["compra.cantidad"].alias("cantidad"),
    dfs_exploded["compra.precio_unitario"].alias("precio_unitario")
)


# Show the DataFrame schema
df_final.printSchema()


df_exploded_2 = df_final.withColumn("nueva", arrays_zip("nombre", "cantidad","precio_unitario"))\
.withColumn("nueva", explode("nueva"))\
.select("numero_caja",col("nueva.nombre").alias("Nombre"), col("nueva.cantidad").alias("Cantidad"),col("nueva.precio_unitario").alias("Precio_Unitario"))

df_exploded_2.printSchema()
df_exploded_2.show()
df_exploded_2.summary().show()

def unir_jsons(files):

    dfs = [spark.read.option("multiline","true").json(archivo_json) for archivo_json in files]
    dfs_unidos = reduce(DataFrame.union,dfs)

    return dfs_unidos

compras_jsons = unir_jsons(archivos)

compras_jsons.show()
