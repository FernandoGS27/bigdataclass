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

# dfs = [spark.read.option("multiline","true").json(archivo_json) for archivo_json in archivos]

# dfs_unidos = reduce(DataFrame.union,dfs)

# dfs_unidos.show()

#df = spark.read.option("multiline","true").json("compras_1.json")

# dfs_exploded = dfs_unidos.select("numero_caja",explode("compras").alias("compra"))

# # Select columns "nombre", "cantidad", and "precio_unitario"
# df_final = dfs_exploded.select("numero_caja",
#     dfs_exploded["compra.nombre"].alias("nombre"),
#     dfs_exploded["compra.cantidad"].alias("cantidad"),
#     dfs_exploded["compra.precio_unitario"].alias("precio_unitario")
# )


# Show the DataFrame schema
# df_final.printSchema()


# df_exploded_2 = df_final.withColumn("nueva", arrays_zip("nombre", "cantidad","precio_unitario"))\
# .withColumn("nueva", explode("nueva"))\
# .select("numero_caja",col("nueva.nombre").alias("Nombre"), col("nueva.cantidad").alias("Cantidad"),col("nueva.precio_unitario").alias("Precio_Unitario"))

# df_exploded_2.printSchema()
# df_exploded_2.show()
# df_exploded_2.summary().show()

def unir_jsons_compras(files):

    dfs = [spark.read.option("multiline","true").json(archivo_json) for archivo_json in files]
    dfs_unidos = reduce(DataFrame.union,dfs)
    
    return dfs_unidos

compras_jsons = unir_jsons_compras(archivos)

compras_jsons.show()

def compras_jsons_a_dataframes(files):

    dfs_exploded = files.select("numero_caja",explode("compras").alias("compra"))

    df_exploded_2 = dfs_exploded.select("numero_caja",
    dfs_exploded["compra.nombre"].alias("nombre"),
    dfs_exploded["compra.cantidad"].alias("cantidad"),
    dfs_exploded["compra.precio_unitario"].alias("precio_unitario"))

    df_exploded_3 = df_exploded_2.withColumn("nueva", arrays_zip("nombre", "cantidad","precio_unitario")).withColumn("nueva", explode("nueva"))
    df_exploded_4 = df_exploded_3.select("numero_caja",col("nueva.nombre").alias("Nombre"), col("nueva.cantidad").alias("Cantidad"),col("nueva.precio_unitario").alias("Precio_Unitario"))

    dfs =  df_exploded_4.withColumn("Cantidad",col("Cantidad").cast("int"))
    dfs = dfs.withColumn("Precio_Unitario",col("Precio_Unitario").cast("float"))

    return dfs

dataframes_jsons = compras_jsons_a_dataframes(compras_jsons)

dataframes_jsons.summary().show()
dataframes_jsons.printSchema()
dataframes_jsons.show()


def total_productos(df):
    sumar_productos = df.groupBy("Nombre").sum("Cantidad")
    sumar_productos = sumar_productos.select(col("Nombre"),col('sum(Cantidad)').alias('Cantidad_Total'))

    return sumar_productos

productos = total_productos(dataframes_jsons)

productos.show()

def total_cajas(df):

    sumar_total_cajas = df.groupBy("numero_caja").sum("Precio_Unitario")
    sumar_total_cajas = sumar_total_cajas.select(col("numero_caja"),col('sum(Precio_Unitario)').alias('Total_Vendido'))

    return sumar_total_cajas

total_vendido = total_cajas(dataframes_jsons)
total_vendido.show()

max_total_vendido=total_vendido.orderBy(col("Total_Vendido").desc()).select("numero_caja").first()[0]

print(max_total_vendido)



