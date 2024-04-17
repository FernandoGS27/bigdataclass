from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import (DateType, IntegerType, FloatType, StringType,
                               StructField, StructType, TimestampType)
from pyspark.sql.functions import col, explode, arrays_zip, exists
from functools import reduce
import sys
import glob

# spark = SparkSession.builder.appName("Tarea_2").getOrCreate()

if __name__ == "__main__":
    # Check if the correct number of arguments is provided
    if len(sys.argv) > 1 and all([exists(file) for file in sys.argv[1:]]):
        archivos = sys.argv[1:]
        spark = SparkSession.builder.appName("Tarea_2").getOrCreate()
        sys.exit(1)
    else:
        print("Se debe proporcionar al menos un archivo")

    

# Load JSON files into DataFrame
dfs = [spark.read.option("multiline","true").json(archivo_json) for archivo_json in archivos]
#compras_jsons = reduce(DataFrame.union,dfs)
dfs.show()


#archivos = ["compras_1.json","compras_2.json","compras_3.json","compras_4.json","compras_5.json"]

# def unir_jsons_compras(files):

#     dfs = [spark.read.option("multiline","true").json(archivo_json) for archivo_json in files]
#     dfs_unidos = reduce(DataFrame.union,dfs)
    
#     return dfs_unidos

# compras_jsons = unir_jsons_compras(archivos)

#compras_jsons.show()

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

#dataframes_jsons = compras_jsons_a_dataframes(compras_jsons)

# dataframes_jsons.summary().show()
# dataframes_jsons.printSchema()
# dataframes_jsons.show()


def total_productos(df):
    sumar_productos = df.groupBy("Nombre").sum("Cantidad")
    sumar_productos = sumar_productos.select(col("Nombre"),col('sum(Cantidad)').alias('Cantidad_Total'))

    return sumar_productos

#productos = total_productos(dataframes_jsons)

#productos.show()

def total_cajas(df):

    sumar_total_cajas = df.groupBy("numero_caja").sum("Precio_Unitario")
    sumar_total_cajas = sumar_total_cajas.select(col("numero_caja"),col('sum(Precio_Unitario)').alias('Total_Vendido'))

    return sumar_total_cajas

#total_vendido = total_cajas(dataframes_jsons)
#total_vendido.show()

def calcular_metricas(df_jsons,df_ventas,df_producto):
    caja_mas_ventas = df_ventas.orderBy(col("Total_Vendido").desc()).select("numero_caja").first()[0]
    caja_menos_ventas = df_ventas.orderBy(col("Total_Vendido").asc()).select("numero_caja").first()[0]
    percentil_25 = df_ventas.orderBy(col("Total_Vendido").asc()).approxQuantile("Total_Vendido",[0.25],0.01)[0]
    percentil_50 = df_ventas.orderBy(col("Total_Vendido").asc()).approxQuantile("Total_Vendido",[0.50],0.01)[0]
    percentil_75 = df_ventas.orderBy(col("Total_Vendido").asc()).approxQuantile("Total_Vendido",[0.75],0.01)[0]
    producto_mas_vendido = df_producto.orderBy(col("Cantidad_Total").desc()).select("Nombre").first()[0]

    ingreso_por_compra = df_jsons.withColumn("ingreso_por_compra", col("Cantidad")*col("Precio_Unitario"))
    ingreso_por_producto = ingreso_por_compra.groupBy("Nombre").sum("ingreso_por_compra")
    ingreso_por_producto = ingreso_por_producto.select(col("Nombre"),col("sum(ingreso_por_compra)").alias("Ingreso_por_compra"))
    producto_mayor_ingreso = ingreso_por_producto.orderBy(col("Ingreso_por_compra").desc()).select("Nombre").first()[0]

    df_metricas = spark.createDataFrame([("caja_con_mas_ventas",caja_mas_ventas),("caja_con_menos_ventas",caja_menos_ventas),("percentil_25_por_caja",percentil_25),\
                                ("percentil_50_por_caja",percentil_50),("percentil_75_por_caja",percentil_75),("producto_mas_vendido_por_unidad",producto_mas_vendido),\
                                    ("producto_de_mayor_ingreso",producto_mayor_ingreso)],["Tipo_de_Metrica","Valor"])
    
    df_metricas_csv = df_metricas.repartition(1).write.csv("metricas",header=True, mode="overwrite")
    
    return df_metricas_csv

#metricas = calcular_metricas(dataframes_jsons,total_vendido,productos)
#metricas.repartition(1).write.csv("metricas",header=True, mode="overwrite")

#metricas.coalesce(1).write.format("csv").option("header", "true").save("metricas.csv")

#metricas.show()





