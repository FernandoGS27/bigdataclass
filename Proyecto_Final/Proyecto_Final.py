from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("Proyecto Final") \
    .getOrCreate()

construccion_df = spark.read.csv("Base_Anonimizada2022.csv",header=True,inferSchema=True)

construccion_df.show()

##Dato que el proyecto se enfoca en construcciones residenciales. Se quitan del dataset todas las construcciones no residenciales

construccion_residencial_df = construccion_df.filter(construccion_df.claobr==1)

construccion_residencial_df.summary().show()