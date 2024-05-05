from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("Proyecto Final") \
    .getOrCreate()

construccion_df = spark.read.csv("Base_Anonimizada2022.csv",header=True,inferSchema=True)

construccion_df.show()

##Dato que el proyecto se enfoca en construcciones residenciales. Se quitan del dataset todas las construcciones no residenciales

construccion_residencial_df = construccion_df.filter(construccion_df.claobr==1)



## Se seleccionan las variables de interes

construccion_residencial_df = construccion_residencial_df.select("pro_num_prov","pc_num_cant","claper","claobr","num_obras","arecon","numviv","numapo","numdor","valobr")

construccion_residencial_df.summary().show()