from pyspark.sql import SparkSession
from pyspark.sql.functions import avg

spark = SparkSession.builder \
    .appName("Proyecto Final") \
    .getOrCreate()

construccion_df = spark.read.csv("Base_Anonimizada2022.csv",header=True,inferSchema=True)

construccion_df.show()

##Dato que el proyecto se enfoca en construcciones residenciales. Se quitan del dataset todas las construcciones no residenciales

construccion_residencial_df = construccion_df.filter(construccion_df.claobr==1)



## Se seleccionan las variables de interes

construccion_residencial_df = construccion_residencial_df.select("pro_num_prov","pc_num_cant","num_obras","arecon","numviv","numapo","numdor","valobr")

construccion_residencial_df.summary().show()

construccion_residencial_agrupada_df = construccion_residencial_df.groupby("pro_num_prov","pc_num_cant").agg(avg("num_obras").alias("pro_num_obras"),avg("arecon").alias("prom_arecon"),
                                                                                                            avg("numviv").alias("prom_numviv"),avg("numapo").alias("prom_numapo"),
                                                                                                            avg("numdor").alias("prom_numdor"),avg("valobr").alias("prom_valobr"))

construccion_residencial_agrupada_df.show()

enaho_2022_df = spark.read.csv("BdBasePublica.csv",header=True,inferSchema=True)

enaho_2022_variables_df = enaho_2022_df.select("ID_HOGAR","ID_VIVIENDA","LINEA","REGION","ZONA","ithb","Escolari","C2A4","TamViv","V18J","V18F1","V2A")

enaho_2022_variables_df.show(40)

