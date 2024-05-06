from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

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

construccion_residencial_agrupada_df = construccion_residencial_df.groupby("pro_num_prov","pc_num_cant").agg(F.avg("num_obras").alias("pro_num_obras"),F.avg("arecon").alias("prom_arecon"),
                                                                                                            F.avg("numviv").alias("prom_numviv"),F.avg("numapo").alias("prom_numapo"),
                                                                                                            F.avg("numdor").alias("prom_numdor"),F.avg("valobr").alias("prom_valobr"))

construccion_residencial_agrupada_df.show()

enaho_2022_df = spark.read.csv("BdBasePublica.csv",header=True,inferSchema=True)

enaho_2022_variables_df = enaho_2022_df.select("ID_HOGAR","LINEA","REGION","ZONA","ithb","Escolari","C2A4","TamViv","V18J","V18F1","V2A")

"La variable 'Tenencia de Viviennda' contiene 5 categorias. Para efectos de este trabajo se agrupan en solo 2. Casa Propia (1) Casa que no es propia (0)"

enaho_2022_variables_binario_df = enaho_2022_variables_df.withcolumn("Tenencia_Vivienda", F.when(enaho_2022_variables_df.V2A==1,2,1).otherwise(0))

enaho_2022_variables_binario_df.show()


'''Los datos de ENAHO vienen a nivel de hogar y nivel individual. Para el siguiente trabajo nos interesa utilizar las variables a nivel de hogar y agrupar aquellas que vienen a nivel individual.
Para esto se crea un identificador a nivel de cada hogar y se grupan las variables a nivel individual'''

windowSpec = Window.orderBy(F.monotonically_increasing_id()).rowsBetween(Window.unboundedPreceding, 0)

enaho_2022_variables_binario_df= enaho_2022_variables_binario_df.withColumn("id", F.sum(F.when(F.col("LINEA") == 1, 1).otherwise(0)).over(windowSpec))

enaho_2022_variables_binario_df.show(40)

#enaho_2022_hogar_agr_df = enaho_2022_variables_df.groupby('id',"")
