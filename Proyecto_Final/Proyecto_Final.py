from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
import findspark

findspark.init('/usr/lib/python3.7/site-packages/pyspark')

spark = SparkSession \
    .builder \
    .appName("Basic JDBC pipeline") \
    .config("spark.driver.extraClassPath", "postgresql-42.2.14.jar") \
    .config("spark.executor.extraClassPath", "postgresql-42.2.14.jar") \
    .getOrCreate()

construccion_df = spark.read.csv("Base_Anonimizada2022.csv",header=True,inferSchema=True)

##Se carga la base de datos que mapea los cantones con su respectivo codigo y se limpia

cantones_df =spark.read.csv("SEN_GEOGRAFICO_1.csv",header=True,inferSchema=True)

##Se carga la base de datos que mapea cada canton con la region

regiones_df = spark.read.csv("division_territorial_por_region.csv",header=True,inferSchema=True)


##Dato que el proyecto se enfoca en construcciones residenciales. Se quitan del dataset todas las construcciones no residenciales

def construcciones_region_df_func(constru_df,canton_df,region_df):

    construccion_residencial_df = constru_df.filter(constru_df.claobr==1)
    construccion_residencial_df = construccion_residencial_df.select("pro_num_prov","pc_num_cant","num_obras","arecon","numviv","numapo","numdor","valobr")
    construccion_residencial_agrupada_df = construccion_residencial_df.groupby("pro_num_prov","pc_num_cant").agg(F.avg("num_obras").alias("pro_num_obras"),F.avg("arecon").alias("prom_arecon"),
                                                                                                            F.avg("numviv").alias("prom_numviv"),F.avg("numapo").alias("prom_numapo"),
                                                                                                            F.avg("numdor").alias("prom_numdor"),F.avg("valobr").alias("prom_valobr"))
    
    cantones_codigo_df = canton_df.withColumn("Codigo_DTA",F.split(canton_df["CodigoDTA"],",")[0])\
                                .withColumn("Canton",F.split(canton_df["CodigoDTA"],",")[1]).drop("CodigoDTA","Nombre")
    
    for column in cantones_codigo_df.columns:
        cantones_codigo_df = cantones_codigo_df.withColumn(column,F.regexp_replace(column,'"',''))

    regiones_limpio_df = region_df.withColumn("Codigo_DTA",F.substring("CODIGO",1,3)).select("Codigo_DTA","CANTON","REGION").distinct()

    construccion_cantones_df=construccion_residencial_agrupada_df.join(cantones_codigo_df,construccion_residencial_agrupada_df["pc_num_cant"]==cantones_codigo_df["Codigo_DTA"],
                                                                   how="inner").drop("pc_num_cant","pro_num_prov")
    
    construccion_regiones_df=construccion_cantones_df.join(regiones_limpio_df,on="Codigo_DTA",how="left").drop("Canton","CANTON","Codigo_DTA")

    columnas_promedio = [col for col in construccion_regiones_df.columns if col!="REGION"]

    construccion_regiones_agrupada_df=construccion_regiones_agrupada_df.withColumn("Codigo_Region",
    F.when(construccion_regiones_agrupada_df["REGION"]=="CENTRAL", 1)
    .when(construccion_regiones_agrupada_df["REGION"]=="CHOROTEGA", 2)
    .when(construccion_regiones_agrupada_df["REGION"]=="PACIFICO CENTRAL", 3)
    .when(construccion_regiones_agrupada_df["REGION"]=="BRUNCA", 4)
    .when(construccion_regiones_agrupada_df["REGION"]=="HUETAR CARIBE", 5)
    .when(construccion_regiones_agrupada_df["REGION"]=="HUETAR NORTE", 6))

    return construccion_regiones_agrupada_df

    



construccion_residencial_df = construccion_df.filter(construccion_df.claobr==1)



## Se seleccionan las variables de interes

construccion_residencial_df = construccion_residencial_df.select("pro_num_prov","pc_num_cant","num_obras","arecon","numviv","numapo","numdor","valobr")

construccion_residencial_df.summary().show()

construccion_residencial_agrupada_df = construccion_residencial_df.groupby("pro_num_prov","pc_num_cant").agg(F.avg("num_obras").alias("pro_num_obras"),F.avg("arecon").alias("prom_arecon"),
                                                                                                            F.avg("numviv").alias("prom_numviv"),F.avg("numapo").alias("prom_numapo"),
                                                                                                            F.avg("numdor").alias("prom_numdor"),F.avg("valobr").alias("prom_valobr"))

construccion_residencial_agrupada_df.show()



cantones_codigo_df = cantones_df.withColumn("Codigo_DTA",F.split(cantones_df["CodigoDTA"],",")[0])\
                                .withColumn("Canton",F.split(cantones_df["CodigoDTA"],",")[1]).drop("CodigoDTA","Nombre")

for column in cantones_codigo_df.columns:
    cantones_codigo_df = cantones_codigo_df.withColumn(column,F.regexp_replace(column,'"',''))

cantones_codigo_df.show()



regiones_limpio_df = regiones_df.withColumn("Codigo_DTA",F.substring("CODIGO",1,3)).select("Codigo_DTA","CANTON","REGION").distinct()

regiones_limpio_df.show()


##Se une los datos de contruccion con los datos de cantones

construccion_cantones_df=construccion_residencial_agrupada_df.join(cantones_codigo_df,construccion_residencial_agrupada_df["pc_num_cant"]==cantones_codigo_df["Codigo_DTA"],
                                                                   how="inner").drop("pc_num_cant","pro_num_prov")

construccion_cantones_df.show()

##Se une los datos de construccion y cantones con los datos de region

construccion_regiones_df=construccion_cantones_df.join(regiones_limpio_df,on="Codigo_DTA",how="left").drop("Canton","CANTON","Codigo_DTA")


construccion_regiones_df.show()

#Se agrupa por Region
columnas_promedio = [col for col in construccion_regiones_df.columns if col!="REGION"]

#Se crea la variable que contiene el codigo de cada region
construccion_regiones_agrupada_df = construccion_regiones_df.groupby("REGION").agg(*(F.avg(col).alias("reg_"+col) for col in columnas_promedio))

construccion_regiones_agrupada_df.show()

construccion_regiones_agrupada_df=construccion_regiones_agrupada_df.withColumn("Codigo_Region",
    F.when(construccion_regiones_agrupada_df["REGION"]=="CENTRAL", 1)
    .when(construccion_regiones_agrupada_df["REGION"]=="CHOROTEGA", 2)
    .when(construccion_regiones_agrupada_df["REGION"]=="PACIFICO CENTRAL", 3)
    .when(construccion_regiones_agrupada_df["REGION"]=="BRUNCA", 4)
    .when(construccion_regiones_agrupada_df["REGION"]=="HUETAR CARIBE", 5)
    .when(construccion_regiones_agrupada_df["REGION"]=="HUETAR NORTE", 6)
)

construccion_regiones_agrupada_df.show()


enaho_2022_df = spark.read.csv("BdBasePublica.csv",header=True,inferSchema=True)

enaho_2022_variables_df = enaho_2022_df.select("ID_HOGAR","LINEA","REGION","ZONA","ithb","Escolari","C2A4","TamViv","V18J1","V18F1","V2A")

"La variable 'Tenencia de Viviennda' contiene 5 categorias. Para efectos de este trabajo se agrupan en solo 2. Casa Propia (1) Casa que no es propia (0)"

enaho_2022_variables_df.show()

enaho_2022_variables_binario_df = enaho_2022_variables_df.withColumn("Tenencia_Vivienda", F.when(enaho_2022_variables_df.V2A.isin([1,2]),1).otherwise(0)).drop("V2A","ID_HOGAR")




'''Los datos de ENAHO vienen a nivel de hogar y nivel individual. Para el siguiente trabajo nos interesa utilizar las variables a nivel de hogar y agrupar aquellas que vienen a nivel individual.
Para esto se crea un identificador a nivel de cada hogar y se grupan las variables a nivel individual'''

windowSpec = Window.orderBy(F.monotonically_increasing_id()).rowsBetween(Window.unboundedPreceding, 0)

enaho_2022_variables_binario_df= enaho_2022_variables_binario_df.withColumn("id", F.sum(F.when(F.col("LINEA") == 1, 1).otherwise(0)).over(windowSpec)).drop("LINEA")

enaho_2022_variables_binario_df.show(40)

enaho_2022_hogar_agr_df = enaho_2022_variables_binario_df.groupby("id","REGION","Tenencia_Vivienda","TamViv","V18J1","V18F1","ZONA").agg(F.sum("Escolari").alias("suma_escolari_hogar"),
                                                                                                                                F.sum("C2A4").alias("suma_horas_trab_hogar"),
                                                                                                                                F.max("ithb").alias("Ingreso_Total_Bruto_Hogar")) \
                                                                                                                                .orderBy(F.col("id").asc())
enaho_2022_hogar_agr_df.show(40)

enaho_2022_hogar_renombrado_df = enaho_2022_hogar_agr_df.withColumnRenamed("TamViv","Cantidad_Personas") \
                                                        .withColumnRenamed("V18J1","Cantidad_vehiculos").withColumnRenamed("V18F1","Cantidad_Computadoras")\
                                                        .withColumnRenamed("REGION","Region_Geo")
enaho_2022_hogar_renombrado_df.show()

# Se unen los datos de la enaho con los datos constructivos agreegados correspondientes a cada region.

tenencia_vivienda_df = enaho_2022_hogar_renombrado_df.join(construccion_regiones_agrupada_df,enaho_2022_hogar_renombrado_df["Region_Geo"]==construccion_regiones_agrupada_df["Codigo_Region"],
                                                           how='left')

tenencia_vivienda_df.show()

# tenencia_vivienda_df\
#     .write \
#     .format("jdbc") \
#     .mode('overwrite') \
#     .option("url", "jdbc:postgresql://host.docker.internal:5433/postgres") \
#     .option("user", "postgres") \
#     .option("password", "testPassword") \
#     .option("dbtable", "Proyecto_Final_1") \
#     .save()


test = construcciones_region_df_func(construccion_df,cantones_df,regiones_df)

test.show()