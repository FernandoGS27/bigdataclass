from .Tarea_2 import  total_productos, total_cajas, calcular_metricas

def test_total_productos(spark_session):

    cajas_data = [(1,"A",4,100), (1,"B",3,200),(1,"C",2,200),(2,"D",1,1000), (2,"E",3,700), (3,"A",1,100),(3,"C",4,200),(3,"D",6,1000),(3,"E",3,700),(4,"B",2,300),(4,"C",4,200)]
    cajas_ds = spark_session.createDataFrame(cajas_data, ['numero_caja','Nombre','Cantidad','Precio_Unitario'])

    actual_ds = total_productos(cajas_ds)

    expected_ds = spark_session.createDataFrame([('A',5),('B',5),('D',7),('C',10),('E',6)],\
                                                 ['Nombre','Cantidad_Total'])
    actual_ds.show()
    expected_ds.show()

    assert actual_ds.collect() == expected_ds.collect()

def test_total_cajas(spark_session):

    cajas_data = [(1,"A",4,100), (1,"B",3,200),(1,"C",2,200),(2,"D",1,1000), (2,"E",3,700), (3,"A",1,100),(3,"C",4,200),(3,"D",6,1000),(3,"E",3,700),(4,"B",2,300),(4,"C",4,200)]
    cajas_ds = spark_session.createDataFrame(cajas_data, ['numero_caja','Nombre','Cantidad','Precio_Unitario'])

    actual_ds = total_cajas(cajas_ds)

    expected_ds = spark_session.createDataFrame([(1,600),(2,1700),(3,2000),(3,600),(4,500)],\
                                                 ['numero_caja','Total_Vendido'])

    actual_ds.show()
    expected_ds.show()

    assert actual_ds.collect() == expected_ds.collect()

