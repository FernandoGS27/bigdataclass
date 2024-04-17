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

    expected_ds = spark_session.createDataFrame([(1,500),(2,1700),(3,2000),(4,500)],\
                                                 ['numero_caja','Total_Vendido'])

    actual_ds.show()
    expected_ds.show()

    assert actual_ds.collect() == expected_ds.collect()

def test_calcular_metricas(spark_session):

    cajas_data = [(1,"A",4,100.0), (1,"B",3,200.0),(1,"C",2,200.0),(2,"D",1,1000.0), (2,"E",3,700.0), (3,"A",1,100.0),(3,"C",4,200.0),(3,"D",6,1000.0),(3,"E",3,700.0),(4,"B",2,300.0),(4,"C",4,200.0)]
    cajas_ds = spark_session.createDataFrame(cajas_data, ['numero_caja','Nombre','Cantidad','Precio_Unitario'])

    total_producto_data = [('A',5),('B',5),('D',7),('C',10),('E',6)]
    total_producto_ds = spark_session.createDataFrame(total_producto_data, ['Nombre','Cantidad_Total'])

    total_ventas_data = [(1,500.0),(2,1700.0),(3,2000.0),(4,500.0)]
    total_ventas_ds = spark_session.createDataFrame(total_ventas_data,['numero_caja','Total_Vendido'])

    actual_ds = calcular_metricas(cajas_ds,total_ventas_ds,total_producto_ds)

    expected_ds = spark_session.createDataFrame([("caja_con_mas_ventas","3"),("caja_con_menos_ventas","4"),("percentil_25_por_caja","500.0"),("percentil_50_por_caja","500.0"),
                                                ("percentil_75_por_caja","1700.0"),("producto_mas_vendido_por_unidad","C"),
                                                    ("producto_de_mayor_ingreso","D")],['Tipo_de_Metrica','Valor'])

    actual_ds.show()
    expected_ds.show()

    assert actual_ds.collect() == expected_ds.collect()


