#Importamos librerias necesarias
from pyspark.sql import SparkSession, functions as F
# Inicializa la sesión de Spark
spark = SparkSession.builder.appName('Tarea3').getOrCreate()
# Define la ruta del archivo .csv en HDFS
file_path = 'hdfs://localhost:9000/Tarea3/rows.csv'
# Lee el archivo .csv
df = spark.read.format('csv').option('header','true').option('inferSchema', 'true').load(file_path)
#imprimimos el esquema
df.printSchema()
# Muestra las primeras filas del DataFrame
df.show()
# Estadisticas básicas
df.summary().show()
# Consulta: Filtrar por valor y seleccionar columnas
print("Beneficiarios con valor mayor a 1\n")
dias = df.filter(F.col('CantidadDeBeneficiarios') > 1).select('CantidadDeBeneficiarios','RangoBeneficioConsolidadoAsignado','FechaUltimoBeneficioAsignado')
dias.show()
# Ordenar filas por los valores en la columna "CantidadDeBeneficiarios" en orden descendente
print("Valores ordenados de mayor a menor\n")
sorted_df = df.sort(F.col("CantidadDeBeneficiarios").desc())
sorted_df.show()
# Ordenar filas por los valores en la columna "Bancarizado" en orden descendente
print("Valores ordenados Bancarizado\n")
sorted_df = df.sort(F.col("Bancarizado").desc())
sorted_df.show()
# Ordenar filas por los valores en la columna "Bancarizado" en orden descendente
print("Valores ordenados EstadoBeneficiario\n")
sorted_df = df.sort(F.col("EstadoBeneficiario").desc())
sorted_df.show()