from pyspark import SparkConf
from pyspark.sql import SparkSession

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from datetime import datetime

# Configuración de Spark para ejecución local
conf = SparkConf()
conf.setMaster("local[*]")
conf.setAppName("Telefonica Churn Analysis")
conf.set("spark.driver.bindAddress", "127.0.0.1")
conf.set("spark.driver.host", "127.0.0.1")
conf.set("spark.ui.enabled", "false")

# Crear la sesión de Spark
spark = SparkSession.builder.config(conf=conf).getOrCreate()
print("SparkSession creada exitosamente con nombre:", spark.sparkContext.appName)
print("Modo de ejecucion:", spark.sparkContext.master)

# Cargar el CSV (ajustar ruta si es necesario)
df = spark.read.csv("../data/telefonica.csv", header=True,
                    inferSchema=True, sep=';')

# Mostrar estructura del DataFrame
print("Columnas disponibles:", df.columns)
df.show(5)

# Transformar a RDD usando dos columnas
rdd = df.rdd.map(lambda row: (row['customer_id'], row['age']))
print("Transformacion RDD (customer_id, age):")
print(rdd.take(5))

# Agrupar por tipo de suscripción (DataFrame)
df.select("subscription_type").groupBy("subscription_type").count().show()

# Convertir a Pandas para visualización local
df_pd = df.toPandas()

# Visualizar distribución de edades
sns.histplot(df_pd['age'])
plt.title("Distribucion de edad de clientes")
plt.xlabel("Edad")
plt.ylabel("Cantidad")
plt.tight_layout()
plt.show()
