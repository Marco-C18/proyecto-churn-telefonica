from pyspark import SparkConf
from pyspark.sql import SparkSession

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from datetime import datetime

from pymongo import MongoClient
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

# Integración con MongoDB

# 1. Filtrar usuarios que han hecho churn
churn_df = df.filter(df['churn'] == True)

# 2. Convertir a Pandas
churn_pd = churn_df.toPandas()

# 3. Convertir 'join_date' a tipo datetime.datetime
churn_pd['join_date'] = churn_pd['join_date'].apply(
    lambda d: datetime(d.year, d.month, d.day) if not pd.isnull(d) else None
)

# 4. Conectarse a MongoDB
client = MongoClient("mongodb://localhost:27017/")
db = client["telefonica"]
collection = db["clientes_churn"]

# 5. Insertar en MongoDB
collection.delete_many({})  # Limpiar colección si ya existía
collection.insert_many(churn_pd.to_dict("records"))

print("Datos de clientes con churn almacenados en MongoDB.")
