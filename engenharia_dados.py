# REQUISITO: O cluster Databricks precisa do token/connector do Maven do Spark-EventHubs
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, expr
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType

spark = SparkSession.builder.appName("OrdersRealTimeIngestion").getOrCreate()

# Parâmetros de Conexão com o Event Hubs
EVENT_HUB_CONNECTION_STR = "Endpoint=sb://<seu-namespace>.servicebus.windows.net/;SharedAccessKeyName=<key-name>;SharedAccessKey=<key>;EntityPath=telemetry-orders"
ehConf = {
    'eventhubs.EVENT_HUB_CONNECTION_STR': spark._jvm.org.apache.spark.eventhubs.EventHubsUtils.encryptServicePrincipalConnectionString(EVENT_HUB_CONNECTION_STR)
}

# Schema baseado no dataset do Kaggle
order_schema = StructType([
    StructField("TransactionNo", StringType(), True),
    StructField("Date", StringType(), True),
    StructField("ProductNo", StringType(), True),
    StructField("ProductName", StringType(), True),
    StructField("Price", DoubleType(), True),
    StructField("Quantity", IntegerType(), True),
    StructField("CustomerNo", IntegerType(), True),
    StructField("Country", StringType(), True)
])

# LEITURA EM REAL-TIME (Camada Bronze)
df_bronze = spark.readStream \
    .format("eventhubs") \
    .options(**ehConf) \
    .load()

# O corpo do evento vem criptografado/binário no campo 'body'
df_orders_parsed = df_bronze \
    .withColumn("body_string", col("body").cast("string")) \
    .withColumn("parsed_data", from_json(col("body_string"), order_schema)) \
    .select("parsed_data.*", "enqueuedTime")

# TRANSFORMAÇÃO & LIMPEZA
# Exemplo: Calculando o valor total da transação e convertendo tipos de data
df_silver = df_orders_parsed \
    .withColumn("TotalPrice", col("Price") * col("Quantity")) \
    .withColumn("EventTimestamp", col("Date").cast("timestamp")) \
    .drop("Date")

# ESCRITA EM REAL-TIME (Tabela Delta)
# Utiliza checkpoint para garantir resiliência (Exactly-Once delivery)
checkpoint_path = "/mnt/telemetry/checkpoints/orders_silver"
write_path = "shadow_catalog.orders_squad.orders_silver" 

query = df_silver.writeStream \
    .format("delta") \
    .outputMode("append") \
    .option("checkpointLocation", checkpoint_path) \
    .toTable(write_path)