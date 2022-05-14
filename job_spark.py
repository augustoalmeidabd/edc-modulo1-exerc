from pyspark.sql.functions import mean, max, min, col, count
from pyspark.sql import SparkSession

#comentario para modificar
spark = (
    SparkSession.builder.appName("ExerciseSpark")
    .getOrCreate()
)

#ler dados do enem 2020
enem = (
    spark
    .read
    .format("csv")
    .option("header", True)
    .option("inferSchema", True)
    .option("delimiter",";")
    .load("s3://datalake-augusto-igti-edc/raw-data/enem/")
)


(
    enem
    .write1
    .format("csv")
    .mode("overwrite")
    .format("parquet")
    .partitionBy("NU_ANO")
    .save("s3://datalake-augusto-igti-edc/staging/enem/")
)