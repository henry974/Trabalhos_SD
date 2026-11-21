import joblib
import pandas as pd
import pyspark
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, pandas_udf, struct, to_json, to_timestamp
from pyspark.sql.types import ArrayType, DoubleType, IntegerType, StringType, StructField, StructType

from src import config
from src.atributos import diagnosticar_motivos, extrair_atributos

ESQUEMA_TELEMETRIA = StructType(
    [
        StructField("event_id", StringType(), True),
        StructField("factory_id", StringType(), True),
        StructField("sensor_id", StringType(), True),
        StructField("ts", StringType(), True),
        StructField("co2_ppm", DoubleType(), True),
        StructField("nox_ppm", DoubleType(), True),
        StructField("pm25_ugm3", DoubleType(), True),
        StructField("schema_version", IntegerType(), True),
    ]
)

ESQUEMA_INFERENCIA_ML = StructType(
    [
        StructField("probabilidade", DoubleType(), True),
        StructField("motivos", ArrayType(StringType()), True),
    ]
)


def inicializar_topicos():
    admin = KafkaAdminClient(bootstrap_servers=config.KAFKA_BROKER)
    existentes = set(admin.list_topics())
    pendentes = [
        NewTopic(
            name=nome,
            num_partitions=config.PARTICOES_TOPICO,
            replication_factor=config.FATOR_REPLICACAO,
        )
        for nome in [config.TOPICO_TELEMETRIA, config.TOPICO_ALERTAS, config.TOPICO_ERROS_SCHEMA]
        if nome not in existentes
    ]
    if pendentes:
        try:
            admin.create_topics(new_topics=pendentes, validate_only=False)
            print(f"Tópicos criados no Kafka: {[t.name for t in pendentes]}")
        except TopicAlreadyExistsError:
            pass
    admin.close()


def iniciar_auditoria_streaming():
    inicializar_topicos()
    modelo_ml = joblib.load(config.CAMINHO_MODELO_ML)

    versao_scala = "2.13" if pyspark.__version__.startswith("4.") else "2.12"
    pacote_kafka = f"org.apache.spark:spark-sql-kafka-0-10_{versao_scala}:{pyspark.__version__}"

    spark = (
        SparkSession.builder.appName("AuditoriaAmbientalTempoReal")
        .master("local[2]")
        .config("spark.jars.packages", pacote_kafka)
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.driver.memory", "1g")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    @pandas_udf(ESQUEMA_INFERENCIA_ML)
    def auditar_emissoes_udf(
        ts: pd.Series, co2: pd.Series, nox: pd.Series, pm25: pd.Series
    ) -> pd.DataFrame:
        lote_bruto = pd.DataFrame({"ts": ts, "co2_ppm": co2, "nox_ppm": nox, "pm25_ugm3": pm25})
        X = extrair_atributos(lote_bruto)
        probas = modelo_ml.predict_proba(X)[:, 1]

        lista_motivos = [
            diagnosticar_motivos(X.iloc[i])
            if float(probas[i]) > config.LIMIAR_PROBABILIDADE
            else []
            for i in range(len(X))
        ]
        return pd.DataFrame(
            {
                "probabilidade": [round(float(p), 4) for p in probas],
                "motivos": lista_motivos,
            }
        )

    fluxo_bruto = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", config.KAFKA_BROKER)
        .option("subscribe", config.TOPICO_TELEMETRIA)
        .option("startingOffsets", "latest")
        .load()
    )

    fluxo_decodificado = fluxo_bruto.selectExpr("CAST(value AS STRING) as payload_bruto").withColumn(
        "leitura", from_json(col("payload_bruto"), ESQUEMA_TELEMETRIA)
    )

    fluxo_decodificado.filter(col("leitura").isNull()).select(
        col("payload_bruto").alias("value")
    ).writeStream.outputMode("append").format("kafka").option(
        "kafka.bootstrap.servers", config.KAFKA_BROKER
    ).option(
        "topic", config.TOPICO_ERROS_SCHEMA
    ).option(
        "checkpointLocation", f"{config.DIRETORIO_CHECKPOINT}/dlq"
    ).trigger(processingTime="2 seconds").start()

    fluxo_auditado = (
        fluxo_decodificado.filter(col("leitura").isNotNull())
        .select("leitura.*")
        .withColumn("event_time", to_timestamp(col("ts")))
        .withWatermark("event_time", "2 minutes")
        .dropDuplicates(["event_id"])
        .na.drop(subset=["co2_ppm", "nox_ppm", "pm25_ugm3"])
        .withColumn(
            "auditoria",
            auditar_emissoes_udf(col("ts"), col("co2_ppm"), col("nox_ppm"), col("pm25_ugm3")),
        )
        .withColumn("probabilidade", col("auditoria.probabilidade"))
        .withColumn("motivos", col("auditoria.motivos"))
        .withColumn("fraude_detectada", col("probabilidade") > config.LIMIAR_PROBABILIDADE)
        .drop("auditoria", "event_time")
    )

    fluxo_auditado.select(
        "factory_id", "ts", "co2_ppm", "fraude_detectada", "probabilidade", "motivos"
    ).writeStream.outputMode("append").format("console").option("truncate", False).trigger(processingTime="2 seconds").start()

    fluxo_auditado.filter(col("fraude_detectada")).select(
        col("factory_id").alias("key"),
        to_json(
            struct(
                "event_id",
                "factory_id",
                "ts",
                "co2_ppm",
                "nox_ppm",
                "probabilidade",
                "motivos",
            )
        ).alias("value"),
    ).writeStream.outputMode("append").format("kafka").option(
        "kafka.bootstrap.servers", config.KAFKA_BROKER
    ).option(
        "topic", config.TOPICO_ALERTAS
    ).option(
        "checkpointLocation", f"{config.DIRETORIO_CHECKPOINT}/alertas"
    ).trigger(processingTime="2 seconds").start()

    print("Motor PySpark em execução...")
    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    iniciar_auditoria_streaming()
