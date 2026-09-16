# Databricks notebook source
from pyspark.sql import functions as F

REF = "/Volumes/voebem/bronze/arquivos/referencias"

# caractere que NAO existe no arquivo -> desliga o quoting do leitor de CSV
SEM_ASPAS = chr(0)

# COMMAND ----------

aerodromos = (
    spark.read.format("csv")
    .option("sep", ";")
    .option("header", "true")
    .option("skipRows", 1)
    .option("encoding", "ISO-8859-1")
    .option("quote", SEM_ASPAS)
    .load(f"{REF}/AerodromosPublicos.csv")
)

aerodromos = aerodromos.select(
    F.col("`Código OACI`").alias("icao"),
    F.col("CIAD").alias("ciad"),
    F.col("Nome").alias("nome"),
    F.col("`Município`").alias("municipio"),
    F.col("UF").alias("uf"),
    F.col("`Município Servido`").alias("municipio_servido"),
    F.col("`UF Servido`").alias("uf_servido"),
    F.col("Latitude").alias("latitude"),
    F.col("Longitude").alias("longitude"),
    F.col("Altitude").alias("altitude"),
    F.col("`Situação`").alias("situacao"),
).withColumn("_ingerido_em", F.current_timestamp())

aerodromos.write.format("delta").mode("overwrite").option(
    "overwriteSchema", "true"
).saveAsTable("voebem.bronze.aerodromos")

print(f"bronze.aerodromos: {spark.table('voebem.bronze.aerodromos').count():,} linhas")
display(spark.sql("SELECT icao, nome, municipio, uf FROM voebem.bronze.aerodromos WHERE icao IN ('SBRB', 'SBGR', 'SBSP', 'SBFZ')"))

# COMMAND ----------

def ler_empresas(arquivo: str):
    """Le um cadastro de empresas. Sem uniao, sem enriquecimento: uma tabela por arquivo."""
    return (
        spark.read.format("csv")
        .option("sep", ";")
        .option("header", "true")
        .option("skipRows", 1)
        .option("encoding", "UTF-8")
        .option("quote", "\"")
        .load(f"{REF}/{arquivo}")
        .select(
            F.col("ICAO").alias("icao"),
            F.col("Estrangeira").alias("sigla_iata"),
            F.col("Razao").alias("razao_social"),
            F.col("Servico").alias("servico"),
            F.col("Cidade").alias("cidade"),
            F.col("UF").alias("uf"),
            F.col("Ativa").alias("situacao")
        )
        .withColumn("_ingerido_em", F.current_timestamp())
    )

for arquivo, tabela in [
    ("pda_empresas_aereas_nacionais.csv", "voebem.bronze.empresas_nacionais"),
    ("pda_empresas_aereas_estrangeiros.csv", "voebem.bronze.empresas_estrangeiras"),
]:
    ler_empresas(arquivo).write.format("delta").mode("overwrite").option(
        "overwriteSchema", "true"
    ).saveAsTable(tabela)
    print(f"{tabela}: {spark.table(tabela).count():,} linhas")

# COMMAND ----------

display(spark.sql("""
    SELECT 'empresas_nacionais' AS tabela, COUNT(*) AS linhas,
           COUNT(CASE WHEN icao IS NOT NULL AND icao <> '' THEN 1 END) AS com_icao
    FROM voebem.bronze.empresas_nacionais
    UNION ALL
    SELECT 'empresas_estrangeiras', COUNT(*),
           COUNT(CASE WHEN icao IS NOT NULL AND icao <> '' THEN 1 END)
    FROM voebem.bronze.empresas_estrangeiras
"""))

# COMMAND ----------

display(spark.sql("""
    SELECT icao, razao_social, servico, uf, situacao
    FROM voebem.bronze.empresas_nacionais
    WHERE icao IN ('GLO', 'TAM', 'AZU', 'PAM')
    ORDER BY icao
"""))

display(spark.sql("""
    SELECT icao, razao_social, servico, situacao
    FROM voebem.bronze.empresas_estrangeiras
    WHERE icao IN ('AAL', 'TAP', 'AVA', 'ARG')
    ORDER BY icao
"""))

# COMMAND ----------

CODIGOS = [
    ("codigo_di", "0", "Etapa Regular"),
    ("codigo_di", "2", "Etapa Extra"),
    ("codigo_di", "3", "Etapa de Retorno"),
    ("codigo_di", "4", "Inclusão de Etapa"),
    ("codigo_di", "6", "Etapa Não Remunerada Sem Transporte de Objetos"),
    ("codigo_di", "7", "Etapa de Voo de Fretamento"),
    ("codigo_di", "9", "Etapa de Voo Charter"),
    ("codigo_di", "D", "Etapa de Voo Duplicada"),
    ("codigo_di", "E", "Etapa Não Remunerada Com Transporte de Objetos"),
    ("codigo_tipo_linha", "N", "Doméstica Mista"),
    ("codigo_tipo_linha", "C", "Doméstica Cargueira"),
    ("codigo_tipo_linha", "I", "Internacional Mista"),
    ("codigo_tipo_linha", "G", "Internacional Cargueira"),
]

codigos = spark.createDataFrame(CODIGOS, "dominio string, codigo string, descricao string")

codigos.write.format("delta").mode("overwrite").option(
    "overwriteSchema", "true"
).saveAsTable("voebem.bronze.codigos_operacao")

print(f"bronze.codigos_operacao: {spark.table('voebem.bronze.codigos_operacao').count()} linhas")
display(spark.table("voebem.bronze.codigos_operacao"))

# COMMAND ----------

display(spark.sql("SHOW TABLES IN voebem.bronze"))