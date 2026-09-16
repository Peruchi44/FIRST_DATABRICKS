# Databricks notebook source
from pyspark.sql import functions as F

CAMINHO = "/Volumes/voebem/bronze/arquivos/vra/*.csv"
TABELA = "voebem.bronze.vra"

# COMMAND ----------

bruto = (
    spark.read.format("csv")
    .option("sep", ";")
    .option("header", "true")
    .option("skipRows", 1)
    .option("quote", "\"")
    .option("escape", "\"")
    .option("encoding", "UTF-8")
    .option("mode", "PERMISSIVE")
    .load(CAMINHO)
)

print("colunas lidas do arquivo: ")
for c in bruto.columns:
    print(f"{c!r}")

# COMMAND ----------

RENOMEAR = {
    "ICAO Empresa Aérea": "icao_empresa_aerea",
    "Número Voo": "numero_voo",
    "Código Autorização (DI)": "codigo_autorizacao_di",
    "Código Tipo Linha": "codigo_tipo_linha",
    "ICAO Aeródromo Origem": "icao_aerodromo_origem",
    "ICAO Aeródromo Destino": "icao_aerodromo_destino",
    "Partida Prevista": "partida_prevista",
    "Partida Real": "partida_real",
    "Chegada Prevista": "chegada_prevista",
    "Chegada Real":
        "chegada_real",
    "Situação Voo": "situacao_voo",
    "Código Justificativa": "codigo_justificativa",
}

faltando = [c for c in RENOMEAR if c not in bruto.columns]
assert not faltando, f"Coluna esperada nao encontrada no CSV: {faltando}"

renomeado = bruto.select(
    *[F.col(f"`{origem}`").cast("string").alias(novo) for origem, novo in RENOMEAR.items()]
)



# COMMAND ----------

bronze = (
    renomeado .withColumn(
        "arquivo_origem", F.col("_metadata.file_name")
        ).withColumn(
            "_ingerido_em", F.current_timestamp())
)
display(bronze)

# COMMAND ----------

(
    bronze.write.format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(TABELA)
)

print(f"{TABELA}: {spark.table(TABELA).count():,} linhas")

# COMMAND ----------

spark.sql(f"""
          COMMENT ON TABLE {TABELA} IS 
          'Bronze - VRA (Voo Regular Ativo) da ANAC, 12 meses (ago/2025 a jul/2026).
          Dado Bruto: todas as colunas string, nenhuma linha descartada.
          Carga full refresh idempotente a partir de /Volumes/voebem/bronze/arquivos/vra/.' 
          """)

# COMMAND ----------

display(
    spark.sql(f"""
        SELECT arquivo_origem, COUNT(*) AS linhas, MAX(_ingerido_em) AS ingerido_em
        FROM {TABELA}
        GROUP BY arquivo_origem
        ORDER BY arquivo_origem
    """)
)