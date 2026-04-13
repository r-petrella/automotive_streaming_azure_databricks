import dlt
from pyspark.sql import functions as F
from pyspark.sql.types import *

STORAGE_ACCOUNT = "storageaccountautomotive"
CONTAINER = "landing"

SOURCE_RAW = f"abfss://{CONTAINER}@{STORAGE_ACCOUNT}.dfs.core.windows.net/auto/raw"
SOURCE_SPEC = f"abfss://{CONTAINER}@{STORAGE_ACCOUNT}.dfs.core.windows.net/auto/specifiche/auto_specifiche.csv"
SCHEMA_PATH = f"abfss://{CONTAINER}@{STORAGE_ACCOUNT}.dfs.core.windows.net/auto/_schema_dlt"

# ==============================================================
# BRONZE
# ==============================================================

@dlt.table(
    name="bronze_consumi",
    comment="Dati grezzi di consumi e prestazioni da Auto Loader"
)
def bronze_consumi():
    return (
        spark.readStream
            .format("cloudFiles")
            .option("cloudFiles.format", "json")
            .option("cloudFiles.useNotifications", "false")
            .option("cloudFiles.includeExistingFiles", "true")
            .option("cloudFiles.schemaLocation", SCHEMA_PATH)
            .load(SOURCE_RAW)
    )




@dlt.table(
    name="bronze_specifiche",
    comment="Dati grezzi di specifiche tecniche da csv"
)
def bronze_specifiche():
    return (
        spark.readStream
            .format("csv")
            .option("header", "true")
            .option("inferSchema", "true")
            .load(SOURCE_SPEC)
    )


# ==============================================================
# SILVER
# ==============================================================

@dlt.table(
    name="silver_veicoli",
    comment="Veicoli puliti (con id e marche validi) con join tra consumi e specifiche"
)
@dlt.expect_or_drop("id_valido", "id IS NOT NULL")
@dlt.expect_or_drop("marca valida", "marca IS NOT NULL")
'''
Con .filter() — i record scartati spariscono silenziosamente, non sai quanti ne hai persi né perché.
Con @dlt.expect_or_drop — DLT tiene traccia delle violazioni e le mostra nella UI della pipeline con metriche dedicate: quanti record sono passati, quanti sono stati droppati e per quale regola. È visibile nel grafo della pipeline in tempo reale.
'''

def silver_veicoli():
    consumi = dlt.read_stream("bronze_consumi")
    specifiche = dlt.read("bronze_specifiche")

    return (
        consumi.join(specifiche, on=["id", "marca", "modello"], how="left")
        # nuove colonne case when
        .withColumn("tipo_di_alimentazione", F.when(F.col("consumo_misto_wltp_l100km").isNuLL() & F.col("consumo_elettrico_kwh100km").isNotNull(), "Elettrico").when(F.col("autonomia_elettrica_km").isNotNull(), "PHEV")
                        .when(F.col("motorizzazione").contains("Hybrid"), "Ibrido").otherwise("Termico"))

        .withColumn("zero_emissioni", F.when(F.col("emissioni_co2_gkm")== 0, True ).otherwise(False))
        
        .withColumn("classe_potenza", F.when(F.col("potenza_cv") <= 100, "Lenta").when(F.col("potenza_cv") > 100 & F.col("potenza_cv") <= 200, "Media").otherwise("Veloce"))

        # colonna timestamp
        .withColumn("ingested_at", F.current_timestamp())
    )


# ==============================================================
# GOLD
# ==============================================================

@dlt.table(
    name="gold_per_alimentazione",
    comment="Aggregazioni per tipo di alimentazione"
)
def gold_per_alimentazione():
    return (
        dlt.read("silver_veicoli")
        .groupBy("tipo_di_alimentazione")
        .agg(
            F.count("*").alias("n_veicoli"),
            F.round(F.avg("potenza_cv"), 1).alias("potenza_media_cv"),
            F.round(F.avg("accelerazione_0_100_s"), 1).alias("media_0_100_s"),
            F.round(F.avg("prezzo_base_eur"), 0).alias("media_prezzo_eur"),
            F.round(F.avg("consumo_misto_wltp_l100km"), 1).alias("media_consumo_misto"),
            F.round(F.avg("consumo_elettrico_kwh100km"), 1).alias("media_consumo_elettrico"),
            F.round(F.avg("emissioni_co2_gkm"), 1).alias("media_co2_gkm")
        )
        .orderBy("media_prezzo_eur")
    )

#-------------------------------------------------------

@dlt.table(
    name="gold_per_marca",
    comment="Aggregazioni per marca"
)
def gold_per_alimentazione():
    return (
        dlt.read("silver_veicoli")
        .groupBy("marca")
        .agg(
            F.count("*").alias("n_veicoli"),
            F.round(F.avg("potenza_cv"), 1).alias("potenza_media_cv"),
            F.round(F.avg("accelerazione_0_100_s"), 1).alias("media_0_100_s"),
            F.round(F.avg("velocita_max_kmh"), 1).alias("media_velocita_max"),
            F.round(F.avg("prezzo_base_eur"), 0).alias("media_prezzo_eur"),

        )
        .orderBy(F.col("potenza_media_cv").desc(), F.col("media_velocita_max").desc())
    )

#-------------------------------------------------------

@dlt.table(
    name="gold_per_classe potenza",
    comment="Aggregazioni per classe potenza"
)
def gold_per_alimentazione():
    return (
        dlt.read("silver_veicoli")
        .groupBy("classe_potenza")
        .agg(
            F.count("*").alias("n_veicoli"),
            F.round(F.avg("potenza_cv"), 1).alias("potenza_media_cv"),
            F.round(F.avg("accelerazione_0_100_s"), 1).alias("media_0_100_s"),
            F.round(F.avg("velocita_max_kmh"), 1).alias("media_velocita_max"),
            F.round(F.avg("prezzo_base_eur"), 0).alias("media_prezzo_eur"),

        )
        .orderBy(F.col("potenza_media_cv").desc(), F.col("media_velocita_max").desc())
    )