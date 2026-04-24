"""
function_app.py
---------------
Azure Function (Python v2 programming model)
Timer Trigger — scrive un record alla volta su ADLS Gen2
simulando l'arrivo progressivo di dati per Auto Loader.

Variabili d'ambiente da impostare in Application Settings:
    ADLS_ACCOUNT_NAME   es. "mystorageaccount"
    ADLS_CONTAINER      es. "landing"
    ADLS_FOLDER         es. "auto/raw"
    ADLS_TENANT_ID      tenant Azure AD
    ADLS_CLIENT_ID      client id del Service Principal
    ADLS_CLIENT_SECRET  secret del Service Principal

    TIMER_SCHEDULE      cron expression es. "0 */2 * * * *" (ogni 2 minuti)
                        oppure "*/30 * * * * *" (ogni 30 secondi)
                        Nota: Azure Functions usa 6 campi (secondi inclusi)
"""

import azure.functions as func
import json
import logging
import os
from datetime import datetime, timezone

from azure.identity import ClientSecretCredential
from azure.storage.filedatalake import DataLakeServiceClient

app = func.FunctionApp()

# ------------------------------------------------------------------
# Configurazione da Application Settings
# ------------------------------------------------------------------s
ACCOUNT_NAME  = os.environ["ADLS_ACCOUNT_NAME"]
CONTAINER     = os.environ["ADLS_CONTAINER"]
FOLDER        = os.environ.get("ADLS_FOLDER", "auto/raw")
TENANT_ID     = os.environ["ADLS_TENANT_ID"]
CLIENT_ID     = os.environ["ADLS_CLIENT_ID"]
CLIENT_SECRET = os.environ["ADLS_CLIENT_SECRET"]
SCHEDULE      = os.environ.get("TIMER_SCHEDULE", "*/30 * * * * *")


# Cartella dati — tutti i file auto_consumi_prestazioni*.json vengono caricati automaticamente

DATASET_FOLDER = os.environ.get("ADLS_DATASET_FOLDER", "auto/dataset")


# Chiave usata su ADLS per tracciare l'indice corrente
INDEX_FILE = f"{FOLDER}/_state/current_index.txt"


# ------------------------------------------------------------------
# Helper: client ADLS Gen2
# ------------------------------------------------------------------
def get_adls_client() -> DataLakeServiceClient:
    credential = ClientSecretCredential(TENANT_ID, CLIENT_ID, CLIENT_SECRET)
    return DataLakeServiceClient(
        account_url=f"https://{ACCOUNT_NAME}.dfs.core.windows.net",
        credential=credential,
    )


# ------------------------------------------------------------------
# Helper: leggi indice corrente dallo stato su ADLS
# ------------------------------------------------------------------
def read_index(fs_client: DataLakeServiceClient) -> int:
    try:
        file_client = fs_client.get_file_client(CONTAINER, INDEX_FILE)
        download    = file_client.download_file()
        content     = download.readall().decode("utf-8").strip()
        return int(content)
    except Exception:
        # File non ancora esistente: siamo al primo invio
        return 0


# ------------------------------------------------------------------
# Helper: salva indice aggiornato su ADLS
# ------------------------------------------------------------------
def write_index(fs_client: DataLakeServiceClient, index: int):
    file_client = fs_client.get_file_client(CONTAINER, INDEX_FILE)
    file_client.upload_data(str(index).encode("utf-8"), overwrite=True)


# ------------------------------------------------------------------
# Helper: carica un record su ADLS come file JSON
# ------------------------------------------------------------------
def upload_record(fs_client: DataLakeServiceClient, record: dict, index: int):
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%f")
    filename  = f"{FOLDER}/auto_{index:04d}_{timestamp}.json"

    file_client = fs_client.get_file_client(CONTAINER, filename)
    content     = json.dumps(record, ensure_ascii=False)
    file_client.upload_data(content.encode("utf-8"), overwrite=True)

    logging.info(f"Caricato record {index} → {filename}")
    return filename


# ------------------------------------------------------------------
# Timer Trigger
# ------------------------------------------------------------------
@app.timer_trigger(
    schedule=SCHEDULE,
    arg_name="mytimer",
    run_on_startup=True,
    use_monitor=False,
)
def auto_producer(mytimer: func.TimerRequest) -> None:
    if mytimer.past_due:
        logging.warning("Il timer è in ritardo rispetto allo schedule.")

    # Connessione ADLS
    fs_client = get_adls_client()

    # Carica dataset direttamente da ADLS
    dataset_client = fs_client.get_file_system_client(CONTAINER)
    paths = sorted([p.name for p in dataset_client.get_paths(path=DATASET_FOLDER) if p.name.endswith(".json")])

    veicoli = []
    for path in paths:
        file_cl = fs_client.get_file_client(CONTAINER, path)
        content = file_cl.download_file().readall().decode("utf-8")
        veicoli += json.loads(content)["veicoli"]

    total = len(veicoli)
    logging.info(f"Dataset caricato: {total} veicoli da {len(paths)} file")

    # Leggi indice corrente
    current_index = read_index(fs_client)

    if current_index >= total:
        logging.info(f"Nessun nuovo record da inviare ({current_index}/{total}).")
        return

    # Invia il record corrente
    record   = veicoli[current_index]
    filename = upload_record(fs_client, record, current_index + 1)

    # Aggiorna indice
    next_index = current_index + 1
    write_index(fs_client, next_index)

    logging.info(
        f"[{next_index}/{total}] {record['marca']} {record['modello']} "
        f"({record['motorizzazione']}) → {filename}"
    )
########################################################################################à
    # Invia il record corrente
    record   = veicoli[current_index]
    filename = upload_record(fs_client, record, current_index + 1)

    # Aggiorna indice
    next_index = current_index + 1
    write_index(fs_client, next_index)

    logging.info(
        f"[{next_index}/{total}] {record['marca']} {record['modello']} "
        f"({record['motorizzazione']}) → {filename}"
    )
