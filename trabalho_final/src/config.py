import os
from pathlib import Path

DIRETORIO_RAIZ = Path(__file__).resolve().parent.parent
DIRETORIO_DADOS = DIRETORIO_RAIZ / "data"
DIRETORIO_MODELOS = DIRETORIO_RAIZ / "models"

CAMINHO_CSV_HISTORICO = DIRETORIO_DADOS / "historico_sensores.csv"
CAMINHO_MODELO_ML = DIRETORIO_MODELOS / "auditor_greenwashing.joblib"
DIRETORIO_CHECKPOINT = str(DIRETORIO_RAIZ / "checkpoints_spark")

KAFKA_BROKER = os.environ.get("KAFKA_BROKER", "localhost:29092")
TOPICO_TELEMETRIA = "sensores_ambientais"
TOPICO_ALERTAS = "alertas_fraude"
TOPICO_ERROS_SCHEMA = "sensores_dlq"

PARTICOES_TOPICO = int(os.environ.get("PARTICOES_TOPICO", "2"))
FATOR_REPLICACAO = int(os.environ.get("FATOR_REPLICACAO", "1"))
LIMIAR_PROBABILIDADE = float(os.environ.get("LIMIAR_PROBABILIDADE", "0.75"))

FABRICAS = [
    {"id": "f1",   "seed": 10,  "size": 1.00, "fraud_intensity": None},
    {"id": "f2",   "seed": 20,  "size": 1.15, "fraud_intensity": None},
    {"id": "f3",   "seed": 30,  "size": 0.90, "fraud_intensity": None},
    {"id": "f4",   "seed": 60,  "size": 1.05, "fraud_intensity": None},
    {"id": "f5",   "seed": 70,  "size": 0.95, "fraud_intensity": None},
    {"id": "f6",   "seed": 80,  "size": 1.10, "fraud_intensity": None},
    {"id": "f7",   "seed": 40,  "size": 1.00, "fraud_intensity": 0.70},
    {"id": "f8",   "seed": 50,  "size": 1.10, "fraud_intensity": 0.80},
    {"id": "f9",   "seed": 90,  "size": 0.95, "fraud_intensity": 0.75},
    {"id": "f10",  "seed": 100, "size": 1.05, "fraud_intensity": 0.85},
]
