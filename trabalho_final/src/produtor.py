import argparse
import time
from datetime import datetime, timedelta
import pandas as pd
from kafka import KafkaProducer

from generator.factory import HonestFactory
from generator.fraud import InspectionHoursDefeatDevice
from src import config


def _criar_fabricas():
    lista = []
    for f in config.FABRICAS:
        dispositivo_fraude = (
            InspectionHoursDefeatDevice(intensity=f["fraud_intensity"])
            if f["fraud_intensity"] is not None
            else None
        )
        lista.append(
            HonestFactory(
                factory_id=f["id"], seed=f["seed"], size=f["size"], fraud=dispositivo_fraude
            )
        )
    return lista


def gerar_base_historica(dias: int = 30):
    config.DIRETORIO_DADOS.mkdir(parents=True, exist_ok=True)
    fabricas = _criar_fabricas()
    ids_fraudadores = {f["id"] for f in config.FABRICAS if f["fraud_intensity"] is not None}

    instante = datetime.now() - timedelta(days=dias)
    limite = datetime.now()
    registros = []

    while instante < limite:
        for fabrica in fabricas:
            leitura = fabrica.reading_at(instante)
            if leitura is not None:
                registros.append(
                    {
                        "event_id": leitura.event_id,
                        "factory_id": leitura.factory_id,
                        "ts": leitura.ts,
                        "co2_ppm": leitura.co2_ppm,
                        "nox_ppm": leitura.nox_ppm,
                        "pm25_ugm3": leitura.pm25_ugm3,
                        "fabrica_fraudadora": int(fabrica.factory_id in ids_fraudadores),
                    }
                )
        instante += timedelta(minutes=10)

    pd.DataFrame(registros).to_csv(config.CAMINHO_CSV_HISTORICO, index=False)
    print(f"Base histórica salva em '{config.CAMINHO_CSV_HISTORICO}' ({len(registros)} leituras).")


def transmitir_telemetria(modo_acelerado: bool = False):
    fabricas = _criar_fabricas()
    produtor = KafkaProducer(bootstrap_servers=[config.KAFKA_BROKER])
    relogio_demo = datetime(2026, 11, 16, 6, 30, 0)

    etiqueta = "DEMO ACELERADO" if modo_acelerado else "TEMPO REAL"
    print(f"Produtor [{etiqueta}] -> Tópico '{config.TOPICO_TELEMETRIA}'")

    try:
        while True:
            ts_corrente = relogio_demo if modo_acelerado else datetime.now()
            for fabrica in fabricas:
                leitura = fabrica.reading_at(ts_corrente)
                if leitura is not None:
                    produtor.send(
                        topic=config.TOPICO_TELEMETRIA,
                        key=fabrica.factory_id.encode("utf-8"),
                        value=leitura.to_json(),
                    )
                    print(
                        f"[{ts_corrente.strftime('%a %H:%M')}] {fabrica.factory_id:<11} | CO2: {leitura.co2_ppm:>5.1f} | NOx: {leitura.nox_ppm:>4.1f}"
                    )

            if modo_acelerado:
                relogio_demo += timedelta(minutes=15)
                time.sleep(1.5)
            else:
                time.sleep(2)
    except KeyboardInterrupt:
        print("\nProdutor finalizado.")
    finally:
        produtor.flush(timeout=5)
        produtor.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Simulador e Produtor de Telemetria Ambiental")
    parser.add_argument("--modo", choices=["historico", "kafka", "demo"], required=True)
    args = parser.parse_args()

    if args.modo == "historico":
        gerar_base_historica()
    else:
        transmitir_telemetria(modo_acelerado=(args.modo == "demo"))
