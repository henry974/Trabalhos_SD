import pandas as pd

COLUNAS_ATRIBUTOS = ["hora", "dia_semana", "co2_ppm", "nox_ppm", "pm25_ugm3"]

def extrair_atributos(df: pd.DataFrame) -> pd.DataFrame:
    dt = pd.to_datetime(df["ts"])
    return pd.DataFrame(
        {
            "hora": dt.dt.hour,
            "dia_semana": dt.dt.day_of_week,
            "co2_ppm": df["co2_ppm"],
            "nox_ppm": df["nox_ppm"],
            "pm25_ugm3": df["pm25_ugm3"],
        }
    )[COLUNAS_ATRIBUTOS]

def diagnosticar_motivos(linha: pd.Series) -> list[str]:
    motivos = []
    if linha["dia_semana"] < 5 and 8 <= linha["hora"] < 17:
        motivos.append("janela_inspecao_ativa")
    if linha["co2_ppm"] < 480:
        motivos.append("queda_anomala_co2")
    if linha["nox_ppm"] < 38:
        motivos.append("queda_anomala_nox")
    return motivos or ["anomalia_multivariada_ml"]
