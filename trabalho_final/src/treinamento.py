import joblib
import pandas as pd
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import classification_report

from src import config
from src.atributos import extrair_atributos


def executar_treinamento():
    config.DIRETORIO_MODELOS.mkdir(parents=True, exist_ok=True)
    df = pd.read_csv(config.CAMINHO_CSV_HISTORICO).dropna(
        subset=["co2_ppm", "nox_ppm", "pm25_ugm3"]
    )

    X = extrair_atributos(df)
    turno_inspecao = (X["dia_semana"] < 5) & (X["hora"] >= 8) & (X["hora"] < 17)
    y = ((df["fabrica_fraudadora"] == 1) & turno_inspecao).astype(int)

    fabricas_unicas = df["factory_id"].unique()
    fabricas_teste = pd.Series(fabricas_unicas).sample(frac=0.30, random_state=7)
    mascara_teste = df["factory_id"].isin(fabricas_teste)

    X_treino, X_teste = X[~mascara_teste], X[mascara_teste]
    y_treino, y_teste = y[~mascara_teste], y[mascara_teste]

    print(f"Fábricas reservadas para teste (inéditas): {sorted(fabricas_teste.tolist())}")

    classificador = RandomForestClassifier(
        n_estimators=150, class_weight="balanced", random_state=42
    )
    classificador.fit(X_treino, y_treino)

    print("\n=== Desempenho em Fábricas Inéditas (Random Forest) ===")
    print(classification_report(y_teste, classificador.predict(X_teste), target_names=["Normal", "Fraude"]))

    joblib.dump(classificador, config.CAMINHO_MODELO_ML)
    print(f"Modelo serializado em '{config.CAMINHO_MODELO_ML}'.")


if __name__ == "__main__":
    executar_treinamento()
