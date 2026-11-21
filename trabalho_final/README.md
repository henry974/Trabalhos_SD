# Auditoria Ambiental em Tempo Real

Pipeline de streaming para detecção de fraudes em emissões industriais ($\text{CO}_2$, $\text{NO}_x$ e $\text{PM}_{2.5}$) usando **Apache Kafka**, **PySpark Structured Streaming** e **Scikit-Learn**.

O sistema simula fábricas enviando leituras de sensores continuamente. Parte dessas fábricas reduz artificialmente os valores medidos durante o horário comercial de inspeção (segunda a sexta, das 8h às 17h), e o consumidor em PySpark identifica essas anomalias em tempo real.

## Estrutura do projeto

- `generator/` — Simulador físico das fábricas e do dispositivo de fraude.
- `src/config.py` — Configurações do broker Kafka, tópicos, caminhos e parâmetros das fábricas.
- `src/atributos.py` — Extração de variáveis compartilhada entre treino e inferência, além do diagnóstico de motivos do alerta.
- `src/produtor.py` — CLI para gerar a base histórica em CSV e enviar telemetria ao Kafka.
- `src/treinamento.py` — Treinamento do modelo Random Forest com validação em fábricas inéditas.
- `src/consumidor_spark.py` — Motor PySpark com deduplicação, Dead Letter Queue (DLQ) e inferência em lote via `pandas_udf`.

## Pré-requisitos

- **Python 3.10+**
- **Java 17** (necessário para o PySpark)
- **Docker** e **Docker Compose**

## Instalação

Crie o ambiente virtual e instale as dependências:

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

## Como rodar

### 1. Subir o Kafka

```bash
docker compose up -d
```

### 2. Gerar os dados históricos e treinar o modelo

```bash
python -m src.produtor --modo historico
python -m src.treinamento
```

Isso cria o histórico em `data/` e salva o modelo treinado em `models/`.

### 3. Iniciar o consumidor PySpark

No **Terminal 1** (com o `.venv` ativado):

```bash
python -m src.consumidor_spark
```

O consumidor cria os tópicos automaticamente no Kafka. Aguarde a mensagem `Motor PySpark em execução...`.

### 4. Iniciar o produtor de telemetria

Em um **Terminal 2** (com o `.venv` ativado), rode em modo acelerado para demonstração:

```bash
python -m src.produtor --modo demo
```

*(Para enviar leituras usando o relógio real da máquina, use `--modo kafka`.)*

## Tópicos Kafka utilizados

- `sensores_ambientais` — Leituras brutas enviadas pelo produtor.
- `alertas_fraude` — Eventos classificados como fraude pelo modelo.
- `sensores_dlq` — Mensagens com JSON inválido desviadas sem interromper o streaming.

## Encerramento e limpeza

Pressione `Ctrl+C` nos terminais do produtor e do consumidor, depois derrube os containers:

```bash
docker compose down
```

Se precisar reiniciar o Kafka do zero, remova também a pasta local de checkpoints do Spark antes de rodar novamente:

```bash
rm -rf checkpoints_spark/
```