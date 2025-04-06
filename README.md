# 📊 Pipeline ETL - Receita Federal

Pipeline orquestrado pelo **Airflow (GCP Composer)** que processa dados da Receita Federal seguindo o padrão da arquitetura **Medalhão**.

---

## 📌 Fluxo da DAG

1. **Extração (Raw)**
   - Scraping do site da Receita Federal com `BeautifulSoup`
   - Armazenamento no GCS:  
     `gs://{bucket}/raw/`

2. **Transformação (Trusted)**
   - Job PySpark no Dataproc:
     - Limpeza de dados  
     - Padronização de campos
   - Saída em GCS:  
     `gs://{bucket}/trusted/`

3. **Formatação para Excel**
   - Conversão para CSV com:
     - Encoding `UTF-8 BOM`
     - Delimitadores compatíveis
   - Saída em:  
     `gs://{bucket}/trusted/`

4. **Otimização (Refined)**
   - Conversão para `Parquet` com:
     - Particionamento  
     - Compressão `Snappy`
   - Saída em:  
     `gs://{bucket}/refined/`

---

## ⚙️ Estrutura do Projeto

```bash
.
├── dag.py                      # DAG principal
└── src/
    ├── configs/
    │   ├── gcp_conn.py         # Configuração de conexões GCP
    │   └── dag-config.toml     # Configurações da DAG e variaveis
    ├── scripts/
    │   └── script.py           # Job PySpark
    └── utils/
        ├── scraping.py         # Função de scraping
        └── add_bom.py          # Função para adicionar BOM no CSV
```

---

## 🔧 Pré-requisitos

### ☁️ GCP:
- Composer 2.x
- Cluster Dataproc configurado
- Bucket no GCS com as seguintes pastas:
  - `raw/`
  - `trusted/`
  - `refined/`

### 🐍 Python:
`requirements.txt`:

```txt
beautifulsoup4==4.12.0
pyspark==3.5.0
google-cloud-storage==2.10.0
```

---

## 🚀 Implantação

### Configurar variáveis no Airflow:

```bash
airflow variables set GCP_PROJECT_ID seu-projeto
airflow variables set GCS_BUCKET seu-bucket
```

### Carregar a DAG:

```bash
gcloud composer environments storage dags import \
    --environment=composer-env \
    --location=us-central1 \
    --source=dags/receita_federal_etl.py
```

---

## 📁 Outputs Esperados

| Camada             | Formato   | Localização                                |
|--------------------|-----------|---------------------------------------------|
| Raw                | Original  | `gs://{bucket}/raw/`                        |
| Trusted            | CSV       | `gs://{bucket}/trusted/`                   |
| Trusted (Excel)    | CSV       | `gs://{bucket}/trusted/receita_federal_excel/` |
| Refined            | Parquet   | `gs://{bucket}/refined/`                   |

---

## 🧾 Versão Minimalista

```markdown
# ETL Receita Federal

**Fluxo**: Scraping → Raw → PySpark → Trusted → Excel/Parquet → Refined

**Infra**:
- Airflow (Composer)
- Dataproc (PySpark)
- GCS (Raw/Trusted/Refined)

**Implantação**:
1. Setar variáveis no Airflow
2. Upload da DAG via `gcloud composer`

**Contatos**: [kauesantana_13@hotmail.com]
```

---

📫 **Contato**: [kauesantana_13@hotmail.com]  
🔗 **GCP Composer**, **Dataproc**, **GCS**, **PySpark**, **Airflow**, **BeautifulSoup**
