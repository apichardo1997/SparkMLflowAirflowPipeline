# Lab 3: Big Data Architectures

Spark data lake (medallion: landing → formatted → exploitation), spark.ml
classification tracked with MLflow, orchestrated with Airflow.

## Structure

```
config/      config.yaml, .env.example
src/         ingestion.py (A.3), formatting.py (A.4), exploitation.py (A.5),
             training.py (B.1/B.2), schemas/quality/checkpoints/visuals, utils/
notebooks/   training.ipynb (B.1–B.3)
dags/        Airflow DAG (C)
data/        landing/ formatted/ exploitation/  (generated, gitignored)
report/      report.md (A.1, A.2, B.3, assumptions)
```

## Setup

Requires Java 8/11/17 (Spark). The notebook runs on Colab; the first cell mounts
Drive, installs deps, and resolves the project root.

```bash
pip install -r requirements.txt
```

## Usage

```bash
bash scripts/run_local.sh        # A.3 → A.4 → A.5
python -m src.training           # B.1 + B.2
```

All paths and parameters live in `config/config.yaml`
