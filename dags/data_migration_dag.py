# dags/data_migration_dag.py
from datetime import datetime
import os
import sys
from airflow import DAG
from airflow.operators.python import PythonOperator

# ---------- Determinar rutas sin usar pathlib ----------
# directorio donde está este archivo .py (dentro del contenedor será /opt/airflow/dags)
DAG_DIR = os.path.dirname(__file__)
# raíz del proyecto asumida como el padre de DAG_DIR
PROJECT_ROOT = os.path.dirname(DAG_DIR)
# ruta al src dentro del contenedor
SRC_PATH = os.path.join(PROJECT_ROOT, "src")
# Normalizar a formato de sistema
SRC_PATH = os.path.normpath(SRC_PATH)
# Insertar SRC_PATH en sys.path (si aún no está)
if SRC_PATH not in sys.path:
    sys.path.insert(0, SRC_PATH)

# Rutas a archivos usados por el flujo
DB_PATH = os.path.normpath(os.path.join(PROJECT_ROOT, "data", "reference", "local.db"))
PROCESSED_TRANSFORMED = os.path.normpath(os.path.join(PROJECT_ROOT, "data", "processed", "transformed.csv"))
PROCESSED_VALIDATION = os.path.normpath(os.path.join(PROJECT_ROOT, "data", "processed", "validation.csv"))
PROCESSED_SUMMARY = os.path.normpath(os.path.join(PROJECT_ROOT, "data", "processed", "summary.csv"))

# ---------- Funciones que realizan import dinámico (evita Broken DAG en parse time) ----------
# Tarea para validar si la db existe, si no, crearla
def ensure_database_exists():
    """Verifica si existe local.db; si no, lo crea usando createDataBase()."""
    from src.connection.data_base import createDataBase

    db_dir = os.path.dirname(DB_PATH)
    if not os.path.exists(db_dir):
        os.makedirs(db_dir, exist_ok=True)

    if not os.path.isfile(DB_PATH):
        print("Doesnt find local.db, creating local database...")
        createDataBase()
        print("Local database created successfully.")
    else:
        print("local.db already exists, no action needed.")

# Tarea para transformar los registros
def task_transform():
    """Ejecuta la transformación y guarda CSV."""
    from src.work_flows.data_migration_flow import transform_records
    out = transform_records("transformed.csv")
    print(f"Transformed file: {out}")
    return out

# Tarea para validar saldos
def task_validate():
    """Ejecuta la validación de saldos."""
    from src.work_flows.data_migration_flow import validate_balances
    out = validate_balances("validation.csv")
    print(f"Validated file: {out}")
    return out

# Tarea para generar resumen por cuentas
def task_summary():
    """Genera resumen por cuentas."""
    from src.work_flows.data_migration_flow import summary_accounts
    out = summary_accounts("summary.csv")
    print(f"Summary file: {out}")
    return out

# Tarea para generar reporte de calidad
def task_generate_report():
    """Genera reporte"""
    from src.work_flows.data_migration_flow import generate_report
    out = generate_report(PROCESSED_TRANSFORMED, PROCESSED_VALIDATION, PROCESSED_SUMMARY, threshold_pct=5.0)
    print(f"Report generated: {out}")
    return out


# ---------- DAG definition ----------
with DAG(
    dag_id="data_migration_dag",
    description="Flujo de migración, validación y control de calidad de datos.",
    schedule_interval=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["migration", "etl", "data_quality"],
) as dag:

    t0_create_db = PythonOperator(
        task_id="ensure_database_exists",
        python_callable=ensure_database_exists,
    )

    t1_transform = PythonOperator(
        task_id="transform_records",
        python_callable=task_transform,
    )

    t2_validate = PythonOperator(
        task_id="validate_balances",
        python_callable=task_validate,
    )

    t3_summary = PythonOperator(
        task_id="summary_accounts",
        python_callable=task_summary,
    )

    t4_quality = PythonOperator(
        task_id="genetate_report",
        python_callable=task_generate_report,
    )

    t0_create_db >> t1_transform >> t2_validate >> t3_summary >> t4_quality