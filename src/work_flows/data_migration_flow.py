import pandas as pd
import os
from src.connection.sql_engine import local_cnx
from src.constants.paths import current_path
from src.constants.queries import transform_query, amount_query, acount_summary

current_path = "/opt/airflow"
DATA_DIR = os.path.join(current_path,"data")
PROCESSED_DIR = os.path.join(DATA_DIR,"processed")
DB_PATH = os.path.join(DATA_DIR,"reference","local.db")   # tu local.db
#PROCESSED_DIR.mkdir(parents=True, exist_ok=True)

def _get_engine():
    """Crea y devuelve la instancia del engine que tu proyecto utiliza."""
    engine = local_cnx(DB_PATH)
    engine.sql_connection()
    return engine


def transform_records(output_filename: str = "transformed.csv") -> str:
    """
    Ejecuta transform_query y guarda el resultado en CSV.
    Retorna la ruta al CSV (string).
    """
    engine = _get_engine()
    try:
        df = engine.get_sql_table(transform_query)
        # Aseguramos las columnas esperadas
        expected_cols = {"transaction_id", "transaction_date", "account_number",
                         "account_name", "debit_amount", "credit_amount", "is_valid_transaction"}
        if not expected_cols.issubset(set(df.columns)):
            # Si falta transaction_date como datetime, intenta parsearlo
            if "transaction_date" in df.columns:
                df["transaction_date"] = pd.to_datetime(df["transaction_date"], errors="coerce")
        out_path = os.path.join(PROCESSED_DIR, output_filename)
        df.to_csv(out_path, index=False)
        return str(out_path)
    finally:
        engine.close_connection()


def validate_balances(output_filename: str = "validation.csv") -> str:
    """
    Ejecuta amount_query y guarda los transaction_id desbalanceados en CSV.
    Retorna la ruta al CSV.
    """
    engine = _get_engine()
    try:
        df = engine.get_sql_table(amount_query)
        out_path = os.path.join(PROCESSED_DIR, output_filename)
        df.to_csv(out_path, index=False)
        return str(out_path)
    finally:
        engine.close_connection()


def summary_accounts(output_filename: str = "summary.csv") -> str:
    """
    Ejecuta acount_summary y guarda el resumen por cuenta en CSV.
    Retorna la ruta al CSV.
    """
    engine = _get_engine()
    try:
        df = engine.get_sql_table(acount_summary)
        out_path = os.path.join(PROCESSED_DIR, output_filename)
        print(f"=== Acounts summary ===\n{df.to_string()}")
        df.to_csv(out_path, index=False)
        return str(out_path)
    finally:
        engine.close_connection()

def generate_report(transformed_csv_path: str, validated_csv_path: str, summary_csv_path: str, threshold_pct: float = 5.0) -> str:
    """
    Valida el porcentaje de filas con is_valid_transaction == 0 en el CSV transformado.
    Si el porcentaje supera threshold_pct, lanza ValueError para provocar fallo del DAG.
    """
    df = pd.read_csv(str(transformed_csv_path), parse_dates=["transaction_date"], dayfirst=False)
    df_1 = pd.read_csv(str(validated_csv_path))    
    
    total = len(df)
    if total == 0:
        # No hay datos; consideramos que no hay falla de calidad, pero puedes cambiar política
        return

    # Asegúrate que columna exista y sea 0/1. Normalizamos si viene TRUE/FALSE
    if "is_valid_transaction" not in df.columns:
        raise ValueError("Column 'is_valid_transaction' doesnt find in transformed dataset.")
    if "transaction_id" not in df.columns:
        raise ValueError("Column 'transaction_id' not found in transformed dataset.")
    
    df_valid = df[df['is_valid_transaction']==1].copy()
    df_invalid = df[df['is_valid_transaction']==0].copy()

    # Verifica que transaction_id no esté en el dataset validado
    if "transaction_id" in df_1.columns:
        df_valid = df_valid[~df_valid['transaction_id'].isin(df_1['transaction_id'])]
    # --- Cálculos de métricas ---
    invalid_count = len(df_invalid)
    pct_invalid = (invalid_count / total) * 100.0

    if pct_invalid > threshold_pct:
        raise ValueError(
            f"Insufficient amount of valid data: {pct_invalid:.2f}% invalid transactions "
            f"(> {threshold_pct}%)."
        )
    df_valid.drop(columns=["is_valid_transaction"], inplace=True)
    # Genera el reporte en texto
    report_df_txt = df.to_string(index=False)
    report_df_1_txt = df.to_string(index=False)

    output_path = os.path.join(PROCESSED_DIR, "report.txt")
    # --- Guardar reporte ---
    with open(output_path, "w", encoding="utf-8") as f:
        f.write("=== Data Migration Valid Transactions Report ===\n\n")
        f.write(f"Total records processed: {total}\n")
        f.write(f"Valid records after filtering: {len(df_valid)}\n")
        f.write(f"Invalid percentage: {pct_invalid:.2f}%\n\n")
        f.write("=== Transformed Transactions ===\n")
        f.write(report_df_txt)
        f.write("\n")
        f.write("\n")
        f.write("=== Validation Transactions ===\n")
        f.write(report_df_1_txt)
        f.write("\n")
    # Limpieza de archivos temporales
    '''try:
        os.remove(transformed_csv_path)
        os.remove(validated_csv_path)
        os.remove(summary_csv_path)
        print("Temporary files removed.")
    except Exception as e:
        print(f"Temporary files couldnt be removed: {e}")'''
    return output_path