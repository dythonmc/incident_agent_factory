import pandas as pd
import json
import markdown
import os
import io
import re
import logging # <-- Importamos logging
from typing import Dict, Any, List, Tuple

DATA_BASE_PATH = "data"

# (El código de las funciones es el mismo, solo cambiamos print por logging.info)
def parse_cv_data_and_text(source_id: str) -> Tuple[Dict[str, Any], str]:
    logging.info(f"--- Lógica: Parseando CV Universal desde: data/datasource_cvs/{source_id}_native.md ---")
    md_content = ""
    try:
        with open(os.path.join(DATA_BASE_PATH, "datasource_cvs", f"{source_id}_native.md"), 'r', encoding='utf-8') as f:
            md_content = f.read()
        if "Volume Characteristics (Estimates)" in md_content: cv_type = "Tipo B (Texto)"
        else: cv_type = "Tipo A (Tabla)"
        logging.info(f"--- Lógica: Detectado CV de '{cv_type}' para la fuente {source_id}. ---")
        final_patterns = {"cv_type": cv_type}
        html_content = markdown.markdown(md_content, extensions=['tables'])
        tables = pd.read_html(io.StringIO(html_content), flavor='lxml')
        SCHEMA_PROCESSING_STATS = {'Day', 'Mean Files'}
        for table_df in tables:
            df = table_df.copy()
            if isinstance(df.columns, pd.MultiIndex): df.columns = df.columns.droplevel(0)
            if SCHEMA_PROCESSING_STATS.issubset(set(df.columns)):
                final_patterns["file_processing_stats"] = df
                break
        return final_patterns, md_content
    except Exception as e:
        logging.error(f"Error crítico al parsear la Hoja de Vida de '{source_id}': {e}")
        return {"cv_type": "Error"}, ""

def process_files_json(file_path: str, date_str: str) -> pd.DataFrame:
    # ... (lógica sin cambios)
    try:
        with open(file_path, 'r', encoding='utf-8') as f: data = json.load(f)
        all_files = []
        for source_id, files_list in data.items():
            for file_info in files_list: file_info['source_id'] = source_id; all_files.append(file_info)
        if not all_files: return pd.DataFrame()
        df = pd.DataFrame(all_files); df['uploaded_at'] = pd.to_datetime(df['uploaded_at'], utc=True)
        target_date = pd.to_datetime(date_str).date(); df = df[df['uploaded_at'].dt.date == target_date]
        return df
    except FileNotFoundError: return pd.DataFrame()
    except Exception as e: logging.error(f"Error al procesar el archivo '{file_path}': {e}"); return pd.DataFrame()

def get_all_source_ids() -> List[str]:
    logging.info("--- Lógica: Obteniendo lista maestra de source_ids ---")
    cv_folder_path = os.path.join(DATA_BASE_PATH, "datasource_cvs")
    try:
        all_cv_files = os.listdir(cv_folder_path)
        return [filename.split('_')[0] for filename in all_cv_files if filename.endswith('_native.md')]
    except Exception as e: logging.error(f"Error al leer los source_ids de los CVs: {e}"); return []