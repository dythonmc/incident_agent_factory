# incident_agent/tools/data_loaders.py

import pandas as pd
import json
import markdown
import os
import io
import re
from typing import Dict, Any, List, Tuple

DATA_BASE_PATH = "data"

def _parse_common_tables(tables_from_html: list) -> dict:
    """Función auxiliar para parsear las 3 tablas comunes a todos los CVs."""
    parsed_tables = {}
    
    # Esquemas para identificar las tablas comunes
    SCHEMA_PROCESSING_STATS = {'Day', 'Mean Files'}
    SCHEMA_UPLOAD_SCHEDULE = {'Day', 'Upload Hour Slot Mean (UTC)'}
    
    for table_df in tables_from_html:
        df = table_df.copy()
        if isinstance(df.columns, pd.MultiIndex):
            df.columns = df.columns.droplevel(0)
        
        column_set = set(df.columns)

        if SCHEMA_PROCESSING_STATS.issubset(column_set):
            parsed_tables["file_processing_stats"] = df
        elif SCHEMA_UPLOAD_SCHEDULE.issubset(column_set):
            parsed_tables["upload_schedule_patterns"] = df
            
    return parsed_tables

def _parse_type_a_summary_table(tables_from_html: list) -> dict:
    """Parsea la tabla de resumen de volumen específica del Tipo A."""
    SCHEMA_DAY_SUMMARY_V1 = {'Day', 'Total Rows Processed', 'Empty Files'}
    for table_df in tables_from_html:
        df = table_df.copy()
        column_set = set(df.columns)
        if SCHEMA_DAY_SUMMARY_V1.issubset(column_set):
            return {"day_of_week_summary": df}
    return {}

def _parse_type_b_summary_text(cv_text: str) -> dict:
    """Parsea el bloque de texto de estadísticas de volumen del Tipo B usando regex."""
    patterns = {}
    
    # Extraer estadísticas del bloque "Summary statistics"
    mean_match = re.search(r'- Mean:\s*([\d\.]+)', cv_text)
    median_match = re.search(r'- Median:\s*([\d\.]+)', cv_text)
    min_match = re.search(r'- Min:\s*(\d+)', cv_text)
    max_match = re.search(r'- Max:\s*(\d+)', cv_text)
    empty_files_match = re.search(r'- Empty files:\s*(\d+)', cv_text)

    # Creamos un diccionario unificado similar a la tabla del Tipo A para consistencia
    stats = {
        "mean_rows": float(mean_match.group(1)) if mean_match else None,
        "median_rows": float(median_match.group(1)) if median_match else None,
        "min_rows": int(min_match.group(1)) if min_match else None,
        "max_rows": int(max_match.group(1)) if max_match else None,
        "total_empty_files": int(empty_files_match.group(1)) if empty_files_match else None,
    }
    patterns["volume_summary_stats"] = stats
    return patterns

def parse_cv_data_and_text(source_id: str) -> Tuple[Dict[str, Any], str]:
    """
    (Lógica Pura - Extractor Universal)
    Detecta el tipo de CV y aplica la lógica de parseo correcta.
    """
    file_path = os.path.join(DATA_BASE_PATH, "datasource_cvs", f"{source_id}_native.md")
    print(f"--- Lógica: Parseando CV Universal desde: {file_path} ---")
    
    md_content = ""
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            md_content = f.read()
        
        # Primero, detectamos el tipo de CV buscando un título único del Tipo B
        if "Volume Characteristics (Estimates)" in md_content:
            cv_type = "Tipo B (Texto)"
        else:
            cv_type = "Tipo A (Tabla)"
        
        print(f"--- Lógica: Detectado CV de '{cv_type}' para la fuente {source_id}. ---")

        # Empezamos a construir nuestro diccionario de patrones unificado
        final_patterns = {"cv_type": cv_type}

        # Extraemos las tablas que son comunes a ambos tipos
        html_content = markdown.markdown(md_content, extensions=['tables'])
        tables = pd.read_html(io.StringIO(html_content), flavor='lxml')
        common_tables = _parse_common_tables(tables)
        final_patterns.update(common_tables)

        # Aplicamos la lógica de extracción específica para cada tipo
        if cv_type == "Tipo A (Tabla)":
            type_a_table = _parse_type_a_summary_table(tables)
            final_patterns.update(type_a_table)
        else: # Es Tipo B
            type_b_stats = _parse_type_b_summary_text(md_content)
            final_patterns.update(type_b_stats)
            
        return final_patterns, md_content
    
    except Exception as e:
        print(f"Error crítico al parsear la Hoja de Vida de '{source_id}': {e}")
        return {"cv_type": "Error"}, ""

# El resto de las funciones de lógica pura no cambian
def process_files_json(file_path: str, date_str: str) -> pd.DataFrame:
    # ... (código sin cambios)
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
    except Exception as e: print(f"Error al procesar el archivo '{file_path}': {e}"); return pd.DataFrame()

def get_all_source_ids() -> List[str]:
    # ... (código sin cambios)
    cv_folder_path = os.path.join(DATA_BASE_PATH, "datasource_cvs")
    try:
        all_cv_files = os.listdir(cv_folder_path)
        return [filename.split('_')[0] for filename in all_cv_files if filename.endswith('_native.md')]
    except Exception as e: print(f"Error al leer los source_ids de los CVs: {e}"); return []