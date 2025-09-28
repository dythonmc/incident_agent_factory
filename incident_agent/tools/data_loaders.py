# incident_agent/tools/data_loaders.py

import pandas as pd
import json
import markdown
import os
import io
from typing import Dict, Any, List

DATA_BASE_PATH = "data"

def parse_cv_data(source_id: str) -> Dict[str, pd.DataFrame]:
    """
    (Lógica Pura) Lee y PARSEA el archivo CV de una fuente de datos.
    Devuelve un diccionario de DataFrames con los patrones.
    """
    file_path = os.path.join(DATA_BASE_PATH, "datasource_cvs", f"{source_id}_native.md")
    print(f"--- Lógica: Parseando Hoja de Vida desde: {file_path} ---")
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            md_content = f.read()
        
        html_content = markdown.markdown(md_content, extensions=['tables'])
        tables = pd.read_html(io.StringIO(html_content), flavor='lxml')
        
        parsed_tables = {}
        if len(tables) >= 4:
            parsed_tables["file_processing_stats"] = tables[0]
            parsed_tables["upload_schedule_patterns"] = tables[1]
            parsed_tables["day_of_week_summary"] = tables[2]
            
            entity_stats_df = tables[3]
            if isinstance(entity_stats_df.columns, pd.MultiIndex):
                 entity_stats_df.columns = entity_stats_df.columns.droplevel(0)
            parsed_tables["entity_stats_by_day"] = entity_stats_df

        return parsed_tables
    except Exception as e:
        print(f"Error al parsear la Hoja de Vida de '{source_id}': {e}")
        return {}

def process_files_json(file_path: str, date_str: str) -> pd.DataFrame:
    """(Lógica Pura) Carga, aplana y filtra un archivo JSON de archivos."""
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        all_files = []
        for source_id, files_list in data.items():
            for file_info in files_list:
                file_info['source_id'] = source_id
                all_files.append(file_info)
        
        if not all_files: return pd.DataFrame()

        df = pd.DataFrame(all_files)
        df['uploaded_at'] = pd.to_datetime(df['uploaded_at'], utc=True)
        target_date = pd.to_datetime(date_str).date()
        df = df[df['uploaded_at'].dt.date == target_date]
        return df
    except FileNotFoundError:
        return pd.DataFrame()
    except Exception as e:
        print(f"Error al procesar el archivo '{file_path}': {e}")
        return pd.DataFrame()

def get_all_source_ids() -> List[str]:
    """(Lógica Pura) Obtiene la lista maestra de TODOS los source_id desde los CVs."""
    print("--- Lógica: Obteniendo lista maestra de source_ids ---")
    cv_folder_path = os.path.join(DATA_BASE_PATH, "datasource_cvs")
    try:
        all_cv_files = os.listdir(cv_folder_path)
        source_ids = [filename.split('_')[0] for filename in all_cv_files if filename.endswith('_native.md')]
        return source_ids
    except Exception as e:
        print(f"Error al leer los source_ids de los CVs: {e}")
        return []