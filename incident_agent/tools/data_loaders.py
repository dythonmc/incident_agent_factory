# incident_agent/tools/data_loaders.py

import pandas as pd
import json
import markdown
import os
import io
from typing import Dict, Any

# Definimos la ruta base a la carpeta de datos para no repetirla
DATA_BASE_PATH = "data"

def load_cv_data(source_id: str) -> Dict[str, pd.DataFrame]:
    """
    Lee y PARSEA el archivo de Hoja de Vida (CV) para una fuente de datos específica.
    Extrae todas las tablas del Markdown y las devuelve como un diccionario de DataFrames de Pandas.

    Args:
        source_id (str): El identificador de la fuente de datos (ej. "195385").

    Returns:
        Dict[str, pd.DataFrame]: Un diccionario donde las claves son los nombres de las tablas
                                 y los valores son los DataFrames correspondientes. O un diccionario
                                 vacío si hay un error.
    """
    file_path = os.path.join(DATA_BASE_PATH, "datasource_cvs", f"{source_id}_native.md")
    print(f"--- Herramienta: Parseando Hoja de Vida desde: {file_path} ---")
    
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            md_content = f.read()
        
        # 1. Convertimos el contenido Markdown a HTML, ACTIVANDO LA EXTENSIÓN DE TABLAS
        html_content = markdown.markdown(md_content, extensions=['tables']) # <--- CAMBIO CLAVE 1
        
        # 2. Pandas lee TODAS las tablas del HTML.
        #    Envolvemos el html_content en io.StringIO para solucionar el FutureWarning.
        tables = pd.read_html(io.StringIO(html_content), flavor='lxml') # <--- CAMBIO CLAVE 2
        
        # 3. Identificamos y nombramos cada tabla para un acceso fácil
        parsed_tables = {}
        if len(tables) >= 4:
            parsed_tables["file_processing_stats"] = tables[0]
            parsed_tables["upload_schedule_patterns"] = tables[1]
            parsed_tables["day_of_week_summary"] = tables[2]
            
            entity_stats_df = tables[3]
            # La tabla de entidades tiene una cabecera de múltiples niveles, la aplanamos.
            if isinstance(entity_stats_df.columns, pd.MultiIndex):
                 entity_stats_df.columns = entity_stats_df.columns.droplevel(0)
            parsed_tables["entity_stats_by_day"] = entity_stats_df

        print(f"--- Herramienta: Se extrajeron {len(parsed_tables)} tablas del CV. ---")
        return parsed_tables

    except FileNotFoundError:
        print(f"Error: No se encontró la Hoja de Vida para la fuente '{source_id}'.")
        return {}
    except Exception as e:
        print(f"Error inesperado al parsear la Hoja de Vida de '{source_id}': {e}")
        return {}

# El resto de las funciones (load_daily_files, etc.) y el bloque de prueba se quedan igual que antes...
def _load_and_process_files_json(file_path: str, date_str: str) -> pd.DataFrame:
    """Función auxiliar interna para cargar, aplanar y filtrar los archivos JSON."""
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
        print(f"ADVERTENCIA: No se encontró el archivo de datos en la ruta: {file_path}")
        return pd.DataFrame()
    except Exception as e:
        print(f"Error inesperado al procesar el archivo '{file_path}': {e}")
        return pd.DataFrame()

def load_daily_files(date_str: str) -> pd.DataFrame:
    """Carga y filtra los archivos procesados para una fecha específica desde el archivo files.json."""
    folder_name = f"{date_str}_20_00_UTC"
    file_path = os.path.join(DATA_BASE_PATH, folder_name, "files.json")
    print(f"--- Herramienta: Cargando archivos del día desde: {file_path} ---")
    return _load_and_process_files_json(file_path, date_str)

def load_historical_files(date_str: str) -> pd.DataFrame:
    """Carga y filtra los archivos de la semana anterior para comparación de patrones."""
    folder_name = f"{date_str}_20_00_UTC"
    file_path = os.path.join(DATA_BASE_PATH, folder_name, "files_last_weekday.json")
    print(f"--- Herramienta: Cargando archivos históricos desde: {file_path} ---")
    return _load_and_process_files_json(file_path, date_str)

# --- Bloque de prueba (lo borraremos más tarde) ---
if __name__ == '__main__':
    print("\n--- EJECUTANDO PRUEBAS LOCALES DE DATA LOADERS (VERSIÓN PARSER DEFINITIVA) ---")
    
    # Prueba 1: Cargar y PARSEAR una Hoja de Vida (CV)
    cv_tables = load_cv_data("195385") 
    print("\n[Prueba 1: Tablas extraídas del CV '195385']")
    if cv_tables:
        print(f"Se encontraron las siguientes tablas: {list(cv_tables.keys())}")
        print("\nEjemplo: 'upload_schedule_patterns'")
        print(cv_tables["upload_schedule_patterns"].head())
    else:
        print("No se pudieron extraer tablas.")
    print("-" * 50)
    
    # Prueba 2: Cargar archivos del día (2025-09-08)
    daily_files_df = load_daily_files("2025-09-08")
    print("\n[Prueba 2: Primeras 5 filas de archivos del 2025-09-08]")
    print(daily_files_df.head())
    print("-" * 50)