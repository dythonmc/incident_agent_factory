# incident_agent/tools/detectors.py

import pandas as pd
from typing import List, Dict, Any

def find_missing_files(
    daily_files_df: pd.DataFrame,
    cv_data: Dict[str, pd.DataFrame],
    source_id: str,
    date_str: str
) -> List[Dict[str, Any]]:
    """
    (Lógica Pura) Detecta si faltan archivos para una fuente de datos.
    Toma DataFrames como entrada y devuelve una lista de incidencias.
    """
    incidents = []
    try:
        day_of_week = pd.to_datetime(date_str).day_name()
        processing_stats = cv_data.get("file_processing_stats")
        if processing_stats is None or processing_stats.empty:
            return incidents
        
        processing_stats.columns = [str(col).lower() for col in processing_stats.columns]
        day_stats = processing_stats[processing_stats['day'].str.lower() == day_of_week.lower()]
        if day_stats.empty:
            return incidents
        
        expected_files_mean = day_stats['mean files'].iloc[0]
        actual_files_count = len(daily_files_df[daily_files_df['source_id'] == source_id])
        
        if actual_files_count < expected_files_mean:
            missing_count = int(expected_files_mean - actual_files_count)
            if missing_count >= 1:
                incidents.append({
                    "source_id": source_id, "incident_type": "Missing File",
                    "description": f"Faltan {missing_count} archivos. Se esperaban ~{expected_files_mean:.0f}, se recibieron {actual_files_count}.",
                    "severity": "URGENT", "date": date_str,
                })
        return incidents
    except Exception as e:
        print(f"Error en la lógica de 'find_missing_files' para {source_id}: {e}")
        return []

def find_duplicated_or_failed_files(
    daily_files_df: pd.DataFrame,
    source_id: str,
    date_str: str
) -> List[Dict[str, Any]]:
    """
    (Lógica Pura) Detecta archivos duplicados o con estado de procesamiento fallido.
    """
    incidents = []
    source_files_df = daily_files_df[daily_files_df['source_id'] == source_id]
    if source_files_df.empty:
        return incidents

    # Nos aseguramos de que la columna 'status' sea de tipo string y manejamos valores nulos (NaN)
    source_files_df = source_files_df.copy()
    source_files_df.loc[:, 'status'] = source_files_df['status'].astype(str).fillna('unknown')

    # --- 1. Detección de Duplicados ---
    duplicates_df = source_files_df[
        (source_files_df['is_duplicated'] == True) & 
        (source_files_df['status'].str.lower() == 'stopped')
    ]
    for _, row in duplicates_df.iterrows():
        incidents.append({
            "source_id": source_id, "incident_type": "Duplicated File",
            "description": f"Archivo duplicado detectado: '{row['filename']}'.", "severity": "URGENT",
            "date": date_str, "details": { "filename": row['filename'], "status": row['status'] }
        })

    # --- 2. Detección de Archivos Fallidos ---
    failed_files_df = source_files_df[source_files_df['status'].str.lower() != 'processed']
    if not duplicates_df.empty:
        failed_files_df = failed_files_df[~failed_files_df.index.isin(duplicates_df.index)]
    
    for _, row in failed_files_df.iterrows():
        incidents.append({
            "source_id": source_id, "incident_type": "Failed File",
            "description": f"Archivo con procesamiento fallido: '{row['filename']}' (Estado: {row['status']}).", "severity": "REQUIERE ATENCIÓN",
            "date": date_str, "details": { "filename": row['filename'], "status": row['status'], "status_message": row['status_message'] }
        })

    return incidents