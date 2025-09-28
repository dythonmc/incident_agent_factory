# incident_agent/tools/detectors.py

import pandas as pd
import re
from typing import List, Dict, Any
from datetime import timedelta

# --- DETECTORES DE INCIDENCIAS (LÓGICA PURA) ---

def find_missing_files(daily_files_df: pd.DataFrame, cv_patterns: Dict[str, Any], source_id: str, date_str: str) -> List[Dict[str, Any]]:
    # ... (código sin cambios)
    incidents = []
    try:
        day_of_week = pd.to_datetime(date_str).day_name()
        processing_stats = cv_patterns.get("file_processing_stats")
        if processing_stats is None or processing_stats.empty: return incidents
        processing_stats.columns = [str(col).lower() for col in processing_stats.columns]
        day_stats = processing_stats[processing_stats['day'].str.lower() == day_of_week.lower()]
        if day_stats.empty: return incidents
        expected_files_mean = day_stats['mean files'].iloc[0]
        actual_files_count = len(daily_files_df[daily_files_df['source_id'] == source_id])
        if actual_files_count < expected_files_mean:
            missing_count = int(expected_files_mean - actual_files_count)
            if missing_count >= 1:
                incidents.append({"source_id": source_id, "incident_type": "Missing File", "description": f"Faltan {missing_count} archivos. Se esperaban ~{expected_files_mean:.0f}, se recibieron {actual_files_count}.", "severity": "URGENT", "date": date_str})
        return incidents
    except Exception as e: print(f"Error en 'find_missing_files' para {source_id}: {e}"); return []

def find_duplicated_or_failed_files(daily_files_df: pd.DataFrame, historical_files_df: pd.DataFrame, source_id: str, date_str: str) -> List[Dict[str, Any]]:
    # ... (código sin cambios)
    incidents = []; reported_filenames = set()
    source_files_df = daily_files_df[daily_files_df['source_id'] == source_id]
    if source_files_df.empty: return incidents
    source_files_df = source_files_df.copy(); source_files_df.loc[:, 'status'] = source_files_df['status'].astype(str).fillna('unknown')
    duplicates_df = source_files_df[(source_files_df['is_duplicated'] == True) & (source_files_df['status'].str.lower() == 'stopped')]
    for _, row in duplicates_df.iterrows():
        filename = row['filename']
        if filename not in reported_filenames:
            incidents.append({"source_id": source_id, "incident_type": "Duplicated File (Flag)", "description": f"Archivo marcado como duplicado: '{filename}'.", "severity": "URGENT", "date": date_str}); reported_filenames.add(filename)
    filename_counts = source_files_df['filename'].value_counts(); intraday_dup_names = filename_counts[filename_counts > 1].index.tolist()
    if intraday_dup_names:
        intraday_duplicates_df = source_files_df[source_files_df['filename'].isin(intraday_dup_names)]
        for _, row in intraday_duplicates_df.iterrows():
            filename = row['filename']
            if filename not in reported_filenames:
                incidents.append({"source_id": source_id, "incident_type": "Intraday Duplicate", "description": f"Archivo subido múltiples veces hoy: '{filename}'.", "severity": "REQUIERE ATENCIÓN", "date": date_str}); reported_filenames.add(filename)
    if not historical_files_df.empty:
        historical_filenames = set(historical_files_df[historical_files_df['source_id'] == source_id]['filename'])
        historical_duplicates_df = source_files_df[source_files_df['filename'].isin(historical_filenames)]
        for _, row in historical_duplicates_df.iterrows():
            filename = row['filename']
            if filename not in reported_filenames:
                incidents.append({"source_id": source_id, "incident_type": "Historical Duplicate", "description": f"Archivo duplicado de la semana anterior: '{filename}'.", "severity": "REQUIERE ATENCIÓN", "date": date_str}); reported_filenames.add(filename)
    failed_files_df = source_files_df[(source_files_df['status'].str.lower() != 'processed') & (~source_files_df['filename'].isin(reported_filenames))]
    for _, row in failed_files_df.iterrows():
        incidents.append({"source_id": source_id, "incident_type": "Failed File", "description": f"Archivo con procesamiento fallido: '{row['filename']}' (Estado: {row['status']}).", "severity": "REQUIERE ATENCIÓN", "date": date_str})
    return incidents

def find_unexpected_empty_files(daily_files_df: pd.DataFrame, cv_patterns: Dict[str, Any], source_id: str, date_str: str) -> List[Dict[str, Any]]:
    # ... (código sin cambios)
    incidents = []
    source_files_df = daily_files_df[daily_files_df['source_id'] == source_id]
    empty_files_today = source_files_df[source_files_df['rows'] == 0]
    if empty_files_today.empty: return incidents
    day_of_week = pd.to_datetime(date_str).day_name()
    day_summary_table = cv_patterns.get("day_of_week_summary")
    are_empty_files_expected = False
    if day_summary_table is not None and not day_summary_table.empty:
        day_stats = day_summary_table[day_summary_table['Day'].str.lower() == day_of_week.lower()]
        if not day_stats.empty:
            empty_files_col_name = next((col for col in day_stats.columns if 'Empty Files' in col), None)
            if empty_files_col_name:
                stats_text = str(day_stats[empty_files_col_name].iloc[0])
                max_empty_match = re.search(r'Max:\s*([\d\.]+)', stats_text)
                if max_empty_match and float(max_empty_match.group(1)) > 0: are_empty_files_expected = True
    if not are_empty_files_expected:
        for _, row in empty_files_today.iterrows():
            incidents.append({"source_id": source_id, "incident_type": "Unexpected Empty File", "description": f"Archivo vacío inesperado: '{row['filename']}'.", "severity": "REQUIERE ATENCIÓN", "date": date_str})
    return incidents

def find_volume_variations(daily_files_df: pd.DataFrame, cv_patterns: Dict[str, Any], source_id: str, date_str: str) -> List[Dict[str, Any]]:
    # ... (código sin cambios)
    incidents = []
    day_of_week = pd.to_datetime(date_str).day_name()
    day_summary_table = cv_patterns.get("day_of_week_summary")
    if day_summary_table is None or day_summary_table.empty: return incidents
    day_stats = day_summary_table[day_summary_table['Day'].str.lower() == day_of_week.lower()]
    if day_stats.empty: return incidents
    total_rows_today = daily_files_df[daily_files_df['source_id'] == source_id]['rows'].sum()
    try:
        stats_col_name = next((col for col in day_stats.columns if 'Rows' in col), None)
        if not stats_col_name: return incidents
        stats_text = str(day_stats[stats_col_name].iloc[0])
        min_rows_match = re.search(r'Min:\s*([\d,]+)', stats_text); max_rows_match = re.search(r'Max:\s*([\d,]+)', stats_text)
        if min_rows_match and max_rows_match:
            expected_min = int(min_rows_match.group(1).replace(',', '')); expected_max = int(max_rows_match.group(1).replace(',', ''))
            if total_rows_today > expected_max or total_rows_today < expected_min:
                incidents.append({"source_id": source_id, "incident_type": "Unexpected Volume Variation", "description": f"Variación de volumen: Se recibieron {total_rows_today:,} filas, fuera del rango esperado ({expected_min:,} - {expected_max:,}).", "severity": "REQUIERE ATENCIÓN", "date": date_str})
    except Exception as e: print(f"--- ↳ ⚠️  ADVERTENCIA: No se pudo procesar la variación de volumen para {source_id}: {e} ---")
    return incidents

# --- NUEVO: Detector de Archivos Fuera de Horario ---
def find_late_uploads(daily_files_df: pd.DataFrame, cv_patterns: Dict[str, Any], source_id: str, date_str: str) -> List[Dict[str, Any]]:
    """
    (Lógica Pura) Detecta archivos subidos más de 4 horas después de la ventana esperada.
    """
    incidents = []
    day_of_week = pd.to_datetime(date_str).day_name()
    schedule_table = cv_patterns.get("upload_schedule_patterns")
    if schedule_table is None or schedule_table.empty: return incidents

    day_schedule = schedule_table[schedule_table['Day'].str.lower() == day_of_week.lower()]
    if day_schedule.empty or 'Upload Time Window Expected' not in day_schedule.columns: return incidents

    window_str = day_schedule['Upload Time Window Expected'].iloc[0]
    # Extraemos la hora final de la ventana, ej: "08:00:00–09:00:00 UTC" -> "09:00:00"
    match = re.search(r'–(\d{2}:\d{2}:\d{2})', window_str)
    if not match: return incidents
    
    end_time_str = match.group(1)
    # Creamos un timestamp del límite superior esperado para hoy
    deadline = pd.to_datetime(f"{date_str} {end_time_str}", utc=True)
    # Añadimos el margen de 4 horas que dice el caso de negocio
    deadline_with_margin = deadline + timedelta(hours=4)

    source_files_today = daily_files_df[daily_files_df['source_id'] == source_id]
    for _, row in source_files_today.iterrows():
        if row['uploaded_at'] > deadline_with_margin:
            incidents.append({
                "source_id": source_id, "incident_type": "File Upload After Schedule",
                "description": f"Archivo '{row['filename']}' subido a las {row['uploaded_at'].strftime('%H:%M')} UTC, más de 4h después del límite esperado de las {deadline.strftime('%H:%M')} UTC.",
                "severity": "ADVERTENCIA", "date": date_str
            })
    return incidents

# --- NUEVO: Detector de Archivos de Períodos Anteriores ---
def _extract_date_from_filename(filename: str):
    """Función de ayuda para encontrar una fecha en formato YYYYMMDD o YYYY-MM-DD."""
    match = re.search(r'(\d{4}[-]?\d{2}[-]?\d{2})', filename)
    if match:
        return pd.to_datetime(match.group(1).replace('-', '')).date()
    return None

def find_previous_period_uploads(daily_files_df: pd.DataFrame, source_id: str, date_str: str) -> List[Dict[str, Any]]:
    """
    (Lógica Pura) Detecta archivos cuya fecha en el nombre es muy anterior a la fecha de subida.
    """
    incidents = []
    upload_date = pd.to_datetime(date_str).date()
    source_files_today = daily_files_df[daily_files_df['source_id'] == source_id]

    for _, row in source_files_today.iterrows():
        filename_date = _extract_date_from_filename(row['filename'])
        if filename_date:
            # ECD: Expected Coverage Data. Asumimos que un archivo no debería ser de más de 7 días antes.
            if (upload_date - filename_date).days > 7:
                 incidents.append({
                    "source_id": source_id, "incident_type": "Previous Period Upload",
                    "description": f"Archivo '{row['filename']}' parece ser de un período anterior (Fecha en nombre: {filename_date}, Fecha de subida: {upload_date}).",
                    "severity": "ADVERTENCIA", "date": date_str
                })
    return incidents