# incident_agent/tools/detectors.py

import pandas as pd
import re
from typing import List, Dict, Any

def find_missing_files(daily_files_df: pd.DataFrame, cv_patterns: Dict[str, Any], source_id: str, date_str: str) -> List[Dict[str, Any]]:
    incidents = []
    try:
        day_of_week = pd.to_datetime(date_str).day_name()
        processing_stats = cv_patterns.get("file_processing_stats")
        if processing_stats is None or processing_stats.empty:
            return incidents
        
        processing_stats.columns = [str(col).lower() for col in processing_stats.columns]
        day_stats = processing_stats[processing_stats['day'].str.lower() == day_of_week.lower()]
        if day_stats.empty: return incidents
        
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

def find_duplicated_or_failed_files(daily_files_df: pd.DataFrame, historical_files_df: pd.DataFrame, source_id: str, date_str: str) -> List[Dict[str, Any]]:
    incidents = []
    reported_filenames = set()
    source_files_df = daily_files_df[daily_files_df['source_id'] == source_id]
    if source_files_df.empty: return incidents
    source_files_df = source_files_df.copy()
    source_files_df.loc[:, 'status'] = source_files_df['status'].astype(str).fillna('unknown')

    duplicates_df = source_files_df[(source_files_df['is_duplicated'] == True) & (source_files_df['status'].str.lower() == 'stopped')]
    for _, row in duplicates_df.iterrows():
        filename = row['filename']
        if filename not in reported_filenames:
            incidents.append({"source_id": source_id, "incident_type": "Duplicated File (Flag)", "description": f"Archivo marcado como duplicado: '{filename}'.", "severity": "URGENT", "date": date_str})
            reported_filenames.add(filename)

    filename_counts = source_files_df['filename'].value_counts()
    intraday_dup_names = filename_counts[filename_counts > 1].index.tolist()
    if intraday_dup_names:
        intraday_duplicates_df = source_files_df[source_files_df['filename'].isin(intraday_dup_names)]
        for _, row in intraday_duplicates_df.iterrows():
            filename = row['filename']
            if filename not in reported_filenames:
                incidents.append({"source_id": source_id, "incident_type": "Intraday Duplicate", "description": f"Archivo subido múltiples veces hoy: '{filename}'.", "severity": "REQUIERE ATENCIÓN", "date": date_str})
                reported_filenames.add(filename)
    
    if not historical_files_df.empty:
        historical_filenames = set(historical_files_df[historical_files_df['source_id'] == source_id]['filename'])
        historical_duplicates_df = source_files_df[source_files_df['filename'].isin(historical_filenames)]
        for _, row in historical_duplicates_df.iterrows():
            filename = row['filename']
            if filename not in reported_filenames:
                incidents.append({"source_id": source_id, "incident_type": "Historical Duplicate", "description": f"Archivo duplicado de la semana anterior: '{filename}'.", "severity": "REQUIERE ATENCIÓN", "date": date_str})
                reported_filenames.add(filename)

    failed_files_df = source_files_df[(source_files_df['status'].str.lower() != 'processed') & (~source_files_df['filename'].isin(reported_filenames))]
    for _, row in failed_files_df.iterrows():
        incidents.append({"source_id": source_id, "incident_type": "Failed File", "description": f"Archivo con procesamiento fallido: '{row['filename']}' (Estado: {row['status']}).", "severity": "REQUIERE ATENCIÓN", "date": date_str})

    return incidents

def find_unexpected_empty_files(
    daily_files_df: pd.DataFrame,
    cv_patterns: Dict[str, Any],
    source_id: str,
    date_str: str
) -> List[Dict[str, Any]]:
    """
    (Lógica Pura) Detecta archivos con cero filas que no son esperados según el CV.
    """
    incidents = []
    
    # 1. Encontrar todos los archivos vacíos de hoy para la fuente actual
    source_files_df = daily_files_df[daily_files_df['source_id'] == source_id]
    empty_files_today = source_files_df[source_files_df['rows'] == 0]
    
    if empty_files_today.empty:
        return incidents # Si no hay archivos vacíos hoy, no hay nada que hacer

    # 2. Consultar el CV para ver si los archivos vacíos son normales en este día
    day_of_week = pd.to_datetime(date_str).day_name()
    day_summary_table = cv_patterns.get("day_of_week_summary")
    
    are_empty_files_expected = False
    if day_summary_table is not None and not day_summary_table.empty:
        day_stats = day_summary_table[day_summary_table['Day'].str.lower() == day_of_week.lower()]
        if not day_stats.empty:
            # Buscamos en las columnas 'Empty Files' o 'Empty Files Analysis'
            empty_files_col_name = next((col for col in day_stats.columns if 'Empty Files' in col), None)
            
            if empty_files_col_name:
                # Extraemos el valor de la celda, que puede ser un string como "• Min: 0<br>• Max: 1<br>..."
                stats_text = str(day_stats[empty_files_col_name].iloc[0])
                # Si el máximo de archivos vacíos esperado es mayor que 0, entonces son esperados.
                max_empty_match = re.search(r'Max:\s*([\d\.]+)', stats_text)
                if max_empty_match and float(max_empty_match.group(1)) > 0:
                    are_empty_files_expected = True

    # 3. Si encontramos archivos vacíos y el CV NO los esperaba, creamos la incidencia
    if not are_empty_files_expected:
        for _, row in empty_files_today.iterrows():
            incidents.append({
                "source_id": source_id,
                "incident_type": "Unexpected Empty File",
                "description": f"Archivo vacío inesperado: '{row['filename']}'. El CV indica que no deberían llegar archivos sin registros los {day_of_week}.",
                "severity": "REQUIERE ATENCIÓN",
                "date": date_str,
                "details": {"filename": row['filename']}
            })
            
    return incidents


def find_volume_variations(
    daily_files_df: pd.DataFrame,
    cv_patterns: Dict[str, Any],
    source_id: str,
    date_str: str
) -> List[Dict[str, Any]]:
    """
    (Lógica Pura) Detecta variaciones de volumen anómalas comparando el total de filas
    del día con los patrones estadísticos del CV para ese día de la semana.
    """
    incidents = []
    
    # 1. Obtenemos el día de la semana para buscar el patrón correcto
    day_of_week = pd.to_datetime(date_str).day_name()
    
    # 2. Extraemos la tabla de resumen del CV
    day_summary_table = cv_patterns.get("day_of_week_summary")
    
    if day_summary_table is None or day_summary_table.empty:
        # Si no hay patrones de volumen en el CV, no podemos detectar nada
        return incidents

    # 3. Buscamos la fila correspondiente al día de la semana actual
    day_stats = day_summary_table[day_summary_table['Day'].str.lower() == day_of_week.lower()]
    if day_stats.empty:
        return incidents

    # 4. Calculamos el volumen total de filas recibidas hoy para esta fuente
    source_files_today_df = daily_files_df[daily_files_df['source_id'] == source_id]
    total_rows_today = source_files_today_df['rows'].sum()

    try:
        # Extraemos las estadísticas de la celda 'Row Statistics' o 'Total Rows Processed'
        stats_col_name = next((col for col in day_stats.columns if 'Rows' in col), None)
        if not stats_col_name:
            return incidents
        
        stats_text = str(day_stats[stats_col_name].iloc[0])
        
        # Usamos regex para extraer los valores Min y Max esperados
        min_rows_match = re.search(r'Min:\s*([\d,]+)', stats_text)
        max_rows_match = re.search(r'Max:\s*([\d,]+)', stats_text)
        
        if min_rows_match and max_rows_match:
            # Convertimos los números a un formato usable (quitando comas)
            expected_min = int(min_rows_match.group(1).replace(',', ''))
            expected_max = int(max_rows_match.group(1).replace(',', ''))

            # 5. Comparamos el volumen de hoy con el rango esperado
            if total_rows_today > expected_max or total_rows_today < expected_min:
                description = (
                    f"Variación de volumen inesperada. Se recibieron {total_rows_today:,} filas, "
                    f"pero el rango esperado para un {day_of_week} es entre {expected_min:,} y {expected_max:,}."
                )
                incidents.append({
                    "source_id": source_id,
                    "incident_type": "Unexpected Volume Variation",
                    "description": description,
                    "severity": "REQUIERE ATENCIÓN",
                    "date": date_str,
                    "details": {
                        "received_rows": int(total_rows_today),
                        "expected_min": expected_min,
                        "expected_max": expected_max
                    }
                })
    except Exception as e:
        print(f"--- ↳ ⚠️  ADVERTENCIA: No se pudo procesar la variación de volumen para {source_id}: {e} ---")

    return incidents

def find_late_uploads(
    daily_files_df: pd.DataFrame,
    cv_patterns: Dict[str, Any],
    source_id: str,
    date_str: str
) -> List[Dict[str, Any]]:
    """
    (Lógica Pura - Esqueleto) Detecta archivos subidos significativamente fuera del horario esperado.
    """
    # Por ahora, esta función es un placeholder.
    # En el futuro, aquí irá la lógica para comparar la hora de subida con la ventana esperada del CV.
    incidents = []
    return incidents

def find_previous_period_uploads(
    daily_files_df: pd.DataFrame,
    cv_patterns: Dict[str, Any],
    source_id: str,
    date_str: str
) -> List[Dict[str, Any]]:
    """
    (Lógica Pura - Esqueleto) Detecta archivos cuya fecha en el nombre no corresponde al período de carga esperado.
    """
    # Por ahora, esta función es un placeholder.
    # En el futuro, aquí irá la lógica para extraer la fecha del filename y compararla con el 'Upload Lag Days Mode' del CV.
    incidents = []
    return incidents