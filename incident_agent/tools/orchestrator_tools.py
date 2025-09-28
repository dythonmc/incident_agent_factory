from google.adk.tools.tool_context import ToolContext
from . import data_loaders, detectors
from typing import List, Dict, Any
import logging # <-- Importamos logging

def run_full_analysis(date_str: str, tool_context: ToolContext) -> List[Dict[str, Any]]:
    logging.info(f"\n--- ⚙️ Herramienta Orquestadora Activada: Análisis para {date_str} ---")
    logging.info("--- fase 1: Recolectando y procesando todos los datos de entrada... ---")
    all_source_ids = data_loaders.get_all_source_ids()
    if not all_source_ids:
        return [{"incident_type": "Process Error", "description": "No se encontraron Hojas de Vida (CVs)."}]
    logging.info(f"--- Encontrada lista maestra de {len(all_source_ids)} fuentes a monitorear.")
    daily_files_df = data_loaders.process_files_json(f"data/{date_str}_20_00_UTC/files.json", date_str)
    historical_files_df = data_loaders.process_files_json(f"data/{date_str}_20_00_UTC/files_last_weekday.json", date_str)
    logging.info(f"--- Cargados {len(daily_files_df)} archivos de hoy y {len(historical_files_df)} archivos históricos.")
    all_incidents = []
    logging.info("\n--- fase 2: Iniciando ciclo de detección fuente por fuente... ---")
    for source_id in all_source_ids:
        logging.info(f"\n--- 🕵️ Analizando fuente: {source_id} ---")
        cv_patterns, _ = data_loaders.parse_cv_data_and_text(source_id)
        if not cv_patterns:
            logging.warning(f"--- ↳ ⚠️  ADVERTENCIA: No se pudo procesar el CV. Se omite esta fuente.")
            continue
        logging.info("--- ↳ Ejecutando: Detector de Archivos Faltantes...")
        missing_incidents = detectors.find_missing_files(daily_files_df, cv_patterns, source_id, date_str)
        if missing_incidents:
            logging.info(f"--- ↳ ❗ INCIDENCIA ENCONTRADA: {missing_incidents[0]['description']}")
            all_incidents.extend(missing_incidents)
        logging.info("--- ↳ Ejecutando: Detector de Duplicados y Fallidos...")
        duplicated_incidents = detectors.find_duplicated_or_failed_files(daily_files_df, historical_files_df, source_id, date_str)
        if duplicated_incidents:
            for incident in duplicated_incidents:
                 logging.info(f"--- ↳ ❗ INCIDENCIA ENCONTRADA: {incident['description']}")
            all_incidents.extend(duplicated_incidents)
    logging.info(f"\n--- fase 3: Ciclo de detección completado. ---")
    logging.info(f"--- ✅ Se consolidaron un total de {len(all_incidents)} incidencias. ---")
    return all_incidents