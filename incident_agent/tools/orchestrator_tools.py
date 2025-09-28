# incident_agent/tools/orchestrator_tools.py

from google.adk.tools.tool_context import ToolContext
from . import data_loaders, detectors
import re
from typing import Dict, Any

def recolectar_informacion_tool(date_str: str, tool_context: ToolContext) -> str:
    """
    Herramienta del RecolectorAgent. Carga y PARSEA todos los datos necesarios
    y los guarda en la memoria de la sesión (state).
    """
    print("--- 📣 Agente 1 (Recolector): Iniciando recolección de datos... ---")
    try:
        daily_files_path = f"data/{date_str}_20_00_UTC/files.json"
        daily_files_df = data_loaders.process_files_json(daily_files_path, date_str)
        tool_context.state['daily_files_df'] = daily_files_df

        all_source_ids = data_loaders.get_all_source_ids()
        tool_context.state['all_source_ids'] = all_source_ids

        cv_data_map = {}
        source_to_workspace_map = {}
        print("--- 📣 Agente 1: Iniciando parseo de todas las Hojas de Vida... ---")
        for source_id in all_source_ids:
            parsed_tables, cv_text = data_loaders.parse_cv_data_and_text(source_id)
            cv_data_map[source_id] = parsed_tables
            
            match = re.search(r'Workspace ID\s*:\s*(\d+)', cv_text)
            if match:
                source_to_workspace_map[source_id] = match.group(1)
            else:
                source_to_workspace_map[source_id] = "default_workspace"
        
        tool_context.state['cv_data_map'] = cv_data_map
        tool_context.state['source_to_workspace_map'] = source_to_workspace_map

        log_message = f"Agente 1 (Recolector) cargó y procesó datos para {len(all_source_ids)} recursos."
        print(f"--- ✅ {log_message} ---")
        tool_context.state['date_str'] = date_str
        return log_message
    except Exception as e:
        return f"Error durante la recolección de datos: {e}"

def ejecutar_ciclo_deteccion_tool(tool_context: ToolContext) -> str:
    """
    Herramienta del DetectorAgent. LEE los datos YA PROCESADOS de la memoria,
    itera y ejecuta los detectores.
    """
    print("\n--- 📣 Agente 2 (Detector): Iniciando ciclo de detección... ---")
    try:
        daily_files_df = tool_context.state.get('daily_files_df')
        all_source_ids = tool_context.state.get('all_source_ids')
        cv_data_map = tool_context.state.get('cv_data_map')
        date_str = tool_context.state.get('date_str')

        if daily_files_df is None or all_source_ids is None or cv_data_map is None:
            return "Error: Datos necesarios no encontrados en memoria."

        all_incidents = []
        for source_id in all_source_ids:
            cv_data = cv_data_map.get(source_id)
            if cv_data is None:
                print(f"--- ⚠️ Agente 2: Omitiendo fuente {source_id} (no se encontró CV en memoria). ---")
                continue
            
            print(f"--- 🕵️ Agente 2: Analizando fuente {source_id}... ---")
            
            missing_incidents = detectors.find_missing_files(daily_files_df, cv_data, source_id, date_str)
            if missing_incidents:
                 print(f"--- ❗ Agente 2: 'Missing Files' encontró {len(missing_incidents)} incidencia(s) para {source_id}. ---")
                 all_incidents.extend(missing_incidents)
            
            duplicated_failed_incidents = detectors.find_duplicated_or_failed_files(daily_files_df, source_id, date_str)
            if duplicated_failed_incidents:
                print(f"--- ❗ Agente 2: 'Duplicated/Failed' encontró {len(duplicated_failed_incidents)} incidencia(s) para {source_id}. ---")
                all_incidents.extend(duplicated_failed_incidents)
        
        tool_context.state['all_incidents'] = all_incidents
        log_message = f"Agente 2 (Detector) ha consolidado {len(all_incidents)} incidencias."
        print(f"--- ✅ {log_message} ---")
        return log_message
    except Exception as e:
        print(f"--- 💥 ERROR CRÍTICO en el ciclo de detección: {e} ---")
        return f"Error durante el ciclo de detección: {e}"

def generar_reporte_final_tool(tool_context: ToolContext) -> str:
    """
    Herramienta para el RedactorAgent. Toma la lista de incidentes de la memoria,
    la agrupa por workspace y la guarda de nuevo en memoria.
    """
    print("\n--- 📣 Agente 3 (Redactor): Iniciando generación de reporte... ---")
    try:
        all_incidents = tool_context.state.get('all_incidents', [])
        source_to_workspace_map = tool_context.state.get('source_to_workspace_map', {})
        if not all_incidents:
            report = {}
            tool_context.state['final_report'] = report
            return "Reporte generado: No se encontraron incidencias."
        report_by_workspace = {}
        for incident in all_incidents:
            source_id = incident.get('source_id')
            workspace_id = source_to_workspace_map.get(str(source_id), "default_workspace")
            if workspace_id not in report_by_workspace:
                report_by_workspace[workspace_id] = []
            report_by_workspace[workspace_id].append(incident)
        tool_context.state['final_report'] = report_by_workspace
        log_message = f"Agente 3 (Redactor) creó el reporte para {len(report_by_workspace)} workspace(s)."
        print(f"--- ✅ {log_message} ---")
        return log_message
    except Exception as e:
        return f"Error durante la generación del reporte: {e}"