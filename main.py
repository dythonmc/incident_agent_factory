# main.py

import asyncio
import os
import ast
import json
import logging # <-- Importamos logging
import argparse # <-- Importamos el manejador de argumentos
from dotenv import load_dotenv
from google.adk.runners import Runner
from google.adk.sessions import InMemorySessionService
from google.genai.types import Content, Part
from incident_agent.agent import root_agent

# --- Lógica para configurar el logging y los argumentos ---
parser = argparse.ArgumentParser()
parser.add_argument(
    "--json-only",
    action="store_true",
    help="Si se especifica, solo imprime el JSON final, sin logs de proceso."
)
args = parser.parse_args()

# Si no es json-only, configuramos un logging visible. Si lo es, los logs se ocultarán.
log_level = logging.CRITICAL if args.json_only else logging.INFO
logging.basicConfig(level=log_level, format='%(message)s')

# --- El resto del script ---
load_dotenv("incident_agent/.env")
if not os.getenv("GOOGLE_API_KEY"):
    logging.critical("ERROR: La variable GOOGLE_API_KEY no está en incident_agent/.env")
    exit()

async def main_workflow(date_to_analyze: str):
    logging.info(f"\n🚀 === INICIANDO ANÁLISIS DE INCIDENCIAS PARA LA FECHA: {date_to_analyze} === 🚀")
    
    session_service = InMemorySessionService()
    runner = Runner(agent=root_agent, app_name="incident_factory_app", session_service=session_service)
    session_id = f"daily_run_{date_to_analyze}"
    await session_service.create_session(app_name=runner.app_name, user_id="system_supervisor", session_id=session_id)
    initial_prompt = f"Ejecuta el análisis completo para la fecha '{date_to_analyze}'."
    
    logging.info(f"💬 Enviando orden al agente: '{initial_prompt}'")
    content = Content(role="user", parts=[Part(text=initial_prompt)])
    final_response_text = ""
    async for event in runner.run_async(user_id="system_supervisor", session_id=session_id, new_message=content):
        if event.is_final_response() and event.content and event.content.parts:
            if event.content.parts[0].function_response:
                 final_response_text = str(event.content.parts[0].function_response.response)
            else: # Si el agente responde con texto por algún error
                 final_response_text = event.content.parts[0].text
            break
            
    logging.info("\n✅ === WORKFLOW COMPLETADO === ✅")
    
    try:
        all_incidents = ast.literal_eval(final_response_text)
        
        # El print final es el único que no es un log, es el resultado.
        # Siempre se imprimirá en la salida estándar.
        print(json.dumps(all_incidents, indent=4))

    except Exception as e:
        logging.error("\nNo se pudo procesar la respuesta final del agente.")
        logging.error(f"Error de parseo: {e}")
        # Si hay un error, imprimimos la respuesta en bruto para depurar
        print(f'{{"error": "Failed to parse agent response", "raw_response": "{final_response_text}"}}')

if __name__ == "__main__":
    FECHA_DE_ANALISIS = "2025-09-08"
    if os.name == 'nt':
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    asyncio.run(main_workflow(FECHA_DE_ANALISIS))