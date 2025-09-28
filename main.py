# main.py

import asyncio
import os
from dotenv import load_dotenv
from google.adk.runners import Runner
from google.adk.sessions import InMemorySessionService
from google.genai.types import Content, Part
from incident_agent.agent import root_agent

load_dotenv("incident_agent/.env")
if not os.getenv("GOOGLE_API_KEY"):
    print("ERROR: La variable GOOGLE_API_KEY no está en incident_agent/.env")
    exit()

async def main_workflow(date_to_analyze: str):
    print(f"\n🚀 === INICIANDO FÁBRICA DE AGENTES PARA LA FECHA: {date_to_analyze} === 🚀")
    
    session_service = InMemorySessionService()
    runner = Runner(agent=root_agent, app_name="incident_factory_app", session_service=session_service)
    session_id = f"daily_run_{date_to_analyze}"
    await session_service.create_session(
        app_name=runner.app_name, user_id="system_supervisor", session_id=session_id, state={'date_str': date_to_analyze}
    )
    
    initial_prompt = f"Inicia el proceso completo de análisis y reporte de incidencias para la fecha '{date_to_analyze}'."
    
    print(f"💬 Enviando orden al Director de la Fábrica ('{root_agent.name}')...")
    content = Content(role="user", parts=[Part(text=initial_prompt)])
    
    async for event in runner.run_async(user_id="system_supervisor", session_id=session_id, new_message=content):
        if event.is_final_response():
            print(f"\n💬 El Director ha finalizado el trabajo con el mensaje: '{event.content.parts[0].text}'")
            break
            
    print("\n✅ === TRABAJO DEL DÍA COMPLETADO === ✅")
    print("\n--- REPORTE FINAL DE INCIDENCIAS POR WORKSPACE ---")
    
    final_session = await session_service.get_session(app_name=runner.app_name, user_id="system_supervisor", session_id=session_id)
    final_report = final_session.state.get('final_report', {})
    
    if isinstance(final_report, str) or not final_report:
        print("🟢 TODO BIEN - No se generó reporte de incidencias.")
    else:
        for workspace_id, incidents in final_report.items():
            print(f"\n🏢 **Workspace ID: {workspace_id}**")
            for incident in incidents:
                print(f"  - 🚨 [{incident.get('severity')}] [Fuente: {incident.get('source_id')}] {incident.get('description')}")

if __name__ == "__main__":
    FECHA_DE_ANALISIS = "2025-09-08"
    if os.name == 'nt':
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    asyncio.run(main_workflow(FECHA_DE_ANALISIS))