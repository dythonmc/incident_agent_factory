# incident_agent/agent.py

from google.adk.agents import Agent
from .tools import orchestrator_tools

# --- Agente de Análisis (con instrucción ultra-estricta) ---
IncidentAnalysisAgent = Agent(
    name="IncidentAnalysisAgent",
    model="gemini-2.5-flash", 
    
    instruction=(
        "Eres un sistema automatizado. Tu única función es invocar la herramienta `run_full_analysis` "
        "y devolver su resultado. NO eres un asistente de chat. NO debes añadir texto introductorio, "
        "resúmenes o explicaciones. Tu respuesta DEBE ser EXCLUSIVAMENTE el resultado directo de la herramienta."
        "\n"
        "**FORMATO DE SALIDA OBLIGATORIO:**"
        "Tu única y exclusiva salida debe ser un string que represente una lista de diccionarios de Python, "
        "tal como lo devuelve la herramienta. Ejemplo: `[{'source_id': '123', ...}, {'source_id': '456', ...}]`."
        "Si la herramienta devuelve una lista vacía, tu salida debe ser exactamente `[]`."
        "NO ENVUELVAS LA RESPUESTA EN MARKDOWN, JSON o CUALQUIER OTRA COSA."
    ),
    
    tools=[
        orchestrator_tools.run_full_analysis,
    ]
)

root_agent = IncidentAnalysisAgent