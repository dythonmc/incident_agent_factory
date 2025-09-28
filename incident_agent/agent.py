# incident_agent/agent.py

from google.adk.agents import Agent
from .tools import orchestrator_tools
from google.adk.agents.callback_context import CallbackContext
from google.adk.models.llm_response import LlmResponse
from typing import Optional

# --- CONTADOR DE TOKENS (Singleton) ---
class TokenCounter:
    def __init__(self):
        self.total_prompt_tokens = 0
        self.total_candidates_tokens = 0

    def add(self, prompt_tokens: int, candidates_tokens: int):
        self.total_prompt_tokens += prompt_tokens
        self.total_candidates_tokens += candidates_tokens
        print(f"--- 🪙 Contador de Tokens: Entrada={prompt_tokens}, Salida={candidates_tokens}. Total acumulado: {self.total_prompt_tokens + self.total_candidates_tokens} ---")

token_counter = TokenCounter()

# --- CALLBACK PARA LOGUEAR EL USO DE TOKENS (CORREGIDO) ---
def log_token_usage(callback_context: CallbackContext, llm_response: LlmResponse) -> Optional[LlmResponse]:
    """
    Este callback se ejecuta después de cada llamada al LLM y actualiza nuestro contador global.
    Los nombres de los parámetros DEBEN ser 'callback_context' y 'llm_response'.
    """
    if llm_response and llm_response.usage_metadata:
        prompt_tokens = llm_response.usage_metadata.prompt_token_count or 0
        candidates_tokens = llm_response.usage_metadata.candidates_token_count or 0
        token_counter.add(prompt_tokens, candidates_tokens)
    return llm_response


# --- DEFINICIÓN DEL EQUIPO DE SUBAGENTES ESPECIALISTAS ---
RecolectorAgent = Agent(
    name="RecolectorAgent", model="gemini-2.5-flash",
    instruction="Tu única tarea es recolectar información. Usa la herramienta `recolectar_informacion_tool`.",
    tools=[orchestrator_tools.recolectar_informacion_tool],
    after_model_callback=log_token_usage
)

DetectorAgent = Agent(
    name="DetectorAgent", model="gemini-2.5-flash",
    instruction="Tu única tarea es ejecutar el ciclo de detección de incidencias usando la herramienta `ejecutar_ciclo_deteccion_tool`.",
    tools=[orchestrator_tools.ejecutar_ciclo_deteccion_tool],
    after_model_callback=log_token_usage
)

RedactorAgent = Agent(
    name="RedactorAgent", model="gemini-2.5-flash",
    instruction="Tu única tarea es generar el reporte final estructurado usando la herramienta `generar_reporte_final_tool`.",
    tools=[orchestrator_tools.generar_reporte_final_tool],
    after_model_callback=log_token_usage
)

# --- DEFINICIÓN DEL AGENTE RAÍZ ---
SequentialAgent = Agent(
    name="SequentialAgent", model="gemini-2.5-flash",
    instruction=(
        "Eres el Director de un proyecto de análisis. Ejecuta a tus especialistas en la siguiente secuencia estricta, sin detenerte: "
        "1. Delega al `RecolectorAgent`. "
        "2. Delega al `DetectorAgent`. "
        "3. Delega al `RedactorAgent`. "
        "Tu trabajo solo termina cuando el `RedactorAgent` ha completado su tarea. "
        "Tu respuesta final debe ser el mensaje de éxito que te entregue el `RedactorAgent`."
    ),
    sub_agents=[
        RecolectorAgent,
        DetectorAgent,
        RedactorAgent,
    ],
    after_model_callback=log_token_usage
)

root_agent = SequentialAgent