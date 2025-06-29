import os
import logging
import gradio as gr
import pandas as pd
import numpy as np
from groq import Groq
from dotenv import load_dotenv

# Configuración inicial
load_dotenv()
logging.basicConfig(filename='app.log', level=logging.INFO)

# Cliente Groq seguro
try:
    client = Groq(api_key=os.getenv("GROQ_API_KEY"))
except Exception as e:
    logging.error(f"Error inicializando cliente Groq: {str(e)}")
    raise

def sanitizar_dataframe(df, max_rows=3):
    df_sanitizado = df.copy().head(max_rows)
    for col in df_sanitizado.columns:
        if any(s in col.lower() for s in ['password', 'email', 'credit', 'card']):
            df_sanitizado[col] = '*****'
    return df_sanitizado.to_string(index=False)

def validar_csv(archivo):
    if archivo.size > 10 * 1024 * 1024:
        raise ValueError("Archivo demasiado grande. Máximo 10MB permitidos.")
    
    df = pd.read_csv(archivo.name)
    
    if len(df.columns) < 1 or len(df) < 1:
        raise ValueError("Archivo CSV no válido: debe contener datos.")
        
    columnas_sensibles = ['password', 'contraseña', 'email', 'correo', 'tarjeta']
    for col in df.columns:
        if col.lower() in columnas_sensibles:
            raise ValueError(f"El archivo contiene columnas potencialmente sensibles ({col}).")
    
    return df

def cargar_csv(archivo):
    try:
        df = validar_csv(archivo)
        logging.info(f"CSV cargado: {len(df)} filas, {len(df.columns)} columnas")
        return df, f"✅ Archivo cargado. {len(df)} filas, {len(df.columns)} columnas."
    except Exception as e:
        logging.error(f"Error cargando CSV: {str(e)}")
        return None, f"❌ Error: {str(e)}"

def responder_pregunta_stream(pregunta, df):
    if df is None:
        yield "⚠️ Primero debes subir un archivo CSV válido."
        return

    try:
        resumen_numerico = df.select_dtypes(include=[np.number]).describe().round(2)
        contexto = (
            "Eres un asistente experto en análisis de datos. Responde en español, de forma precisa y concisa.\n\n"
            f"Fragmento del CSV:\n{sanitizar_dataframe(df)}\n\n"
            f"Columnas: {', '.join(df.columns)}\n\n"
            f"Resumen numérico:\n{resumen_numerico.to_string()}\n\n"
            f"Pregunta: {pregunta}"
        )

        completion = client.chat.completions.create(
            model="meta-llama/llama-4-scout-17b-16e-instruct",
            messages=[
                {"role": "system", "content": "Responde con precisión y brevedad en español."},
                {"role": "user", "content": contexto}
            ],
            temperature=0.3,
            top_p=0.8,
            max_completion_tokens=300,
            stream=True,
        )

        respuesta = ""
        for chunk in completion:
            token = chunk.choices[0].delta.content or ""
            respuesta += token
            yield respuesta

    except Exception as e:
        logging.error(f"Error en Groq: {str(e)}", exc_info=True)
        yield "❌ Ocurrió un error procesando tu pregunta. Por favor intenta nuevamente."

# Interfaz
with gr.Blocks(css="...") as interfaz:
    df_state = gr.State(None)
    
    with gr.Row():
        gr.HTML("<h1>🤖 🇵🇪 Asistente Inteligente de CSV</h1>")

    with gr.Column():
        archivo_csv = gr.File(label="📎 Sube tu archivo CSV", file_types=[".csv"])
        boton_cargar = gr.Button("📥 Cargar archivo")
        salida_carga = gr.Textbox(label="", interactive=False)

    with gr.Column():
        entrada_pregunta = gr.Textbox(label="💬 Pregunta", placeholder="Ejemplo: ¿Cuántas filas hay?", lines=2)
        boton_preguntar = gr.Button("🤖 Preguntar")
        salida_respuesta = gr.Textbox(label="🧠 Respuesta del asistente", interactive=False, lines=10)

    boton_cargar.click(fn=cargar_csv, inputs=[archivo_csv], outputs=[df_state, salida_carga])
    boton_preguntar.click(fn=responder_pregunta_stream, inputs=[entrada_pregunta, df_state], outputs=[salida_respuesta])

interfaz.launch()
