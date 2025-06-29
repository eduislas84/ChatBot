import logging
import pandas as pd
from telegram import Update
from telegram.ext import ApplicationBuilder, CommandHandler, MessageHandler, ContextTypes, filters
from groq import Groq
import io
import contextlib

TELEGRAM_TOKEN = ""
GROQ_API_KEY = ""

groq_client = Groq(api_key=GROQ_API_KEY)

user_dataframes = {}
ultimo_comando_usuario = {}

logging.basicConfig(level=logging.INFO)

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "👋 ¡Hola! Soy un bot que te ayuda a hablar con tus datos.\n\n"
        "📁 Envíame un archivo CSV y luego haz preguntas sobre él.\n"
        "Usa /ayuda para ver ejemplos de preguntas."
    )

async def ayuda(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    df = user_dataframes.get(user_id)

    if df is None:
        await update.message.reply_text(
            "ℹ Para darte ejemplos personalizados, primero debes subir un archivo CSV.\n\n"
            "Mientras tanto, aquí tienes algunos ejemplos generales:\n"
            "• ¿Cuál es el salario promedio?\n"
            "• ¿Cuántas personas viven en Ciudad de México?\n"
            "• ¿Quién es el más joven?"
        )
        return

    columnas = df.columns.tolist()
    ejemplos = []

    if len(columnas) >= 1:
        ejemplos.append(f"• ¿Cuál es el promedio de la columna '{columnas[0]}'?")
    if len(columnas) >= 2:
        ejemplos.append(f"• ¿Cuál es el valor máximo en '{columnas[1]}'?")
    if len(columnas) >= 3:
        valor = df[columnas[2]].dropna().unique()
        valor_ejemplo = valor[0] if len(valor) > 0 else "algún valor"
        ejemplos.append(f"• ¿Cuántas filas tienen '{columnas[2]}' igual a '{valor_ejemplo}'?")
    if len(columnas) >= 4:
        ejemplos.append(f"• ¿Quién tiene el mayor valor en '{columnas[3]}'?")

    ejemplos_texto = "\n".join(ejemplos)

    await update.message.reply_text(
        f"🧠 Aquí tienes ejemplos basados en tu archivo:\n{ejemplos_texto}\n\n"
        "Usa /info para ver más detalles del archivo cargado."
    )

async def info(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    df = user_dataframes.get(user_id)

    if df is None:
        await update.message.reply_text("📁 No has subido ningún archivo CSV aún.")
        return

    filas, columnas = df.shape
    columnas_lista = ", ".join(df.columns)
    await update.message.reply_text(
        f"📊 El archivo tiene {filas} filas y {columnas} columnas.\n"
        f"Columnas: {columnas_lista}"
    )

async def recibir_csv(update: Update, context: ContextTypes.DEFAULT_TYPE):
    archivo = update.message.document

    if not archivo.file_name.endswith('.csv'):
        await update.message.reply_text("❌ Solo acepto archivos .CSV.")
        return

    archivo_path = await archivo.get_file()
    archivo_local = f"{update.effective_user.id}_data.csv"
    await archivo_path.download_to_drive(archivo_local)

    try:
        df = pd.read_csv(archivo_local)
        if df.empty or df.columns.size == 0:
            raise ValueError("El CSV está vacío o mal formateado.")
        user_dataframes[update.effective_user.id] = df
        await update.message.reply_text("✅ Archivo CSV cargado correctamente. Ya puedes hacer preguntas.")
    except Exception as e:
        await update.message.reply_text(f"❌ Error al leer el CSV: {e}")

async def responder_pregunta(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    df = user_dataframes.get(user_id)

    if df is None:
        await update.message.reply_text("📁 Primero necesitas subir un archivo CSV.")
        return

    pregunta = update.message.text.strip()

    if len(pregunta) < 5:
        await update.message.reply_text("❗ La pregunta es muy corta. Intenta con algo más específico.")
        return

    contexto = df.iloc[:10, :5].to_string()

    prompt = f"""
Tengo este DataFrame en pandas:

{contexto}

Dime únicamente el código Python necesario para responder esta pregunta sobre el DataFrame:
{pregunta}

No des explicaciones. Solo quiero una línea de código concreta que se pueda ejecutar en Python.
"""

    try:
        respuesta_llm = groq_client.chat.completions.create(
            messages=[
                {"role": "system", "content": "Eres un asistente que responde con solo una línea de código Python para pandas."},
                {"role": "user", "content": prompt}
            ],
            model="llama3-8b-8192",
            max_tokens=60
        )

        respuesta = respuesta_llm.choices[0].message.content.strip().strip('`').strip()

        # Guardar el último comando del usuario
        ultimo_comando_usuario[user_id] = respuesta

        await update.message.reply_text(f"python\n{respuesta}\n", parse_mode="Markdown")

    except Exception as e:
        logging.exception("Error al usar Groq")
        await update.message.reply_text(f"❌ Error con el modelo GROQ:\n{e}")

async def ejecutar(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    df = user_dataframes.get(user_id)
    comando = ultimo_comando_usuario.get(user_id)

    if df is None:
        await update.message.reply_text("📁 Primero debes subir un archivo CSV.")
        return

    if not comando:
        await update.message.reply_text("❗ Aún no has hecho una pregunta para generar un comando.")
        return

    try:
        output = io.StringIO()
        with contextlib.redirect_stdout(output):
            resultado = eval(comando, {"df": df})
            print(resultado)

        await update.message.reply_text(f"📤 Resultado:\n{output.getvalue()}")
    except Exception as e:
        await update.message.reply_text(f"❌ Error al ejecutar el comando:\n{e}")

def main():
    app = ApplicationBuilder().token(TELEGRAM_TOKEN).build()

    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("ayuda", ayuda))
    app.add_handler(CommandHandler("info", info))
    app.add_handler(CommandHandler("ejecutar", ejecutar))
    app.add_handler(MessageHandler(filters.Document.ALL, recibir_csv))
    app.add_handler(MessageHandler(filters.TEXT & (~filters.COMMAND), responder_pregunta))

    print("✅ Bot iniciado. Presiona Ctrl+C para detener.")
    app.run_polling()

if __name__ == '__main__':
    main()
