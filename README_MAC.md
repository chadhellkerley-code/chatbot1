# Insta CLI (macOS Edition)

## Requisitos
- macOS con Python 3.11 o 3.10 instalado (`python3 --version`)
- Acceso a internet para instalar dependencias

## Instalación
```bash
cd <carpeta_del_proyecto>
./setup_mac.sh
```

## Ejecución
```bash
./run_mac.sh
```

## Dónde se guardan las cosas
- Sesiones de Instagram: `./.sessions/<username>.json`
- Listas de leads: `./text/leads/<lista>.txt` (una cuenta por línea)
- Logs de envíos: `./storage/sent_log.jsonl`
- Estado del autoresponder: `./storage/autoresponder_state.json`

## Autoresponder (OpenAI)
En el menú 5) Auto-responder:
- Pegá tu `OPENAI_API_KEY`
- Escribí el prompt del bot
- Configurá el delay entre chequeos
El bot responde sólo cuando el **último mensaje del hilo no es tuyo** y evita duplicados por hilo.

## Tips
- Si tu terminal no muestra emojis, ejecuta con `EMOJI` apagado:
  ```bash
  INSTACLI_EMOJI=0 ./run_mac.sh
  ```
- Para parar el envío masivo presioná `CTRL+C`. 
