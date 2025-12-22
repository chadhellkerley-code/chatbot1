INSTALACIÓN Y USO
=================

REQUISITOS PREVIOS:
-------------------
1. Python 3.10 o superior
2. Redis Server

PASO 1: Instalar Redis
-----------------------
Ubuntu/Debian:
  sudo apt install redis-server
  sudo systemctl start redis-server
  redis-cli ping  (debe responder: PONG)

PASO 2: Instalar dependencias
------------------------------
  python3 -m venv .venv
  source .venv/bin/activate
  pip install -r requirements.txt
  playwright install chromium

PASO 3: Iniciar el sistema
---------------------------
Terminal 1 (Worker):
  source .venv/bin/activate
  celery -A src.celery_app worker --loglevel=info

Terminal 2 (App):
  source .venv/bin/activate
  python app.py

OPCIONAL: Auto-respuestas con OpenAI
-------------------------------------
Crear archivo .env con:
  OPENAI_API_KEY=sk-tu-api-key

SOLUCIÓN DE PROBLEMAS:
----------------------
- Error Redis: sudo systemctl start redis-server
- Error módulos: source .venv/bin/activate
- Worker no funciona: verificar Terminal 1
