# Opt-in Browser Automation

El modo navegador es opcional y se activa solamente si `OPTIN_ENABLE=1`. Los pasos siguientes no modifican el flujo base de la aplicación.

## Instalación rápida

```bash
pip install -r requirements.txt
pip install -r requirements_optin.txt
python -m playwright install
```

## Variables de entorno recomendadas

Ejemplo de `.env` para habilitar el modo navegador:

```
OPTIN_ENABLE=1
OPTIN_HEADLESS=false
OPTIN_PROXY_URL=
SESSION_ENCRYPTION_KEY=   # (opcional) clave Fernet base64 para cifrar sesiones
OPTIN_KEYBOARD_DELAY_MIN=0.08
OPTIN_KEYBOARD_DELAY_MAX=0.22
OPTIN_ACTION_DELAY_MIN=0.3
OPTIN_ACTION_DELAY_MAX=1.1
OPTIN_NAVIGATION_TIMEOUT=25
OPTIN_WAIT_TIMEOUT=12
OPTIN_SEND_CODE_COOLDOWN=45
OPTIN_TOTP_cuenta1=JBSWY3DPEHPK3PXP  # secreto TOTP por alias (opcional)
```

- Las sesiones se guardan en `data/optin_sessions/<alias>.json` (cifradas si defines `SESSION_ENCRYPTION_KEY`).
- El registro de auditoría se escribe en `logs/optin_audit.jsonl`.

## Comandos CLI

| Acción | Comando |
| --- | --- |
| Login humanizado y guardar sesión | `python scripts/run_optin_login.py --account cuenta1 --user usuario` |
| Enviar DM usando sesión guardada | `python scripts/run_optin_send_dm.py --account cuenta1 --to destino --text "Hola"` |
| Responder no leídos | `python scripts/run_optin_reply_dm.py --account cuenta1 --reply "¡Gracias por escribir!"` |
| Grabar flujo manual | `python scripts/run_optin_record.py --alias onboarding` |
| Reproducir flujo grabado | `python scripts/run_optin_playback.py --alias onboarding --account cuenta1 --var EMAIL=example@mail.com` |

Si omites `--password` o los placeholders, el script solicitará los valores de forma segura en consola (no se registran en logs).

## Integración con el menú principal

Cuando `OPTIN_ENABLE=1` aparece la opción `10) Modo Automático (Opt-in navegador)` en el menú principal. Desde allí puedes:

1. Realizar login humanizado (con 2FA TOTP/SMS/WhatsApp).
2. Enviar DMs con delays parecidos a los humanos.
3. Responder chats no leídos.
4. Grabar un flujo manual una sola vez.
5. Reproducir el flujo grabado reutilizando sesiones guardadas.

El modo sigue siendo opt-in: si no instalas las dependencias ni habilitas la variable de entorno, la aplicación continúa funcionando exactamente igual que antes.
