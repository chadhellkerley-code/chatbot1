"""Job de login con reintentos y manejo de challenges."""

import logging
from typing import Dict, Any, Optional
from src.queue_config import app
from src.auth.onboarding import login_and_persist
from src.state_manager import get_state_manager
from celery import Task
from celery.exceptions import Retry

logger = logging.getLogger(__name__)


class LoginTask(Task):
    """Tarea de login con manejo de estado."""
    
    autoretry_for = (Exception,)
    retry_kwargs = {'max_retries': 3, 'countdown': 5}
    retry_backoff = True
    retry_backoff_max = 300  # 5 minutos max
    retry_jitter = True
    
    def on_failure(self, exc, task_id, args, kwargs, einfo):
        """Callback cuando falla el login."""
        account_payload = args[0] if args else kwargs.get('account_payload', {})
        username = account_payload.get('username', 'unknown')
        
        logger.error(f"Login failed permanently for @{username}: {exc}")
        
        # Marcar cuenta como problemática en Redis
        state_mgr = get_state_manager()
        state_mgr.save_account_state(username, {
            'status': 'login_failed',
            'last_error': str(exc),
            'failed_at': task_id,
        })


@app.task(
    bind=True,
    base=LoginTask,
    name='src.jobs.login_job.login_account',
    queue='login',
    priority=10,
    time_limit=180,  # 3 minutos max
    soft_time_limit=150,
)
def login_account(self, account_payload: Dict[str, Any]) -> Dict[str, Any]:
    """
    Ejecuta login y persiste sesión en Redis + archivos.
    
    Args:
        account_payload: {
            'username': str,
            'password': str,
            'totp_secret': str (opcional),
            'proxy': dict (opcional),
        }
    
    Returns:
        {
            'username': str,
            'status': 'ok' | 'need_code' | 'failed',
            'message': str,
            'profile_path': str,
        }
    """
    username = account_payload.get('username', '')
    state_mgr = get_state_manager()
    
    try:
        logger.info(f"[LOGIN JOB] Starting login for @{username}")
        
        # Verificar si ya hay sesión válida en Redis
        if state_mgr.session_exists(username):
            logger.info(f"Valid session found in Redis for @{username}, skipping login")
            return {
                'username': username,
                'status': 'ok',
                'message': 'Session reused from Redis',
                'profile_path': f'redis://{username}',
            }
        
        # Ejecutar login con Playwright
        result = login_and_persist(account_payload, headless=True)
        
        logger.info(f"[LOGIN JOB] Result for @{username}: {result.get('status')}")
        
        # Si login exitoso, guardar sesión en Redis
        if result.get('status') == 'ok':
            # Cargar storage_state desde archivo
            profile_path = result.get('profile_path', '')
            if profile_path:
                try:
                    import json
                    from pathlib import Path
                    storage_data = json.loads(Path(profile_path).read_text())
                    
                    # Guardar en Redis con TTL de 7 días
                    state_mgr.save_session(username, {
                        'storage_state': storage_data,
                        'profile_path': profile_path,
                        'login_method': 'playwright',
                    }, ttl=86400 * 7)
                    
                    # Actualizar estado de cuenta
                    state_mgr.save_account_state(username, {
                        'status': 'active',
                        'last_login': result.get('message', ''),
                    })
                except Exception as e:
                    logger.warning(f"Could not save session to Redis for @{username}: {e}")
        
        elif result.get('status') == 'need_code':
            # Challenge detectado - requiere intervención manual
            state_mgr.save_account_state(username, {
                'status': 'challenge',
                'message': result.get('message', ''),
            })
            logger.warning(f"Challenge required for @{username}")
        
        else:
            # Login falló
            state_mgr.save_account_state(username, {
                'status': 'login_failed',
                'message': result.get('message', ''),
            })
            
            # Reintentar si es un error temporal
            if 'timeout' in result.get('message', '').lower():
                raise Retry(exc=Exception(result.get('message')))
        
        return result
        
    except Exception as exc:
        logger.error(f"[LOGIN JOB] Error for @{username}: {exc}")
        
        # Guardar estado de error
        state_mgr.save_account_state(username, {
            'status': 'error',
            'last_error': str(exc),
        })
        
        # Re-lanzar para que Celery maneje el retry
        raise self.retry(exc=exc)


@app.task(
    name='src.jobs.login_job.refresh_session',
    queue='login',
    priority=8,
)
def refresh_session(username: str) -> Dict[str, Any]:
    """
    Refresca sesión existente sin hacer login completo.
    Útil para extender TTL de sesiones activas.
    """
    state_mgr = get_state_manager()
    
    session = state_mgr.load_session(username)
    if not session:
        logger.warning(f"No session to refresh for @{username}")
        return {'status': 'no_session', 'username': username}
    
    # Extender TTL
    state_mgr.save_session(username, session, ttl=86400 * 7)
    logger.info(f"Session refreshed for @{username}")
    
    return {'status': 'ok', 'username': username}
