"""Job de autorrespuesta con OpenAI y lógica de negocio."""

import logging
import time
import random
from typing import Dict, Any, Optional, List
from src.queue_config import app
from src.state_manager import get_state_manager
from celery import Task

logger = logging.getLogger(__name__)


@app.task(
    name='src.jobs.auto_reply_job.generate_reply',
    queue='replies',
    priority=8,
    time_limit=60,
)
def generate_reply(
    username: str,
    thread_id: str,
    message_text: str,
    conversation_history: List[Dict],
    system_prompt: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Genera respuesta automática usando OpenAI.
    
    Args:
        username: Usuario que responde
        thread_id: ID del thread
        message_text: Mensaje recibido
        conversation_history: Historial de mensajes
        system_prompt: Prompt personalizado (opcional)
    
    Returns:
        {
            'reply_text': str,
            'should_send': bool,
            'confidence': float,
            'metadata': dict,
        }
    """
    try:
        logger.info(f"[REPLY JOB] Generating reply for @{username} in thread {thread_id}")
        
        # Importar OpenAI
        try:
            from openai import OpenAI
            import os
            api_key = os.getenv('OPENAI_API_KEY')
            if not api_key:
                raise Exception("OPENAI_API_KEY not configured")
            client = OpenAI(api_key=api_key)
        except Exception as e:
            logger.error(f"OpenAI not available: {e}")
            return {
                'reply_text': '',
                'should_send': False,
                'error': 'OpenAI not configured',
            }
        
        # Construir contexto de conversación
        messages = [
            {
                'role': 'system',
                'content': system_prompt or 'Sos un asistente cordial que responde mensajes de Instagram de forma breve y humana.',
            }
        ]
        
        # Agregar historial
        for msg in conversation_history[-10:]:  # Últimos 10 mensajes
            role = 'assistant' if msg.get('is_outbound') else 'user'
            messages.append({
                'role': role,
                'content': msg.get('text', ''),
            })
        
        # Mensaje actual
        messages.append({
            'role': 'user',
            'content': message_text,
        })
        
        # Generar respuesta
        response = client.chat.completions.create(
            model='gpt-4o-mini',
            messages=messages,
            temperature=0.7,
            max_tokens=150,
        )
        
        reply_text = response.choices[0].message.content.strip()
        
        logger.info(f"[REPLY JOB] Generated reply ({len(reply_text)} chars)")
        
        return {
            'reply_text': reply_text,
            'should_send': True,
            'confidence': 0.9,
            'model': 'gpt-4o-mini',
            'tokens_used': response.usage.total_tokens,
        }
        
    except Exception as exc:
        logger.error(f"[REPLY JOB] Error generating reply: {exc}")
        return {
            'reply_text': '',
            'should_send': False,
            'error': str(exc),
        }


@app.task(
    name='src.jobs.auto_reply_job.process_and_reply',
    queue='replies',
    priority=9,
)
def process_and_reply(
    username: str,
    password: str,
    proxy: Optional[Dict],
    thread_id: str,
    new_messages: List[Dict],
) -> Dict[str, Any]:
    """
    Procesa mensajes nuevos y envía respuesta automática.
    
    Workflow:
    1. Obtener historial de conversación
    2. Generar respuesta con OpenAI
    3. Aplicar delays humanos
    4. Enviar respuesta
    5. Marcar como leído
    """
    state_mgr = get_state_manager()
    
    try:
        logger.info(f"[AUTO REPLY] Processing {len(new_messages)} messages for @{username}")
        
        # Verificar si está rate limited
        if state_mgr.is_rate_limited(username):
            logger.warning(f"Skipping auto-reply for @{username} - rate limited")
            return {'skipped': True, 'reason': 'rate_limited'}
        
        # Obtener último mensaje
        last_message = new_messages[-1] if new_messages else None
        if not last_message:
            return {'skipped': True, 'reason': 'no_messages'}
        
        message_text = last_message.get('text', '')
        if not message_text:
            return {'skipped': True, 'reason': 'empty_message'}
        
        # Construir historial (simplificado por ahora)
        conversation_history = [
            {
                'text': msg.get('text', ''),
                'is_outbound': False,  # Asumimos que son entrantes
                'timestamp': msg.get('timestamp'),
            }
            for msg in new_messages
        ]
        
        # Generar respuesta
        reply_result = generate_reply(
            username=username,
            thread_id=thread_id,
            message_text=message_text,
            conversation_history=conversation_history,
        )
        
        if not reply_result.get('should_send'):
            logger.info(f"[AUTO REPLY] Not sending reply: {reply_result.get('error', 'unknown')}")
            return reply_result
        
        reply_text = reply_result.get('reply_text', '')
        if not reply_text:
            return {'skipped': True, 'reason': 'empty_reply'}
        
        # Delay humano antes de responder (simula que estás escribiendo)
        # Basado en longitud del mensaje: ~50 chars/segundo
        typing_delay = len(reply_text) / 50.0
        typing_delay = max(3, min(typing_delay, 15))  # Entre 3 y 15 segundos
        
        # Agregar jitter
        typing_delay += random.uniform(1, 5)
        
        logger.info(f"[AUTO REPLY] Waiting {typing_delay:.1f}s (simulating typing)...")
        time.sleep(typing_delay)
        
        # Enviar respuesta usando el job de envío
        from src.jobs.send_message_job import send_dm
        
        # Obtener username del destinatario desde el thread
        # (simplificado - en producción obtenerlo del thread real)
        target_user = last_message.get('from_user', '')
        
        send_result = send_dm.apply_async(
            kwargs={
                'username': username,
                'password': password,
                'proxy': proxy,
                'target_user': target_user,
                'message_text': reply_text,
                'human_delay': False,  # Ya aplicamos delay arriba
            }
        )
        
        logger.info(f"[AUTO REPLY] Reply queued: task_id={send_result.id}")
        
        return {
            'success': True,
            'reply_text': reply_text,
            'send_task_id': send_result.id,
            'typing_delay': typing_delay,
        }
        
    except Exception as exc:
        logger.error(f"[AUTO REPLY] Error: {exc}")
        return {
            'success': False,
            'error': str(exc),
        }


@app.task(
    name='src.jobs.auto_reply_job.check_and_reply_all',
    queue='replies',
    priority=7,
)
def check_and_reply_all() -> Dict[str, Any]:
    """
    Verifica mensajes nuevos y genera respuestas automáticas.
    Ejecutado periódicamente o triggered por polling.
    """
    from core.accounts import list_all
    
    logger.info("[AUTO REPLY ALL] Starting auto-reply check")
    
    # Obtener cuentas con auto-reply habilitado
    all_accounts = list_all()
    auto_reply_accounts = [
        acc for acc in all_accounts
        if acc.get('auto_reply_enabled', False)
    ]
    
    logger.info(f"[AUTO REPLY ALL] {len(auto_reply_accounts)} accounts with auto-reply enabled")
    
    results = []
    for account in auto_reply_accounts:
        username = account.get('username')
        
        # Obtener mensajes nuevos desde Redis/estado
        state_mgr = get_state_manager()
        state = state_mgr.load_account_state(username)
        
        if not state or state.get('unread_count', 0) == 0:
            continue
        
        # Encolar procesamiento de respuestas
        # (En producción, obtener threads y mensajes reales)
        logger.info(f"[AUTO REPLY ALL] Queueing auto-reply for @{username}")
        
        # Por ahora, solo logging
        results.append({
            'username': username,
            'status': 'queued',
        })
    
    return {
        'total_checked': len(auto_reply_accounts),
        'replies_queued': len(results),
        'results': results,
    }
