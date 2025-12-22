"""
Sistema de colas con BullMQ/Redis para Instagram automation.

Arquitectura:
- Jobs separados: login, send_message, read_messages, auto_reply
- Reintentos controlados con backoff exponencial
- Manejo de fallos con dead letter queue
- Concurrencia por cuenta para evitar rate limits
- Persistencia de estados en Redis
"""

from celery import Celery
import os
from dotenv import load_dotenv

load_dotenv()

REDIS_URL = os.getenv('REDIS_URL', 'redis://localhost:6379/0')

# Configuración avanzada de Celery como BullMQ
app = Celery('instagram_automation',
             broker=REDIS_URL,
             backend=REDIS_URL,
             include=[
                 'src.jobs.login_job',
                 'src.jobs.send_message_job',
                 'src.jobs.read_messages_job',
                 'src.jobs.auto_reply_job',
             ])

app.conf.update(
    # Serialización
    task_serializer='json',
    accept_content=['json'],
    result_serializer='json',
    
    # Timezone
    timezone='America/Argentina/Cordoba',
    enable_utc=True,
    
    # Resultados
    result_expires=7200,  # 2 horas
    result_backend_transport_options={
        'master_name': 'mymaster',
        'visibility_timeout': 3600,
    },
    
    # Concurrencia y límites
    worker_concurrency=int(os.getenv('CELERY_CONCURRENCY', 4)),
    worker_prefetch_multiplier=1,  # Importante: evita que un worker acapare todas las tareas
    worker_max_tasks_per_child=50,  # Reinicia workers periódicamente para liberar memoria
    
    # Acknowledgment
    task_acks_late=True,  # Solo marca como completada cuando termina
    task_reject_on_worker_lost=True,  # Re-encola si el worker muere
    
    # Reintentos globales
    task_autoretry_for=(Exception,),
    task_retry_backoff=True,  # Backoff exponencial
    task_retry_backoff_max=600,  # Max 10 minutos entre reintentos
    task_retry_jitter=True,  # Añade jitter para evitar thundering herd
    
    # Rate limiting por cuenta (evita spam a Instagram)
    task_default_rate_limit='10/m',  # 10 tareas por minuto por defecto
    
    # Rutas de tareas (diferentes colas por prioridad)
    task_routes={
        'src.jobs.login_job.*': {'queue': 'login', 'priority': 10},
        'src.jobs.send_message_job.*': {'queue': 'messages', 'priority': 7},
        'src.jobs.read_messages_job.*': {'queue': 'polling', 'priority': 5},
        'src.jobs.auto_reply_job.*': {'queue': 'replies', 'priority': 8},
    },
    
    # Límites de tiempo
    task_soft_time_limit=300,  # 5 minutos soft limit (lanza excepción)
    task_time_limit=360,  # 6 minutos hard limit (mata el proceso)
    
    # Monitoreo
    worker_send_task_events=True,
    task_send_sent_event=True,
    
    # Beat scheduler para polling periódico
    beat_schedule={
        'poll-messages-every-30s': {
            'task': 'src.jobs.read_messages_job.poll_all_accounts',
            'schedule': 30.0,  # cada 30 segundos
            'options': {'queue': 'polling', 'priority': 5}
        },
    },
)

if __name__ == '__main__':
    app.start()
