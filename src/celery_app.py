"""
Celery app - importa configuración avanzada desde queue_config.

Para compatibilidad con código legacy, mantenemos este archivo.
La configuración real está en src/queue_config.py
"""

from src.queue_config import app

# Re-exportar para compatibilidad
__all__ = ['app']

