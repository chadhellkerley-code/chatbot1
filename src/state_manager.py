"""
Gestión de estados de cuenta con Redis.
Almacena cookies, sesiones, fingerprints y estado de autenticación.
"""

import json
import logging
from typing import Dict, Optional, Any
from datetime import datetime, timedelta
import redis
from cryptography.fernet import Fernet
import os
from pathlib import Path

logger = logging.getLogger(__name__)

REDIS_URL = os.getenv('REDIS_URL', 'redis://localhost:6379/0')
ENCRYPTION_KEY = os.getenv('SESSION_ENCRYPTION_KEY')

# Si no existe la key, generarla y guardarla
if not ENCRYPTION_KEY:
    key_file = Path(__file__).parent.parent / '.session_key'
    if key_file.exists():
        ENCRYPTION_KEY = key_file.read_text().strip()
    else:
        ENCRYPTION_KEY = Fernet.generate_key().decode()
        key_file.write_text(ENCRYPTION_KEY)
        logger.warning(f"Generated new encryption key at {key_file}")

cipher = Fernet(ENCRYPTION_KEY.encode() if isinstance(ENCRYPTION_KEY, str) else ENCRYPTION_KEY)


class AccountStateManager:
    """Gestiona estados de cuenta en Redis con encriptación."""
    
    def __init__(self, redis_url: str = REDIS_URL):
        self.redis_client = redis.from_url(redis_url, decode_responses=False)
        self.redis_text = redis.from_url(redis_url, decode_responses=True)
    
    def _key(self, username: str, suffix: str = 'state') -> str:
        """Genera key de Redis para username."""
        return f"ig:account:{username.lower()}:{suffix}"
    
    def save_session(self, username: str, session_data: Dict[str, Any], ttl: int = 86400 * 7) -> bool:
        """
        Guarda sesión encriptada en Redis.
        
        Args:
            username: Usuario de Instagram
            session_data: Datos de sesión (cookies, storage_state, etc)
            ttl: Tiempo de vida en segundos (default 7 días)
        """
        try:
            # Serializar y encriptar
            json_data = json.dumps(session_data, ensure_ascii=False)
            encrypted = cipher.encrypt(json_data.encode('utf-8'))
            
            # Guardar en Redis
            key = self._key(username, 'session')
            self.redis_client.setex(key, ttl, encrypted)
            
            # Metadata sin encriptar para queries rápidas
            meta_key = self._key(username, 'meta')
            metadata = {
                'username': username,
                'last_login': datetime.utcnow().isoformat(),
                'session_expires': (datetime.utcnow() + timedelta(seconds=ttl)).isoformat(),
            }
            self.redis_text.setex(meta_key, ttl, json.dumps(metadata))
            
            logger.info(f"Session saved for @{username} (TTL: {ttl}s)")
            return True
        except Exception as e:
            logger.error(f"Failed to save session for @{username}: {e}")
            return False
    
    def load_session(self, username: str) -> Optional[Dict[str, Any]]:
        """Carga sesión encriptada desde Redis."""
        try:
            key = self._key(username, 'session')
            encrypted = self.redis_client.get(key)
            
            if not encrypted:
                logger.debug(f"No session found for @{username}")
                return None
            
            # Desencriptar y deserializar
            decrypted = cipher.decrypt(encrypted)
            session_data = json.loads(decrypted.decode('utf-8'))
            
            logger.info(f"Session loaded for @{username}")
            return session_data
        except Exception as e:
            logger.error(f"Failed to load session for @{username}: {e}")
            return None
    
    def delete_session(self, username: str) -> bool:
        """Elimina sesión de Redis."""
        try:
            key = self._key(username, 'session')
            meta_key = self._key(username, 'meta')
            self.redis_client.delete(key, meta_key)
            logger.info(f"Session deleted for @{username}")
            return True
        except Exception as e:
            logger.error(f"Failed to delete session for @{username}: {e}")
            return False
    
    def session_exists(self, username: str) -> bool:
        """Verifica si existe sesión válida."""
        key = self._key(username, 'session')
        return self.redis_client.exists(key) > 0
    
    def save_account_state(self, username: str, state: Dict[str, Any], ttl: int = 86400 * 30) -> bool:
        """
        Guarda estado general de cuenta (no sensible).
        
        Args:
            state: {
                'status': 'active' | 'banned' | 'challenge' | 'rate_limited',
                'last_activity': timestamp,
                'messages_sent_today': int,
                'rate_limit_until': timestamp,
                'proxy': str,
                'fingerprint': dict,
            }
        """
        try:
            key = self._key(username, 'state')
            state['updated_at'] = datetime.utcnow().isoformat()
            self.redis_text.setex(key, ttl, json.dumps(state, ensure_ascii=False))
            logger.debug(f"State saved for @{username}")
            return True
        except Exception as e:
            logger.error(f"Failed to save state for @{username}: {e}")
            return False
    
    def load_account_state(self, username: str) -> Optional[Dict[str, Any]]:
        """Carga estado de cuenta."""
        try:
            key = self._key(username, 'state')
            data = self.redis_text.get(key)
            if not data:
                return None
            return json.loads(data)
        except Exception as e:
            logger.error(f"Failed to load state for @{username}: {e}")
            return None
    
    def increment_daily_counter(self, username: str, counter_name: str = 'messages_sent') -> int:
        """Incrementa contador diario (ej: mensajes enviados)."""
        key = self._key(username, f'counter:{counter_name}')
        count = self.redis_text.incr(key)
        
        # Expira a medianoche
        if count == 1:
            ttl = (datetime.utcnow().replace(hour=23, minute=59, second=59) - datetime.utcnow()).seconds
            self.redis_text.expire(key, ttl)
        
        return count
    
    def get_daily_counter(self, username: str, counter_name: str = 'messages_sent') -> int:
        """Obtiene valor de contador diario."""
        key = self._key(username, f'counter:{counter_name}')
        value = self.redis_text.get(key)
        return int(value) if value else 0
    
    def set_rate_limit(self, username: str, seconds: int) -> None:
        """Marca cuenta como rate limited por X segundos."""
        key = self._key(username, 'rate_limit')
        self.redis_text.setex(key, seconds, '1')
        
        # Actualizar estado
        state = self.load_account_state(username) or {}
        state['status'] = 'rate_limited'
        state['rate_limit_until'] = (datetime.utcnow() + timedelta(seconds=seconds)).isoformat()
        self.save_account_state(username, state)
    
    def is_rate_limited(self, username: str) -> bool:
        """Verifica si cuenta está rate limited."""
        key = self._key(username, 'rate_limit')
        return self.redis_text.exists(key) > 0


# Singleton global
_state_manager: Optional[AccountStateManager] = None

def get_state_manager() -> AccountStateManager:
    """Obtiene instancia singleton del state manager."""
    global _state_manager
    if _state_manager is None:
        _state_manager = AccountStateManager()
    return _state_manager
