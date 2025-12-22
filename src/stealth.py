"""
Playwright Stealth y Fingerprinting consistente.
Evita detección de bots por Instagram.
"""

import logging
import random
import hashlib
from typing import Dict, Any, Optional
from playwright.sync_api import BrowserContext, Page
from pathlib import Path
import json

logger = logging.getLogger(__name__)


class FingerprintManager:
    """Gestiona fingerprints consistentes por cuenta."""
    
    def __init__(self, storage_dir: Path = None):
        self.storage_dir = storage_dir or Path(__file__).parent.parent / 'profiles'
        self.storage_dir.mkdir(parents=True, exist_ok=True)
    
    def _fingerprint_path(self, username: str) -> Path:
        """Path del archivo de fingerprint."""
        return self.storage_dir / username / 'fingerprint.json'
    
    def load_or_create(self, username: str) -> Dict[str, Any]:
        """Carga fingerprint existente o crea uno nuevo consistente."""
        fp_path = self._fingerprint_path(username)
        
        if fp_path.exists():
            try:
                data = json.loads(fp_path.read_text())
                logger.debug(f"Loaded fingerprint for @{username}")
                return data
            except Exception as e:
                logger.warning(f"Could not load fingerprint for @{username}: {e}")
        
        # Crear fingerprint nuevo basado en username (determinístico)
        fingerprint = self._generate_fingerprint(username)
        
        # Guardar
        fp_path.parent.mkdir(parents=True, exist_ok=True)
        fp_path.write_text(json.dumps(fingerprint, indent=2))
        logger.info(f"Created new fingerprint for @{username}")
        
        return fingerprint
    
    def _generate_fingerprint(self, username: str) -> Dict[str, Any]:
        """Genera fingerprint consistente basado en username."""
        # Usar hash del username como seed para reproducibilidad
        seed = int(hashlib.md5(username.encode()).hexdigest()[:8], 16)
        rng = random.Random(seed)
        
        # User agents realistas (Chrome en diferentes OS)
        user_agents = [
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        ]
        
        # Viewports comunes
        viewports = [
            {'width': 1920, 'height': 1080},
            {'width': 1366, 'height': 768},
            {'width': 1440, 'height': 900},
            {'width': 1536, 'height': 864},
        ]
        
        # Timezones comunes
        timezones = [
            'America/Argentina/Buenos_Aires',
            'America/Sao_Paulo',
            'America/Mexico_City',
            'America/New_York',
            'Europe/Madrid',
        ]
        
        # Locales
        locales = [
            'es-AR',
            'es-ES',
            'es-MX',
            'en-US',
            'pt-BR',
        ]
        
        return {
            'user_agent': rng.choice(user_agents),
            'viewport': rng.choice(viewports),
            'timezone': rng.choice(timezones),
            'locale': rng.choice(locales),
            'device_scale_factor': rng.choice([1, 1.5, 2]),
            'has_touch': rng.choice([True, False]),
            'color_scheme': rng.choice(['light', 'dark']),
            'reduced_motion': rng.choice(['no-preference', 'reduce']),
            # WebGL vendor/renderer (importante para fingerprinting)
            'webgl_vendor': 'Google Inc.',
            'webgl_renderer': rng.choice([
                'ANGLE (NVIDIA GeForce GTX 1060 Direct3D11 vs_5_0 ps_5_0)',
                'ANGLE (Intel(R) UHD Graphics 630 Direct3D11 vs_5_0 ps_5_0)',
                'ANGLE (AMD Radeon RX 580 Series Direct3D11 vs_5_0 ps_5_0)',
            ]),
            # Canvas fingerprint (simplificado)
            'canvas_hash': hashlib.sha256(f"{username}_canvas".encode()).hexdigest()[:16],
        }


def apply_stealth(context: BrowserContext, fingerprint: Dict[str, Any]) -> None:
    """
    Aplica técnicas de stealth al contexto de Playwright.
    
    Técnicas:
    - User agent consistente
    - Viewport realista
    - Timezone y locale
    - WebGL fingerprinting
    - Navigator properties
    - Permisos y features
    """
    
    # Inyectar scripts de stealth en cada página nueva
    context.add_init_script("""
        // Ocultar webdriver
        Object.defineProperty(navigator, 'webdriver', {
            get: () => undefined
        });
        
        // Ocultar automation
        delete navigator.__proto__.webdriver;
        
        // Chrome runtime
        window.chrome = {
            runtime: {}
        };
        
        // Permisos
        const originalQuery = window.navigator.permissions.query;
        window.navigator.permissions.query = (parameters) => (
            parameters.name === 'notifications' ?
                Promise.resolve({ state: Notification.permission }) :
                originalQuery(parameters)
        );
        
        // Plugins
        Object.defineProperty(navigator, 'plugins', {
            get: () => [1, 2, 3, 4, 5]
        });
        
        // Languages
        Object.defineProperty(navigator, 'languages', {
            get: () => ['es-AR', 'es', 'en']
        });
    """)
    
    # WebGL fingerprinting
    webgl_script = f"""
        const getParameter = WebGLRenderingContext.prototype.getParameter;
        WebGLRenderingContext.prototype.getParameter = function(parameter) {{
            if (parameter === 37445) {{
                return '{fingerprint.get("webgl_vendor", "Google Inc.")}';
            }}
            if (parameter === 37446) {{
                return '{fingerprint.get("webgl_renderer", "ANGLE")}';
            }}
            return getParameter.apply(this, arguments);
        }};
    """
    context.add_init_script(webgl_script)
    
    # Canvas fingerprinting (básico)
    canvas_script = """
        const originalToDataURL = HTMLCanvasElement.prototype.toDataURL;
        HTMLCanvasElement.prototype.toDataURL = function() {
            // Añadir ruido mínimo al canvas para consistencia
            const context = this.getContext('2d');
            if (context) {
                const imageData = context.getImageData(0, 0, this.width, this.height);
                // Modificar levemente algunos pixels (determinístico)
                for (let i = 0; i < imageData.data.length; i += 100) {
                    imageData.data[i] = (imageData.data[i] + 1) % 256;
                }
                context.putImageData(imageData, 0, 0);
            }
            return originalToDataURL.apply(this, arguments);
        };
    """
    context.add_init_script(canvas_script)
    
    # Battery API (evitar detección)
    context.add_init_script("""
        if ('getBattery' in navigator) {
            navigator.getBattery = () => Promise.resolve({
                charging: true,
                chargingTime: 0,
                dischargingTime: Infinity,
                level: 1
            });
        }
    """
)
    
    logger.debug(f"Stealth applied: {fingerprint.get('user_agent', 'unknown')}")


def configure_browser_context(
    context: BrowserContext,
    username: str,
    proxy: Optional[Dict] = None,
) -> None:
    """
    Configura contexto de navegador con stealth y fingerprint.
    
    Args:
        context: Contexto de Playwright
        username: Usuario (para cargar fingerprint consistente)
        proxy: Configuración de proxy (opcional)
    """
    # Cargar o crear fingerprint
    fp_manager = FingerprintManager()
    fingerprint = fp_manager.load_or_create(username)
    
    # Aplicar stealth
    apply_stealth(context, fingerprint)
    
    # Configurar permisos
    context.grant_permissions(['notifications', 'geolocation'])
    
    # Configurar geolocation (basado en timezone)
    # Coordenadas aproximadas según timezone
    geo_coords = {
        'America/Argentina/Buenos_Aires': {'latitude': -34.6037, 'longitude': -58.3816},
        'America/Sao_Paulo': {'latitude': -23.5505, 'longitude': -46.6333},
        'America/Mexico_City': {'latitude': 19.4326, 'longitude': -99.1332},
        'America/New_York': {'latitude': 40.7128, 'longitude': -74.0060},
        'Europe/Madrid': {'latitude': 40.4168, 'longitude': -3.7038},
    }
    
    timezone = fingerprint.get('timezone', 'America/Argentina/Buenos_Aires')
    coords = geo_coords.get(timezone, geo_coords['America/Argentina/Buenos_Aires'])
    
    context.set_geolocation(coords)
    
    # Configurar extra headers (anti-fingerprinting)
    context.set_extra_http_headers({
        'Accept-Language': f"{fingerprint.get('locale', 'es-AR')},es;q=0.9,en;q=0.8",
        'Accept-Encoding': 'gzip, deflate, br',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
        'DNT': '1',
        'Connection': 'keep-alive',
        'Upgrade-Insecure-Requests': '1',
    })
    
    logger.info(f"Browser context configured for @{username} with fingerprint")


def add_human_behavior(page: Page) -> None:
    """
    Añade comportamientos humanos a la página.
    - Mouse movements aleatorios
    - Scroll natural
    - Pausas variables
    """
    
    # Script para movimientos de mouse aleatorios
    page.evaluate("""
        () => {
            let mouseX = 0;
            let mouseY = 0;
            
            function moveMouseRandomly() {
                const deltaX = (Math.random() - 0.5) * 100;
                const deltaY = (Math.random() - 0.5) * 100;
                
                mouseX = Math.max(0, Math.min(window.innerWidth, mouseX + deltaX));
                mouseY = Math.max(0, Math.min(window.innerHeight, mouseY + deltaY));
                
                const event = new MouseEvent('mousemove', {
                    clientX: mouseX,
                    clientY: mouseY,
                    bubbles: true
                });
                document.dispatchEvent(event);
            }
            
            // Mover mouse cada 2-5 segundos
            setInterval(moveMouseRandomly, Math.random() * 3000 + 2000);
        }
    """)
    
    logger.debug("Human behavior scripts injected")


__all__ = [
    'FingerprintManager',
    'apply_stealth',
    'configure_browser_context',
    'add_human_behavior',
]
