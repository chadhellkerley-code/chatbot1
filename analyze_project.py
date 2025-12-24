"""
Script de Análisis de Dependencias del Proyecto
Identifica qué archivos están siendo importados y usados activamente
"""

import os
import re
from pathlib import Path
from collections import defaultdict

# Directorio raíz del proyecto
PROJECT_ROOT = Path(__file__).parent

# Archivos a analizar (Python files)
PYTHON_FILES = []

# Patrones de import
IMPORT_PATTERNS = [
    r'^import\s+(\w+)',
    r'^from\s+(\w+)\s+import',
    r'^from\s+(\w+)\.(\w+)\s+import',
]

def find_python_files():
    """Encuentra todos los archivos Python en el proyecto"""
    for root, dirs, files in os.walk(PROJECT_ROOT):
        # Ignorar directorios
        dirs[:] = [d for d in dirs if d not in ['.venv', '.venv-1', '.venv_win', 'venv', '__pycache__', '.git']]
        
        for file in files:
            if file.endswith('.py'):
                PYTHON_FILES.append(Path(root) / file)

def analyze_imports(file_path):
    """Analiza los imports de un archivo"""
    imports = []
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            for line in f:
                line = line.strip()
                for pattern in IMPORT_PATTERNS:
                    match = re.match(pattern, line)
                    if match:
                        imports.append(match.group(1))
                        break
    except Exception as e:
        print(f"Error leyendo {file_path}: {e}")
    
    return imports

def main():
    print("=" * 80)
    print("ANALISIS DE DEPENDENCIAS DEL PROYECTO")
    print("=" * 80)
    print()
    
    # Encontrar archivos Python
    find_python_files()
    print(f"[*] Archivos Python encontrados: {len(PYTHON_FILES)}")
    print()
    
    # Analizar imports
    import_map = defaultdict(list)  # module -> [files that import it]
    
    for py_file in PYTHON_FILES:
        imports = analyze_imports(py_file)
        for imp in imports:
            import_map[imp].append(py_file.relative_to(PROJECT_ROOT))
    
    # Archivos en la raíz del proyecto
    root_files = [f for f in PROJECT_ROOT.glob('*.py')]
    
    print("=" * 80)
    print("ARCHIVOS PYTHON EN LA RAÍZ")
    print("=" * 80)
    print()
    
    for file in sorted(root_files):
        module_name = file.stem
        importers = import_map.get(module_name, [])
        
        size_kb = file.stat().st_size / 1024
        status = "[OK] EN USO" if importers else "[!] NO IMPORTADO"
        
        print(f"{status} - {file.name} ({size_kb:.1f} KB)")
        if importers:
            print(f"   Importado por: {len(importers)} archivo(s)")
            for imp in importers[:3]:  # Mostrar solo los primeros 3
                print(f"      - {imp}")
            if len(importers) > 3:
                print(f"      ... y {len(importers) - 3} más")
        print()
    
    # Archivos duplicados potenciales
    print("=" * 80)
    print("ANÁLISIS DE DUPLICADOS POTENCIALES")
    print("=" * 80)
    print()
    
    # Buscar archivos con el mismo nombre en diferentes ubicaciones
    file_names = defaultdict(list)
    for py_file in PYTHON_FILES:
        file_names[py_file.name].append(py_file.relative_to(PROJECT_ROOT))
    
    duplicates = {name: paths for name, paths in file_names.items() if len(paths) > 1}
    
    if duplicates:
        for name, paths in sorted(duplicates.items()):
            print(f"[FILE] {name} - {len(paths)} copias:")
            for path in paths:
                full_path = PROJECT_ROOT / path
                size_kb = full_path.stat().st_size / 1024
                module_name = path.stem
                importers = import_map.get(module_name, [])
                status = "[OK] USADO" if importers else "[X] NO USADO"
                print(f"   {status} - {path} ({size_kb:.1f} KB)")
            print()
    else:
        print("No se encontraron archivos duplicados por nombre.")
        print()
    
    # Archivos específicos a revisar
    print("=" * 80)
    print("ARCHIVOS ESPECÍFICOS A REVISAR")
    print("=" * 80)
    print()
    
    files_to_check = [
        'instagram_adapter.py',
        'playwright_service.py',
        'license_client.py',
        'backend_license_client.py',
        'licensekit.py',
    ]
    
    for filename in files_to_check:
        print(f"\n[SEARCH] Buscando: {filename}")
        found = False
        for py_file in PYTHON_FILES:
            if py_file.name == filename:
                found = True
                rel_path = py_file.relative_to(PROJECT_ROOT)
                size_kb = py_file.stat().st_size / 1024
                module_name = py_file.stem
                importers = import_map.get(module_name, [])
                
                print(f"   [FOUND] {rel_path} ({size_kb:.1f} KB)")
                if importers:
                    print(f"      [OK] Importado por {len(importers)} archivo(s)")
                else:
                    print(f"      [!] NO esta siendo importado directamente")
        
        if not found:
            print(f"   [X] No encontrado")
    
    print()
    print("=" * 80)
    print("RESUMEN")
    print("=" * 80)
    print()
    print(f"Total de archivos Python: {len(PYTHON_FILES)}")
    print(f"Archivos en raíz: {len(root_files)}")
    print(f"Archivos duplicados (por nombre): {len(duplicates)}")
    print()
    print("[OK] Analisis completado")
    print()

if __name__ == "__main__":
    main()
