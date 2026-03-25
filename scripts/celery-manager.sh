#!/bin/bash
# Script helper para gestionar workers de Celery

set -e

VENV_PATH=".venv/bin/activate"
CELERY_APP="src.queue_config"

# Colores
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

function print_header() {
    echo -e "${BLUE}========================================${NC}"
    echo -e "${BLUE}  $1${NC}"
    echo -e "${BLUE}========================================${NC}"
}

function print_success() {
    echo -e "${GREEN}✅ $1${NC}"
}

function print_error() {
    echo -e "${RED}❌ $1${NC}"
}

function print_warning() {
    echo -e "${YELLOW}⚠️  $1${NC}"
}

function check_redis() {
    print_header "Verificando Redis"
    
    if redis-cli ping > /dev/null 2>&1; then
        print_success "Redis está corriendo"
        return 0
    else
        print_error "Redis no está corriendo"
        echo "Inicia Redis con: sudo systemctl start redis-server"
        return 1
    fi
}

function check_venv() {
    if [ ! -f "$VENV_PATH" ]; then
        print_error "Virtual environment no encontrado en $VENV_PATH"
        return 1
    fi
    return 0
}

function start_worker() {
    print_header "Iniciando Worker de Celery"
    
    check_redis || exit 1
    check_venv || exit 1
    
    source "$VENV_PATH"
    
    echo "Iniciando worker con todas las colas..."
    celery -A "$CELERY_APP" worker \
        --loglevel=info \
        --concurrency=4 \
        --max-tasks-per-child=50 \
        --time-limit=300 \
        --soft-time-limit=270
}

function start_beat() {
    print_header "Iniciando Celery Beat (Scheduler)"
    
    check_redis || exit 1
    check_venv || exit 1
    
    source "$VENV_PATH"
    
    echo "Iniciando beat para polling automático..."
    celery -A "$CELERY_APP" beat --loglevel=info
}

function start_flower() {
    print_header "Iniciando Flower (Dashboard)"
    
    check_redis || exit 1
    check_venv || exit 1
    
    source "$VENV_PATH"
    
    echo "Iniciando Flower en http://localhost:5555 ..."
    celery -A "$CELERY_APP" flower --port=5555
}

function stop_workers() {
    print_header "Deteniendo Workers"
    
    pkill -f "celery.*worker" && print_success "Workers detenidos" || print_warning "No hay workers corriendo"
    pkill -f "celery.*beat" && print_success "Beat detenido" || print_warning "No hay beat corriendo"
    pkill -f "celery.*flower" && print_success "Flower detenido" || print_warning "No hay flower corriendo"
}

function status() {
    print_header "Estado del Sistema"
    
    # Redis
    if redis-cli ping > /dev/null 2>&1; then
        print_success "Redis: Corriendo"
    else
        print_error "Redis: Detenido"
    fi
    
    # Workers
    if pgrep -f "celery.*worker" > /dev/null; then
        WORKER_COUNT=$(pgrep -f "celery.*worker" | wc -l)
        print_success "Workers: $WORKER_COUNT corriendo"
    else
        print_warning "Workers: Ninguno corriendo"
    fi
    
    # Beat
    if pgrep -f "celery.*beat" > /dev/null; then
        print_success "Beat: Corriendo"
    else
        print_warning "Beat: Detenido"
    fi
    
    # Flower
    if pgrep -f "celery.*flower" > /dev/null; then
        print_success "Flower: Corriendo (http://localhost:5555)"
    else
        print_warning "Flower: Detenido"
    fi
    
    echo ""
    echo "Tareas en Redis:"
    source "$VENV_PATH" 2>/dev/null
    celery -A "$CELERY_APP" inspect active 2>/dev/null || echo "  (no disponible)"
}

function purge_queue() {
    print_header "Limpiar Cola de Tareas"
    
    read -p "⚠️  Esto eliminará TODAS las tareas pendientes. ¿Continuar? (y/N): " -n 1 -r
    echo
    if [[ $REPLY =~ ^[Yy]$ ]]; then
        source "$VENV_PATH"
        celery -A "$CELERY_APP" purge -f
        print_success "Cola limpiada"
    else
        print_warning "Cancelado"
    fi
}

function test_system() {
    print_header "Ejecutar Tests"
    
    check_redis || exit 1
    check_venv || exit 1
    
    source "$VENV_PATH"
    python run_test_jobs.py
}

function show_help() {
    echo "Uso: $0 [comando]"
    echo ""
    echo "Comandos disponibles:"
    echo "  start-worker    Iniciar worker de Celery"
    echo "  start-beat      Iniciar Celery Beat (polling automático)"
    echo "  start-flower    Iniciar Flower (dashboard web)"
    echo "  stop            Detener todos los workers"
    echo "  status          Ver estado del sistema"
    echo "  purge           Limpiar cola de tareas"
    echo "  test            Ejecutar tests"
    echo "  help            Mostrar esta ayuda"
    echo ""
    echo "Ejemplos:"
    echo "  $0 start-worker    # Inicia worker en foreground"
    echo "  $0 status          # Ver estado"
    echo "  $0 stop            # Detener todo"
}

# Main
case "${1:-help}" in
    start-worker)
        start_worker
        ;;
    start-beat)
        start_beat
        ;;
    start-flower)
        start_flower
        ;;
    stop)
        stop_workers
        ;;
    status)
        status
        ;;
    purge)
        purge_queue
        ;;
    test)
        test_system
        ;;
    help|--help|-h)
        show_help
        ;;
    *)
        print_error "Comando desconocido: $1"
        echo ""
        show_help
        exit 1
        ;;
esac
