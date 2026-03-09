#!/usr/bin/env bash
# Deploy Telegram MCP Server to Laba
# Standard: Rick.ai Laba Deployment Standard 5.18
#
# Prerequisites:
#   1. .env.laba file with credentials
#   2. Supabase migration applied
#   3. Docker installed on laba
#
# Usage:
#   ./scripts/deploy-to-laba.sh          # Build and start
#   ./scripts/deploy-to-laba.sh restart   # Restart
#   ./scripts/deploy-to-laba.sh stop      # Stop
#   ./scripts/deploy-to-laba.sh logs      # View logs
#   ./scripts/deploy-to-laba.sh status    # Check status

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
COMPOSE_FILE="$PROJECT_DIR/docker-compose.laba.yml"
SERVICE_NAME="telegram-mcp-laba"

cd "$PROJECT_DIR"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

log_info()  { echo -e "${GREEN}[INFO]${NC} $1"; }
log_warn()  { echo -e "${YELLOW}[WARN]${NC} $1"; }
log_error() { echo -e "${RED}[ERROR]${NC} $1"; }

# Pre-flight checks
preflight() {
    log_info "Running pre-flight checks..."

    # Check .env.laba exists
    if [ ! -f "$PROJECT_DIR/.env.laba" ]; then
        log_error ".env.laba not found! Create it with required credentials."
        echo "Required variables:"
        echo "  TELEGRAM_API_ID=..."
        echo "  TELEGRAM_API_HASH=..."
        echo "  TELEGRAM_SESSION_STRING=..."
        echo "  SUPABASE_URL=https://supabase.rick.ai"
        echo "  SUPABASE_API_KEY=..."
        echo "  TELEGRAM_USER=ikrasinsky"
        echo "  LABA_MODE=true"
        exit 1
    fi

    # Check Docker is available
    if ! command -v docker &> /dev/null; then
        log_error "Docker not found. Install Docker first."
        exit 1
    fi

    # Check Docker Compose
    if ! docker compose version &> /dev/null; then
        log_error "Docker Compose not found."
        exit 1
    fi

    # Check .env.laba has required variables
    local missing=0
    for var in TELEGRAM_API_ID TELEGRAM_API_HASH TELEGRAM_SESSION_STRING SUPABASE_API_KEY; do
        if ! grep -q "^${var}=" "$PROJECT_DIR/.env.laba" 2>/dev/null; then
            log_warn "Missing ${var} in .env.laba"
            missing=1
        fi
    done

    if [ $missing -eq 1 ]; then
        log_warn "Some environment variables may be missing. Deployment may fail."
    fi

    log_info "Pre-flight checks passed."
}

build() {
    log_info "Building Docker image..."
    docker compose -f "$COMPOSE_FILE" build --no-cache
    log_info "Build complete."
}

start() {
    preflight
    log_info "Starting $SERVICE_NAME..."
    docker compose -f "$COMPOSE_FILE" up -d
    log_info "$SERVICE_NAME started."
    sleep 3
    status
}

stop() {
    log_info "Stopping $SERVICE_NAME..."
    docker compose -f "$COMPOSE_FILE" down
    log_info "$SERVICE_NAME stopped."
}

restart() {
    log_info "Restarting $SERVICE_NAME..."
    docker compose -f "$COMPOSE_FILE" restart
    log_info "$SERVICE_NAME restarted."
    sleep 3
    status
}

status() {
    log_info "Status of $SERVICE_NAME:"
    docker compose -f "$COMPOSE_FILE" ps
    echo ""
    log_info "Last 10 log lines:"
    docker compose -f "$COMPOSE_FILE" logs --tail=10 "$SERVICE_NAME" 2>/dev/null || true
}

logs() {
    docker compose -f "$COMPOSE_FILE" logs -f "$SERVICE_NAME"
}

# Main
case "${1:-start}" in
    build)   build ;;
    start)   build && start ;;
    stop)    stop ;;
    restart) restart ;;
    status)  status ;;
    logs)    logs ;;
    *)
        echo "Usage: $0 {build|start|stop|restart|status|logs}"
        exit 1
        ;;
esac
