"""
Docker Configuration y Scripts para el Microservicio FastAPI
===========================================================

Este archivo contiene las configuraciones Docker y scripts necesarios
para ejecutar el microservicio de notificaciones en contenedores.
"""

# =============================================================================
# DOCKERFILE
# =============================================================================

DOCKERFILE_CONTENT = """
# Dockerfile para el microservicio de notificaciones
FROM python:3.11-slim

# Configurar zona horaria
ENV TZ=UTC
RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone

# Crear usuario no-root
RUN groupadd -r appuser && useradd -r -g appuser appuser

# Directorio de trabajo
WORKDIR /app

# Instalar dependencias del sistema
RUN apt-get update && apt-get install -y \\
    gcc \\
    && rm -rf /var/lib/apt/lists/*

# Copiar requirements
COPY requirements.txt .

# Instalar dependencias Python
RUN pip install --no-cache-dir -r requirements.txt

# Copiar código fuente
COPY . .

# Cambiar a usuario no-root
RUN chown -R appuser:appuser /app
USER appuser

# Exponer puerto
EXPOSE 8000

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \\
    CMD curl -f http://localhost:8000/health || exit 1

# Comando por defecto
CMD ["python", "fastapi_microservice.py"]
"""

# =============================================================================
# DOCKER COMPOSE
# =============================================================================

DOCKER_COMPOSE_CONTENT = """
version: '3.8'

services:
  # NATS JetStream
  nats:
    image: nats:2.10-alpine
    container_name: nats-server
    ports:
      - "4222:4222"
      - "8222:8222"  # Management interface
    command: [
      "--jetstream",
      "--store_dir", "/data",
      "--max_memory_store", "1GB",
      "--max_file_store", "10GB"
    ]
    volumes:
      - nats_data:/data
    networks:
      - microservices
    restart: unless-stopped

  # Microservicio de Notificaciones
  notification-service:
    build: .
    container_name: notification-service
    ports:
      - "8000:8000"
    environment:
      # Configuración NATS
      - NATS_SERVERS=nats://nats:4222
      - NATS_STREAM_NAME=notification-events
      - NATS_SUBJECT_PREFIX=notifications.events
      - NATS_MAX_MESSAGES=1000000
      - NATS_MAX_AGE_SECONDS=2592000  # 30 días
      - NATS_REPLICAS=1
      
      # Configuración del servicio
      - LOG_LEVEL=INFO
      - LOG_STRUCTURED=true
      - SERVICE_NAME=notification-service
      - SERVICE_VERSION=1.0.0
    depends_on:
      - nats
    networks:
      - microservices
    restart: unless-stopped
    healthcheck:
      test: ["CMD", "curl", "-f", "http://localhost:8000/health"]
      interval: 30s
      timeout: 10s
      retries: 3
      start_period: 40s

  # Ejemplo de otro microservicio que publica eventos
  user-service:
    image: nginx:alpine  # Placeholder - reemplazar con tu servicio real
    container_name: user-service
    ports:
      - "8001:80"
    networks:
      - microservices
    restart: unless-stopped

volumes:
  nats_data:

networks:
  microservices:
    driver: bridge
"""

# =============================================================================
# REQUIREMENTS.TXT
# =============================================================================

REQUIREMENTS_CONTENT = """
# FastAPI y dependencias
fastapi==0.104.1
uvicorn[standard]==0.24.0
pydantic==2.5.0

# TC NATS Events
git+https://github.com/ENG-Teamcore4-0/tc-nats-events.git@main

# Utilidades adicionales
httpx==0.25.2  # Para health checks
pyyaml==6.0.1  # Para configuración
python-multipart==0.0.6  # Para form data
"""

# =============================================================================
# SCRIPTS DE DESARROLLO
# =============================================================================

DEV_SCRIPT_CONTENT = """
#!/bin/bash
# dev.sh - Script para desarrollo local

set -e

echo "🚀 Iniciando entorno de desarrollo..."

# Verificar si Docker está ejecutándose
if ! docker info > /dev/null 2>&1; then
    echo "❌ Docker no está ejecutándose"
    exit 1
fi

# Crear network si no existe
docker network create microservices 2>/dev/null || true

# Iniciar NATS
echo "📡 Iniciando NATS JetStream..."
docker run -d \\
    --name nats-dev \\
    --network microservices \\
    -p 4222:4222 \\
    -p 8222:8222 \\
    nats:2.10-alpine \\
    --jetstream \\
    --store_dir /data \\
    --max_memory_store 1GB \\
    --max_file_store 10GB

# Esperar a que NATS esté listo
echo "⏳ Esperando que NATS esté listo..."
sleep 5

# Configurar variables de entorno
export NATS_SERVERS="nats://localhost:4222"
export NATS_STREAM_NAME="dev-notification-events"
export NATS_SUBJECT_PREFIX="dev.notifications.events"
export LOG_LEVEL="DEBUG"
export LOG_STRUCTURED="false"

echo "✅ Entorno listo. Puedes ejecutar:"
echo "   python fastapi_microservice.py"
echo ""
echo "🌐 Endpoints disponibles:"
echo "   - API: http://localhost:8000"
echo "   - NATS Management: http://localhost:8222"
echo ""
echo "🛑 Para detener: ./stop-dev.sh"
"""

STOP_DEV_SCRIPT_CONTENT = """
#!/bin/bash
# stop-dev.sh - Detener entorno de desarrollo

set -e

echo "🛑 Deteniendo entorno de desarrollo..."

# Detener y remover contenedor NATS
docker stop nats-dev 2>/dev/null || true
docker rm nats-dev 2>/dev/null || true

echo "✅ Entorno detenido"
"""

# =============================================================================
# KUBERNETES MANIFESTS
# =============================================================================

K8S_DEPLOYMENT_CONTENT = """
apiVersion: apps/v1
kind: Deployment
metadata:
  name: notification-service
  labels:
    app: notification-service
    version: v1
spec:
  replicas: 3
  selector:
    matchLabels:
      app: notification-service
  template:
    metadata:
      labels:
        app: notification-service
        version: v1
    spec:
      containers:
      - name: notification-service
        image: notification-service:1.0.0
        ports:
        - containerPort: 8000
          name: http
        env:
        - name: NATS_SERVERS
          value: "nats://nats-service:4222"
        - name: NATS_STREAM_NAME
          value: "notification-events"
        - name: NATS_SUBJECT_PREFIX
          value: "notifications.events"
        - name: LOG_STRUCTURED
          value: "true"
        - name: LOG_LEVEL
          value: "INFO"
        resources:
          requests:
            memory: "128Mi"
            cpu: "100m"
          limits:
            memory: "512Mi"
            cpu: "500m"
        livenessProbe:
          httpGet:
            path: /health
            port: 8000
          initialDelaySeconds: 30
          periodSeconds: 10
        readinessProbe:
          httpGet:
            path: /health
            port: 8000
          initialDelaySeconds: 5
          periodSeconds: 5
        lifecycle:
          preStop:
            exec:
              command: ["/bin/sh", "-c", "sleep 10"]
---
apiVersion: v1
kind: Service
metadata:
  name: notification-service
  labels:
    app: notification-service
spec:
  selector:
    app: notification-service
  ports:
  - port: 80
    targetPort: 8000
    name: http
  type: ClusterIP
---
apiVersion: networking.k8s.io/v1
kind: Ingress
metadata:
  name: notification-service
  annotations:
    nginx.ingress.kubernetes.io/rewrite-target: /
spec:
  rules:
  - host: notifications.ejemplo.com
    http:
      paths:
      - path: /
        pathType: Prefix
        backend:
          service:
            name: notification-service
            port:
              number: 80
"""

# =============================================================================
# CONFIGURACIÓN DE MONITOREO
# =============================================================================

PROMETHEUS_CONFIG_CONTENT = """
# prometheus.yml - Configuración para monitoreo
global:
  scrape_interval: 15s

scrape_configs:
  - job_name: 'notification-service'
    static_configs:
      - targets: ['notification-service:8000']
    metrics_path: '/metrics'
    scrape_interval: 30s

  - job_name: 'nats'
    static_configs:
      - targets: ['nats:8222']
    metrics_path: '/varz'
    scrape_interval: 30s
"""

# =============================================================================
# EJEMPLO DE TESTS
# =============================================================================

TEST_EXAMPLE_CONTENT = """
# test_notification_service.py - Ejemplo de tests

import pytest
import asyncio
from httpx import AsyncClient
from fastapi.testclient import TestClient

from fastapi_microservice import app, notification_service


@pytest.fixture
async def client():
    # Configurar servicio para tests
    await notification_service.start()
    
    async with AsyncClient(app=app, base_url="http://test") as ac:
        yield ac
    
    await notification_service.stop()


@pytest.mark.asyncio
async def test_health_endpoint(client):
    response = await client.get("/health")
    assert response.status_code == 200
    data = response.json()
    assert data["service"] == "notification-service"


@pytest.mark.asyncio
async def test_send_email_notification(client):
    notification_data = {
        "user_id": "test-user-123",
        "type": "email",
        "template": "welcome",
        "data": {
            "email": "test@ejemplo.com",
            "name": "Test User"
        }
    }
    
    response = await client.post("/notifications/send", json=notification_data)
    assert response.status_code == 200
    data = response.json()
    assert data["status"] == "sent"
    assert "notification_id" in data


@pytest.mark.asyncio
async def test_get_statistics(client):
    response = await client.get("/stats")
    assert response.status_code == 200
    data = response.json()
    assert "total_processed" in data
    assert "emails_sent" in data
"""

# =============================================================================
# FUNCIÓN PARA GENERAR ARCHIVOS
# =============================================================================

def generate_deployment_files():
    """Generar todos los archivos de configuración."""
    
    files = {
        "Dockerfile": DOCKERFILE_CONTENT,
        "docker-compose.yml": DOCKER_COMPOSE_CONTENT,
        "requirements.txt": REQUIREMENTS_CONTENT,
        "dev.sh": DEV_SCRIPT_CONTENT,
        "stop-dev.sh": STOP_DEV_SCRIPT_CONTENT,
        "k8s-deployment.yaml": K8S_DEPLOYMENT_CONTENT,
        "prometheus.yml": PROMETHEUS_CONFIG_CONTENT,
        "test_notification_service.py": TEST_EXAMPLE_CONTENT
    }
    
    for filename, content in files.items():
        with open(filename, 'w') as f:
            f.write(content.strip())
        print(f"✅ Generado: {filename}")


if __name__ == "__main__":
    print("🐳 Generando archivos de configuración Docker...")
    generate_deployment_files()
    print("\n🎉 Archivos generados exitosamente!")
    print("\nPara comenzar:")
    print("1. chmod +x dev.sh stop-dev.sh")
    print("2. ./dev.sh")
    print("3. python fastapi_microservice.py")