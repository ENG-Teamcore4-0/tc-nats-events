"""
FastAPI Microservice Consumer Example
====================================

Este ejemplo muestra cómo crear un microservicio completo con FastAPI
que consume eventos usando TC NATS Events. Incluye:

- Configuración del microservicio
- Manejo de eventos en background
- API REST para consultar estado
- Logging estructurado
- Manejo de errores
- Health checks
- Graceful shutdown

Caso de uso: Servicio de Notificaciones que procesa eventos de usuarios
"""

import asyncio
import signal
import logging
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from typing import Dict, List, Any, Optional
from dataclasses import dataclass, field

from fastapi import FastAPI, HTTPException, BackgroundTasks, Depends
from fastapi.responses import JSONResponse
from pydantic import BaseModel
import uvicorn

from tc_nats_events import (
    DurableEventConsumer,
    EventPublisher,
    NATSConfig,
    Event,
    EventMetadata,
    setup_logging,
)


# =============================================================================
# CONFIGURACIÓN Y MODELOS
# =============================================================================

@dataclass
class NotificationStats:
    """Estadísticas del servicio de notificaciones."""
    total_processed: int = 0
    total_failed: int = 0
    emails_sent: int = 0
    sms_sent: int = 0
    push_notifications_sent: int = 0
    last_processed_at: Optional[str] = None
    service_started_at: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())


class NotificationRequest(BaseModel):
    """Modelo para solicitud de notificación manual."""
    user_id: str
    type: str  # email, sms, push
    template: str
    data: Dict[str, Any]


class NotificationResponse(BaseModel):
    """Respuesta del servicio."""
    notification_id: str
    status: str
    message: str


class HealthResponse(BaseModel):
    """Respuesta de health check."""
    status: str
    service: str
    version: str
    uptime_seconds: float
    consumer_synced: bool
    events_processed: int
    last_event: Optional[str]


# =============================================================================
# SERVICIO DE NOTIFICACIONES
# =============================================================================

class NotificationService:
    """Servicio de notificaciones que procesa eventos de usuarios."""
    
    def __init__(self):
        self.stats = NotificationStats()
        self.processed_notifications: Dict[str, Dict[str, Any]] = {}
        self.failed_notifications: List[Dict[str, Any]] = []
        
        # Configuración
        self.config = NATSConfig.from_env()
        
        # Componentes de NATS
        self.consumer: Optional[DurableEventConsumer] = None
        self.publisher: Optional[EventPublisher] = None
        
        # Estado del servicio
        self.is_running = False
        self.startup_time = datetime.now(timezone.utc)
        
        # Logger
        self.logger = logging.getLogger("notification-service")
    
    async def start(self):
        """Iniciar el servicio de notificaciones."""
        try:
            self.logger.info("🚀 Iniciando servicio de notificaciones...")
            
            # Crear consumer
            self.consumer = DurableEventConsumer(
                service_name="notification-service",
                config=self.config
            )
            
            # Crear publisher para eventos salientes
            self.publisher = EventPublisher(
                service_name="notification-service", 
                config=self.config
            )
            
            # Registrar handlers de eventos
            self._register_event_handlers()
            
            # Conectar publisher
            await self.publisher.connect()
            self.logger.info("✅ Publisher conectado")
            
            # Iniciar consumer
            await self.consumer.start()
            self.logger.info("✅ Consumer iniciado")
            
            # Esperar sincronización
            self.logger.info("⏳ Esperando sincronización inicial...")
            while not self.consumer.is_synced:
                await asyncio.sleep(0.5)
                sync_status = self.consumer.get_sync_status()
                self.logger.info(f"📊 Sincronizando: {sync_status['events_processed']} eventos procesados")
            
            self.is_running = True
            self.logger.info("🎉 Servicio de notificaciones listo y sincronizado!")
            
        except Exception as e:
            self.logger.error(f"❌ Error iniciando servicio: {e}")
            await self.stop()
            raise
    
    def _register_event_handlers(self):
        """Registrar todos los handlers de eventos."""
        
        # Eventos de usuario
        self.consumer.register_handler("user.registered", self.handle_user_registered)
        self.consumer.register_handler("user.profile_updated", self.handle_user_profile_updated)
        self.consumer.register_handler("user.password_reset_requested", self.handle_password_reset)
        
        # Eventos de órdenes
        self.consumer.register_handler("order.created", self.handle_order_created)
        self.consumer.register_handler("order.confirmed", self.handle_order_confirmed)
        self.consumer.register_handler("order.shipped", self.handle_order_shipped)
        self.consumer.register_handler("order.delivered", self.handle_order_delivered)
        
        # Eventos de pago
        self.consumer.register_handler("payment.completed", self.handle_payment_completed)
        self.consumer.register_handler("payment.failed", self.handle_payment_failed)
        
        # Handler por defecto para eventos no manejados
        self.consumer.register_default_handler(self.handle_unknown_event)
        
        self.logger.info("📝 Handlers de eventos registrados")
    
    # =========================================================================
    # HANDLERS DE EVENTOS
    # =========================================================================
    
    async def handle_user_registered(self, event: Event):
        """Procesar registro de nuevo usuario."""
        try:
            user_data = event.data
            user_id = user_data["user_id"]
            email = user_data["email"]
            name = user_data.get("name", "Usuario")
            
            self.logger.info(f"👤 Procesando registro de usuario: {user_id}")
            
            # Enviar email de bienvenida
            notification_id = await self._send_email(
                user_id=user_id,
                email=email,
                template="welcome",
                data={
                    "name": name,
                    "welcome_bonus": 100,
                    "activation_link": f"https://app.ejemplo.com/activate/{user_id}"
                }
            )
            
            # Programar email de seguimiento
            await self._schedule_follow_up_email(user_id, email, name)
            
            # Actualizar estadísticas
            self.stats.total_processed += 1
            self.stats.emails_sent += 1
            self.stats.last_processed_at = datetime.now(timezone.utc).isoformat()
            
            self.logger.info(f"✅ Email de bienvenida enviado: {notification_id}")
            
        except Exception as e:
            await self._handle_processing_error(event, e, "user.registered")
    
    async def handle_user_profile_updated(self, event: Event):
        """Procesar actualización de perfil."""
        try:
            user_data = event.data
            user_id = user_data["user_id"]
            
            # Solo enviar notificación si cambió email o teléfono
            if "email" in user_data.get("updates", {}) or "phone" in user_data.get("updates", {}):
                await self._send_email(
                    user_id=user_id,
                    email=user_data["email"],
                    template="profile_updated",
                    data={"updates": user_data.get("updates", {})}
                )
                self.stats.emails_sent += 1
            
            self.stats.total_processed += 1
            self.stats.last_processed_at = datetime.now(timezone.utc).isoformat()
            
        except Exception as e:
            await self._handle_processing_error(event, e, "user.profile_updated")
    
    async def handle_password_reset(self, event: Event):
        """Procesar solicitud de reset de contraseña."""
        try:
            user_data = event.data
            user_id = user_data["user_id"]
            email = user_data["email"]
            reset_token = user_data["reset_token"]
            
            await self._send_email(
                user_id=user_id,
                email=email,
                template="password_reset",
                data={
                    "reset_link": f"https://app.ejemplo.com/reset/{reset_token}",
                    "expires_in": "24 horas"
                }
            )
            
            self.stats.total_processed += 1
            self.stats.emails_sent += 1
            self.stats.last_processed_at = datetime.now(timezone.utc).isoformat()
            
        except Exception as e:
            await self._handle_processing_error(event, e, "user.password_reset_requested")
    
    async def handle_order_created(self, event: Event):
        """Procesar creación de orden."""
        try:
            order_data = event.data
            order_id = order_data["order_id"]
            user_id = order_data["customer_id"]
            email = order_data["customer_email"]
            
            await self._send_email(
                user_id=user_id,
                email=email,
                template="order_confirmation",
                data={
                    "order_id": order_id,
                    "total_amount": order_data["total_amount"],
                    "items": order_data["items"],
                    "estimated_delivery": "3-5 días hábiles"
                }
            )
            
            self.stats.total_processed += 1
            self.stats.emails_sent += 1
            self.stats.last_processed_at = datetime.now(timezone.utc).isoformat()
            
        except Exception as e:
            await self._handle_processing_error(event, e, "order.created")
    
    async def handle_order_confirmed(self, event: Event):
        """Procesar confirmación de orden."""
        try:
            order_data = event.data
            
            # Enviar notificación push
            await self._send_push_notification(
                user_id=order_data["customer_id"],
                title="Orden Confirmada",
                message=f"Tu orden {order_data['order_id']} ha sido confirmada",
                data={"order_id": order_data["order_id"]}
            )
            
            self.stats.total_processed += 1
            self.stats.push_notifications_sent += 1
            self.stats.last_processed_at = datetime.now(timezone.utc).isoformat()
            
        except Exception as e:
            await self._handle_processing_error(event, e, "order.confirmed")
    
    async def handle_order_shipped(self, event: Event):
        """Procesar envío de orden."""
        try:
            order_data = event.data
            
            # Enviar email con tracking
            await self._send_email(
                user_id=order_data["customer_id"],
                email=order_data["customer_email"],
                template="order_shipped",
                data={
                    "order_id": order_data["order_id"],
                    "tracking_number": order_data.get("tracking_number"),
                    "carrier": order_data.get("carrier", "Courier"),
                    "tracking_url": f"https://track.ejemplo.com/{order_data.get('tracking_number')}"
                }
            )
            
            # Enviar SMS si el usuario tiene teléfono
            if order_data.get("customer_phone"):
                await self._send_sms(
                    user_id=order_data["customer_id"],
                    phone=order_data["customer_phone"],
                    message=f"Tu orden {order_data['order_id']} ha sido enviada. "
                           f"Tracking: {order_data.get('tracking_number')}"
                )
                self.stats.sms_sent += 1
            
            self.stats.total_processed += 1
            self.stats.emails_sent += 1
            self.stats.last_processed_at = datetime.now(timezone.utc).isoformat()
            
        except Exception as e:
            await self._handle_processing_error(event, e, "order.shipped")
    
    async def handle_order_delivered(self, event: Event):
        """Procesar entrega de orden."""
        try:
            order_data = event.data
            
            # Solicitar feedback
            await self._send_email(
                user_id=order_data["customer_id"],
                email=order_data["customer_email"],
                template="order_delivered_feedback",
                data={
                    "order_id": order_data["order_id"],
                    "feedback_url": f"https://app.ejemplo.com/feedback/{order_data['order_id']}"
                }
            )
            
            self.stats.total_processed += 1
            self.stats.emails_sent += 1
            self.stats.last_processed_at = datetime.now(timezone.utc).isoformat()
            
        except Exception as e:
            await self._handle_processing_error(event, e, "order.delivered")
    
    async def handle_payment_completed(self, event: Event):
        """Procesar pago completado."""
        try:
            payment_data = event.data
            order_id = payment_data["order_id"]
            amount = payment_data["amount"]
            
            # Enviar recibo por email
            await self._send_email(
                user_id=payment_data["customer_id"],
                email=payment_data["customer_email"],
                template="payment_receipt",
                data={
                    "order_id": order_id,
                    "amount": amount,
                    "payment_method": payment_data.get("payment_method"),
                    "transaction_id": payment_data.get("transaction_id")
                }
            )
            
            self.stats.total_processed += 1
            self.stats.emails_sent += 1
            self.stats.last_processed_at = datetime.now(timezone.utc).isoformat()
            
        except Exception as e:
            await self._handle_processing_error(event, e, "payment.completed")
    
    async def handle_payment_failed(self, event: Event):
        """Procesar pago fallido."""
        try:
            payment_data = event.data
            
            # Notificar fallo de pago
            await self._send_email(
                user_id=payment_data["customer_id"],
                email=payment_data["customer_email"],
                template="payment_failed",
                data={
                    "order_id": payment_data["order_id"],
                    "reason": payment_data.get("failure_reason", "Error desconocido"),
                    "retry_url": f"https://app.ejemplo.com/retry-payment/{payment_data['order_id']}"
                }
            )
            
            self.stats.total_processed += 1
            self.stats.emails_sent += 1
            self.stats.last_processed_at = datetime.now(timezone.utc).isoformat()
            
        except Exception as e:
            await self._handle_processing_error(event, e, "payment.failed")
    
    async def handle_unknown_event(self, event: Event):
        """Manejar eventos desconocidos."""
        self.logger.warning(f"⚠️  Evento no manejado: {event.event_type}")
        
        # Solo contar como procesado, no como error
        self.stats.total_processed += 1
        self.stats.last_processed_at = datetime.now(timezone.utc).isoformat()
    
    # =========================================================================
    # MÉTODOS DE ENVÍO DE NOTIFICACIONES
    # =========================================================================
    
    async def _send_email(self, user_id: str, email: str, template: str, data: Dict[str, Any]) -> str:
        """Simular envío de email."""
        notification_id = f"email-{datetime.now().strftime('%Y%m%d%H%M%S')}-{user_id}"
        
        # Simular envío (en producción, aquí irían proveedores como SendGrid, SES, etc.)
        await asyncio.sleep(0.1)  # Simular latencia de API
        
        notification_data = {
            "notification_id": notification_id,
            "type": "email",
            "user_id": user_id,
            "email": email,
            "template": template,
            "data": data,
            "status": "sent",
            "sent_at": datetime.now(timezone.utc).isoformat()
        }
        
        # Guardar en historial
        self.processed_notifications[notification_id] = notification_data
        
        # Emitir evento de notificación enviada
        await self.publisher.publish("notification.email.sent", notification_data)
        
        self.logger.info(f"📧 Email enviado: {template} -> {email}")
        return notification_id
    
    async def _send_sms(self, user_id: str, phone: str, message: str) -> str:
        """Simular envío de SMS."""
        notification_id = f"sms-{datetime.now().strftime('%Y%m%d%H%M%S')}-{user_id}"
        
        # Simular envío
        await asyncio.sleep(0.05)
        
        notification_data = {
            "notification_id": notification_id,
            "type": "sms",
            "user_id": user_id,
            "phone": phone,
            "message": message,
            "status": "sent",
            "sent_at": datetime.now(timezone.utc).isoformat()
        }
        
        self.processed_notifications[notification_id] = notification_data
        await self.publisher.publish("notification.sms.sent", notification_data)
        
        self.logger.info(f"📱 SMS enviado: {phone}")
        return notification_id
    
    async def _send_push_notification(self, user_id: str, title: str, message: str, data: Dict[str, Any]) -> str:
        """Simular envío de push notification."""
        notification_id = f"push-{datetime.now().strftime('%Y%m%d%H%M%S')}-{user_id}"
        
        # Simular envío
        await asyncio.sleep(0.02)
        
        notification_data = {
            "notification_id": notification_id,
            "type": "push",
            "user_id": user_id,
            "title": title,
            "message": message,
            "data": data,
            "status": "sent",
            "sent_at": datetime.now(timezone.utc).isoformat()
        }
        
        self.processed_notifications[notification_id] = notification_data
        await self.publisher.publish("notification.push.sent", notification_data)
        
        self.logger.info(f"🔔 Push notification enviada: {title}")
        return notification_id
    
    async def _schedule_follow_up_email(self, user_id: str, email: str, name: str):
        """Programar email de seguimiento."""
        # En producción, esto usaría un scheduler como Celery o similar
        await asyncio.sleep(1)  # Simular delay
        
        follow_up_data = {
            "user_id": user_id,
            "email": email,
            "name": name,
            "scheduled_for": "24 horas después del registro"
        }
        
        # Emitir evento para programar seguimiento
        await self.publisher.publish("notification.follow_up.scheduled", follow_up_data)
        
        self.logger.info(f"📅 Email de seguimiento programado para: {email}")
    
    async def _handle_processing_error(self, event: Event, error: Exception, event_type: str):
        """Manejar errores de procesamiento."""
        error_data = {
            "event_type": event_type,
            "event_id": event.metadata.event_id if event.metadata else "unknown",
            "error": str(error),
            "event_data": event.data,
            "failed_at": datetime.now(timezone.utc).isoformat()
        }
        
        self.failed_notifications.append(error_data)
        self.stats.total_failed += 1
        
        self.logger.error(f"❌ Error procesando {event_type}: {error}")
        
        # Emitir evento de error
        await self.publisher.publish("notification.processing.failed", error_data)
    
    # =========================================================================
    # MÉTODOS DE GESTIÓN DEL SERVICIO
    # =========================================================================
    
    async def send_manual_notification(self, request: NotificationRequest) -> str:
        """Enviar notificación manual através de API."""
        try:
            if request.type == "email":
                notification_id = await self._send_email(
                    user_id=request.user_id,
                    email=request.data["email"],
                    template=request.template,
                    data=request.data
                )
                self.stats.emails_sent += 1
            
            elif request.type == "sms":
                notification_id = await self._send_sms(
                    user_id=request.user_id,
                    phone=request.data["phone"],
                    message=request.data["message"]
                )
                self.stats.sms_sent += 1
            
            elif request.type == "push":
                notification_id = await self._send_push_notification(
                    user_id=request.user_id,
                    title=request.data["title"],
                    message=request.data["message"],
                    data=request.data.get("payload", {})
                )
                self.stats.push_notifications_sent += 1
            
            else:
                raise ValueError(f"Tipo de notificación no soportado: {request.type}")
            
            self.stats.total_processed += 1
            self.stats.last_processed_at = datetime.now(timezone.utc).isoformat()
            
            return notification_id
            
        except Exception as e:
            self.logger.error(f"❌ Error enviando notificación manual: {e}")
            raise
    
    def get_stats(self) -> Dict[str, Any]:
        """Obtener estadísticas del servicio."""
        uptime = (datetime.now(timezone.utc) - self.startup_time).total_seconds()
        
        return {
            "total_processed": self.stats.total_processed,
            "total_failed": self.stats.total_failed,
            "success_rate": (
                (self.stats.total_processed - self.stats.total_failed) / 
                max(self.stats.total_processed, 1)
            ),
            "emails_sent": self.stats.emails_sent,
            "sms_sent": self.stats.sms_sent,
            "push_notifications_sent": self.stats.push_notifications_sent,
            "last_processed_at": self.stats.last_processed_at,
            "service_started_at": self.stats.service_started_at,
            "uptime_seconds": uptime,
            "consumer_synced": self.consumer.is_synced if self.consumer else False,
            "consumer_state": self.consumer.state if self.consumer else "stopped",
            "notifications_in_history": len(self.processed_notifications),
            "failed_notifications": len(self.failed_notifications)
        }
    
    def get_health(self) -> Dict[str, Any]:
        """Health check del servicio."""
        uptime = (datetime.now(timezone.utc) - self.startup_time).total_seconds()
        
        # Determinar estado
        if not self.is_running:
            status = "unhealthy"
        elif self.consumer and not self.consumer.is_synced:
            status = "syncing"
        elif self.stats.total_failed > 0 and (
            self.stats.total_failed / max(self.stats.total_processed, 1) > 0.1
        ):
            status = "degraded"
        else:
            status = "healthy"
        
        return {
            "status": status,
            "service": "notification-service",
            "version": "1.0.0",
            "uptime_seconds": uptime,
            "consumer_synced": self.consumer.is_synced if self.consumer else False,
            "events_processed": self.stats.total_processed,
            "last_event": self.stats.last_processed_at
        }
    
    async def stop(self):
        """Parar el servicio gracefully."""
        self.logger.info("🛑 Deteniendo servicio de notificaciones...")
        
        self.is_running = False
        
        if self.consumer:
            await self.consumer.stop()
            self.logger.info("✅ Consumer detenido")
        
        if self.publisher:
            await self.publisher.disconnect()
            self.logger.info("✅ Publisher desconectado")
        
        self.logger.info("✅ Servicio de notificaciones detenido")


# =============================================================================
# FASTAPI APPLICATION
# =============================================================================

# Instancia global del servicio
notification_service = NotificationService()

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Manejar startup y shutdown del servicio."""
    # Startup
    try:
        await notification_service.start()
    except Exception as e:
        logging.error(f"Error iniciando servicio: {e}")
        raise
    
    yield
    
    # Shutdown
    await notification_service.stop()


# Crear aplicación FastAPI
app = FastAPI(
    title="Notification Service",
    description="Microservicio de notificaciones usando TC NATS Events",
    version="1.0.0",
    lifespan=lifespan
)


# =============================================================================
# API ENDPOINTS
# =============================================================================

@app.get("/health", response_model=HealthResponse)
async def health_check():
    """Health check del servicio."""
    health_data = notification_service.get_health()
    return HealthResponse(**health_data)


@app.get("/stats")
async def get_statistics():
    """Obtener estadísticas del servicio."""
    return notification_service.get_stats()


@app.get("/notifications")
async def get_notifications(limit: int = 100):
    """Obtener historial de notificaciones."""
    notifications = list(notification_service.processed_notifications.values())
    # Ordenar por fecha más reciente
    notifications.sort(key=lambda x: x["sent_at"], reverse=True)
    return {
        "total": len(notifications),
        "notifications": notifications[:limit]
    }


@app.get("/notifications/{notification_id}")
async def get_notification(notification_id: str):
    """Obtener detalles de una notificación específica."""
    if notification_id not in notification_service.processed_notifications:
        raise HTTPException(status_code=404, detail="Notificación no encontrada")
    
    return notification_service.processed_notifications[notification_id]


@app.get("/failures")
async def get_failures(limit: int = 50):
    """Obtener notificaciones fallidas."""
    failures = notification_service.failed_notifications[-limit:]
    return {
        "total_failures": len(notification_service.failed_notifications),
        "recent_failures": failures
    }


@app.post("/notifications/send", response_model=NotificationResponse)
async def send_notification(request: NotificationRequest):
    """Enviar notificación manual."""
    try:
        notification_id = await notification_service.send_manual_notification(request)
        
        return NotificationResponse(
            notification_id=notification_id,
            status="sent",
            message=f"Notificación {request.type} enviada exitosamente"
        )
    
    except Exception as e:
        raise HTTPException(
            status_code=500, 
            detail=f"Error enviando notificación: {str(e)}"
        )


@app.get("/consumer/status")
async def get_consumer_status():
    """Obtener estado del consumer de eventos."""
    if not notification_service.consumer:
        return {"status": "not_initialized"}
    
    sync_status = notification_service.consumer.get_sync_status()
    
    return {
        "consumer_state": notification_service.consumer.state,
        "is_synced": notification_service.consumer.is_synced,
        "sync_status": sync_status,
        "service_running": notification_service.is_running
    }


@app.get("/")
async def root():
    """Endpoint raíz con información del servicio."""
    return {
        "service": "notification-service",
        "description": "Microservicio de notificaciones usando TC NATS Events",
        "version": "1.0.0",
        "status": "running" if notification_service.is_running else "stopped",
        "endpoints": {
            "health": "/health",
            "stats": "/stats",
            "notifications": "/notifications",
            "send": "/notifications/send",
            "consumer_status": "/consumer/status"
        }
    }


# =============================================================================
# PUNTO DE ENTRADA
# =============================================================================

def setup_signal_handlers():
    """Configurar manejo de señales para graceful shutdown."""
    
    def signal_handler(signum, frame):
        logging.info(f"Señal recibida: {signum}. Iniciando shutdown graceful...")
        # El shutdown real se maneja en el lifespan de FastAPI
    
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)


if __name__ == "__main__":
    # Configurar logging
    setup_logging(
        level="INFO",
        structured=True,  # JSON logs para producción
        service_name="notification-service"
    )
    
    # Configurar señales
    setup_signal_handlers()
    
    # Ejecutar servidor
    uvicorn.run(
        "fastapi_microservice:app",
        host="0.0.0.0",
        port=8000,
        reload=False,  # No reload en producción
        access_log=True,
        log_config=None  # Usar el logging configurado
    )