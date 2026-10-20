package cl.duoc.rabbitmqadmin.service;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.AmqpException;
import org.springframework.amqp.core.Binding;
import org.springframework.amqp.core.Binding.DestinationType;
import org.springframework.amqp.core.DirectExchange;
import org.springframework.amqp.core.Exchange;
import org.springframework.amqp.core.FanoutExchange;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.core.QueueInformation;
import org.springframework.amqp.core.TopicExchange;
import org.springframework.amqp.rabbit.core.RabbitAdmin;
import org.springframework.stereotype.Service;

import cl.duoc.rabbitmqadmin.config.RabbitNames;
import cl.duoc.rabbitmqadmin.web.dto.BindingRequest;
import cl.duoc.rabbitmqadmin.web.dto.ExchangeRequest;
import cl.duoc.rabbitmqadmin.web.dto.QueueRequest;

// Toda la lógica de administración de RabbitMQ vive aquí. Los controladores
// solo reciben la petición y llaman a estos métodos: no conocen la librería
// de RabbitMQ.
@Service
public class RabbitAdminService {

	private static final Logger log = LoggerFactory.getLogger(RabbitAdminService.class);

	private final RabbitAdmin rabbitAdmin;
	private final RabbitNames names;

	public RabbitAdminService(RabbitAdmin rabbitAdmin, RabbitNames names) {
		this.rabbitAdmin = rabbitAdmin;
		this.names = names;
	}

	public QueueInformation crearCola(QueueRequest request) {
		Map<String, Object> arguments = new HashMap<>();
		if (request.messageTtlMs() != null) {
			arguments.put("x-message-ttl", request.messageTtlMs());
		}
		if (request.deadLetterExchange() != null) {
			arguments.put("x-dead-letter-exchange", request.deadLetterExchange());
		}
		Queue queue = new Queue(request.name(), request.isDurable(), false, false, arguments);
		try {
			rabbitAdmin.declareQueue(queue);
		}
		catch (AmqpException e) {
			// RabbitMQ rechaza redeclarar una cola existente con otros argumentos.
			throw new ConflictoException("La cola '" + request.name() + "' ya existe con una configuración distinta");
		}
		log.info("Cola creada: {}", request.name());
		return obtenerCola(request.name());
	}

	public QueueInformation obtenerCola(String name) {
		QueueInformation info = rabbitAdmin.getQueueInfo(name);
		if (info == null) {
			throw new RecursoNoEncontradoException("La cola '" + name + "' no existe");
		}
		return info;
	}

	public void eliminarCola(String name) {
		if (names.managedQueues().contains(name)) {
			throw new ConflictoException("La cola '" + name + "' es parte de la topología base y no se puede eliminar");
		}
		obtenerCola(name);
		rabbitAdmin.deleteQueue(name);
		log.info("Cola eliminada: {}", name);
	}

	public int purgarCola(String name) {
		obtenerCola(name);
		int mensajes = rabbitAdmin.purgeQueue(name);
		log.info("Cola purgada: {} ({} mensajes)", name, mensajes);
		return mensajes;
	}

	public void crearExchange(ExchangeRequest request) {
		boolean durable = request.isDurable();
		Exchange exchange = switch (request.type()) {
			case DIRECT -> new DirectExchange(request.name(), durable, false);
			case TOPIC -> new TopicExchange(request.name(), durable, false);
			case FANOUT -> new FanoutExchange(request.name(), durable, false);
		};
		try {
			rabbitAdmin.declareExchange(exchange);
		}
		catch (AmqpException e) {
			throw new ConflictoException("El exchange '" + request.name() + "' ya existe con otro tipo o configuración");
		}
		log.info("Exchange creado: {} ({})", request.name(), request.type());
	}

	public void eliminarExchange(String name) {
		if (names.managedExchanges().contains(name)) {
			throw new ConflictoException("El exchange '" + name + "' es parte de la topología base y no se puede eliminar");
		}
		if (!existeExchange(name)) {
			throw new RecursoNoEncontradoException("El exchange '" + name + "' no existe");
		}
		rabbitAdmin.deleteExchange(name);
		log.info("Exchange eliminado: {}", name);
	}

	public void crearBinding(BindingRequest request) {
		obtenerCola(request.queue());
		if (!existeExchange(request.exchange())) {
			throw new RecursoNoEncontradoException("El exchange '" + request.exchange() + "' no existe");
		}
		rabbitAdmin.declareBinding(new Binding(request.queue(), DestinationType.QUEUE, request.exchange(),
			request.routingKey(), null));
		log.info("Binding creado: {} -> {} ({})", request.exchange(), request.queue(), request.routingKey());
	}

	private boolean existeExchange(String name) {
		return Boolean.TRUE.equals(rabbitAdmin.getRabbitTemplate().execute(channel -> {
			try {
				channel.exchangeDeclarePassive(name);
				return true;
			}
			catch (IOException e) {
				return false;
			}
		}));
	}
}
