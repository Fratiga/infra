package cl.duoc.rabbitmqadmin.config;

import java.util.List;
import java.util.stream.Stream;

import org.springframework.boot.context.properties.ConfigurationProperties;

// Nombres de exchanges, colas y rutas, leídos de application.properties
// (prefijo edututor.rabbit). Ningún otro lugar del servicio los escribe a mano.
@ConfigurationProperties(prefix = "edututor.rabbit")
public record RabbitNames(Exchange exchange, String dlqSuffix, Command email, Command session, Command certificate) {

	public record Exchange(String direct, String topic, String deadLetter) {
	}

	// Un caso de uso de mensajería: cola principal, rutas direct/topic y ruta
	// hacia su DLQ.
	public record Command(String queue, String directKey, String topicKey, String dlqKey) {
	}

	// Un binding de la topología base: exchange -> cola con su ruta.
	public record BindingBase(String exchange, String queue, String routingKey) {
	}

	public List<Command> commands() {
		return List.of(email, session, certificate);
	}

	public String dlqOf(Command command) {
		return command.queue() + dlqSuffix;
	}

	// Recursos que forman la topología base: la API no permite borrarlos.
	public List<String> managedQueues() {
		return commands().stream().flatMap(c -> Stream.of(c.queue(), dlqOf(c))).toList();
	}

	// Los bindings que declara RabbitTopologyConfig: la API no permite borrarlos.
	public List<BindingBase> managedBindings() {
		return commands().stream().flatMap(c -> Stream.of(
			new BindingBase(exchange.direct(), c.queue(), c.directKey()),
			new BindingBase(exchange.topic(), c.queue(), c.topicKey()),
			new BindingBase(exchange.deadLetter(), dlqOf(c), c.dlqKey()))).toList();
	}

	public List<String> managedExchanges() {
		return List.of(exchange.direct(), exchange.topic(), exchange.deadLetter());
	}
}
