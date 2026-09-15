package cl.duoc.rabbitmqadmin;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.springframework.amqp.core.BindingBuilder;
import org.springframework.amqp.core.Declarable;
import org.springframework.amqp.core.Declarables;
import org.springframework.amqp.core.DirectExchange;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.core.QueueBuilder;
import org.springframework.amqp.core.TopicExchange;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

// Provisionamiento declarativo de la topología de RabbitMQ (infra-as-code):
// 3 exchanges, 3 colas de comando + sus 3 DLQ, con bindings direct y topic.
// Spring Boot declara todo esto en el broker automáticamente al arrancar.
@Configuration
public class RabbitTopologyConfig {

	public static final String CMD_DIRECT = "cmd.direct";
	public static final String CMD_TOPIC = "cmd.topic";
	public static final String CMD_DEAD_DLX = "cmd.dead.dlx";

	private record Cola(String nombre, String rutaDirect, String rutaTopic, String rutaDlq) {
	}

	private static final Cola[] COLAS = {
		new Cola("q.cmd.email", "email.send", "email.*", "email.dlq"),
		new Cola("q.cmd.session", "session.ticket", "session.#", "session.dlq"),
		new Cola("q.cmd.certificate", "certificate.gen", "certificate.*", "certificate.dlq"),
	};

	@Bean
	DirectExchange cmdDirectExchange() {
		return new DirectExchange(CMD_DIRECT, true, false);
	}

	@Bean
	TopicExchange cmdTopicExchange() {
		return new TopicExchange(CMD_TOPIC, true, false);
	}

	@Bean
	DirectExchange cmdDeadLetterExchange() {
		return new DirectExchange(CMD_DEAD_DLX, true, false);
	}

	@Bean
	Declarables colasYBindings() {
		List<Declarable> declarables = new ArrayList<>();

		for (Cola c : COLAS) {
			Queue principal = QueueBuilder.durable(c.nombre())
				.withArguments(Map.of(
					"x-dead-letter-exchange", CMD_DEAD_DLX,
					"x-dead-letter-routing-key", c.rutaDlq()))
				.build();
			Queue dlq = QueueBuilder.durable(c.nombre() + ".dlq").build();

			declarables.add(principal);
			declarables.add(dlq);
			declarables.add(BindingBuilder.bind(principal).to(new DirectExchange(CMD_DIRECT)).with(c.rutaDirect()));
			declarables.add(BindingBuilder.bind(principal).to(new TopicExchange(CMD_TOPIC)).with(c.rutaTopic()));
			declarables.add(BindingBuilder.bind(dlq).to(new DirectExchange(CMD_DEAD_DLX)).with(c.rutaDlq()));
		}

		return new Declarables(declarables);
	}
}
