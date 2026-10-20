package cl.duoc.rabbitmqadmin.config;

import java.util.List;
import java.util.Map;

import org.springframework.amqp.core.Binding;
import org.springframework.amqp.core.BindingBuilder;
import org.springframework.amqp.core.Declarable;
import org.springframework.amqp.core.Declarables;
import org.springframework.amqp.core.DirectExchange;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.core.QueueBuilder;
import org.springframework.amqp.core.TopicExchange;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

// Topología de EduTutor (infra-as-code): 3 exchanges y, por cada caso de uso
// (email, ticket de sesión, constancia), su cola, su DLQ y sus bindings. Los
// nombres vienen de RabbitNames; aquí no hay textos sueltos.
@Configuration
public class RabbitTopologyConfig {

	@Bean
	DirectExchange cmdDirectExchange(RabbitNames names) {
		return new DirectExchange(names.exchange().direct(), true, false);
	}

	@Bean
	TopicExchange cmdTopicExchange(RabbitNames names) {
		return new TopicExchange(names.exchange().topic(), true, false);
	}

	@Bean
	DirectExchange cmdDeadLetterExchange(RabbitNames names) {
		return new DirectExchange(names.exchange().deadLetter(), true, false);
	}

	@Bean
	Declarables emailTopology(RabbitNames names, @Qualifier("cmdDirectExchange") DirectExchange direct,
			TopicExchange topic, @Qualifier("cmdDeadLetterExchange") DirectExchange deadLetter) {
		return commandTopology(names, names.email(), direct, topic, deadLetter);
	}

	@Bean
	Declarables sessionTopology(RabbitNames names, @Qualifier("cmdDirectExchange") DirectExchange direct,
			TopicExchange topic, @Qualifier("cmdDeadLetterExchange") DirectExchange deadLetter) {
		return commandTopology(names, names.session(), direct, topic, deadLetter);
	}

	@Bean
	Declarables certificateTopology(RabbitNames names, @Qualifier("cmdDirectExchange") DirectExchange direct,
			TopicExchange topic, @Qualifier("cmdDeadLetterExchange") DirectExchange deadLetter) {
		return commandTopology(names, names.certificate(), direct, topic, deadLetter);
	}

	private Declarables commandTopology(RabbitNames names, RabbitNames.Command command, DirectExchange direct,
			TopicExchange topic, DirectExchange deadLetter) {
		Queue queue = QueueBuilder.durable(command.queue())
			.withArguments(Map.of(
				"x-dead-letter-exchange", names.exchange().deadLetter(),
				"x-dead-letter-routing-key", command.dlqKey()))
			.build();
		Queue dlq = QueueBuilder.durable(names.dlqOf(command)).build();

		Binding byDirect = BindingBuilder.bind(queue).to(direct).with(command.directKey());
		Binding byTopic = BindingBuilder.bind(queue).to(topic).with(command.topicKey());
		Binding toDlq = BindingBuilder.bind(dlq).to(deadLetter).with(command.dlqKey());

		return new Declarables(List.<Declarable>of(queue, dlq, byDirect, byTopic, toDlq));
	}
}
