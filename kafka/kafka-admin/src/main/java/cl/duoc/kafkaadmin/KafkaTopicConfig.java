package cl.duoc.kafkaadmin;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

// Provisionamiento declarativo de tópicos (infra-as-code), igual que
// rabbitmq-admin hace con exchanges/colas. El caso pide 3 particiones y
// factor de réplica 3 (para un clúster de 3 brokers); en este entorno de
// desarrollo con un solo broker, la réplica queda en 1 — la partición por
// sessionId (que hace el productor) no depende del factor de réplica.
@Configuration
public class KafkaTopicConfig {

	private static final int PARTICIONES = 3;
	private static final short REPLICAS = 1;

	@Bean
	NewTopic sessionsEvents() {
		return TopicBuilder.name("sessions.events")
			.partitions(PARTICIONES)
			.replicas(REPLICAS)
			.config("retention.ms", String.valueOf(7L * 24 * 60 * 60 * 1000)) // 7 días
			.config("cleanup.policy", "delete")
			.build();
	}

	@Bean
	NewTopic auditTimeline() {
		return TopicBuilder.name("audit.timeline")
			.partitions(PARTICIONES)
			.replicas(REPLICAS)
			.config("retention.ms", String.valueOf(30L * 24 * 60 * 60 * 1000)) // 30 días
			.config("cleanup.policy", "compact,delete")
			.build();
	}

	@Bean
	NewTopic sessionsEventsDlt() {
		return TopicBuilder.name("sessions.events.DLT")
			.partitions(PARTICIONES)
			.replicas(REPLICAS)
			.config("retention.ms", String.valueOf(14L * 24 * 60 * 60 * 1000)) // 14 días
			.build();
	}

	@Bean
	NewTopic auditTimelineDlt() {
		return TopicBuilder.name("audit.timeline.DLT")
			.partitions(PARTICIONES)
			.replicas(REPLICAS)
			.config("retention.ms", String.valueOf(14L * 24 * 60 * 60 * 1000))
			.build();
	}
}
