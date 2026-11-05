package cl.duoc.kafkaadmin.config;

import java.util.List;
import java.util.Map;

import org.springframework.boot.context.properties.ConfigurationProperties;

// Lista de tópicos gestionados, tal como se declara en application.yml.
@ConfigurationProperties(prefix = "edututor.kafka")
public record KafkaTopicsProperties(Boolean provisionOnStartup, List<TopicSpec> topics) {

	public KafkaTopicsProperties {
		topics = topics == null ? List.of() : List.copyOf(topics);
	}

	public boolean provisionar() {
		return provisionOnStartup == null || provisionOnStartup;
	}

	public boolean esGestionado(String nombre) {
		return topics.stream().anyMatch(t -> t.name().equals(nombre));
	}

	public record TopicSpec(String name, int partitions, short replicationFactor, Map<String, String> configs) {

		public TopicSpec {
			configs = configs == null ? Map.of() : Map.copyOf(configs);
		}
	}
}
