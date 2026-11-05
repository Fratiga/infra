package cl.duoc.kafkaadmin.web.dto;

import java.util.Map;

import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Pattern;

public record TopicRequest(
		@NotBlank(message = "el nombre es obligatorio")
		@Pattern(regexp = NombreTopico.PATRON, message = NombreTopico.MENSAJE)
		String name,

		@NotNull(message = "las particiones son obligatorias")
		@Min(value = 1, message = "debe haber al menos 1 partición")
		@Max(value = 100, message = "máximo 100 particiones")
		Integer partitions,

		@NotNull(message = "el factor de replicación es obligatorio")
		@Min(value = 1, message = "el factor de replicación mínimo es 1")
		@Max(value = 10, message = "el factor de replicación máximo es 10")
		Integer replicationFactor,

		// retention.ms, cleanup.policy, min.insync.replicas, etc. (ver TopicConfigRules)
		Map<String, String> configs) {

	public Map<String, String> configsOVacio() {
		return configs == null ? Map.of() : configs;
	}
}
