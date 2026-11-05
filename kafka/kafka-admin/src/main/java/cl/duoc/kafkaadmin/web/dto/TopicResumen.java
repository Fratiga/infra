package cl.duoc.kafkaadmin.web.dto;

import java.util.Map;

public record TopicResumen(String name, int particiones, int factorReplicacion, boolean gestionado,
		Map<String, String> configs) {
}
