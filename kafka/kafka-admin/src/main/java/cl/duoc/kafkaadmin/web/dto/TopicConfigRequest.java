package cl.duoc.kafkaadmin.web.dto;

import java.util.List;
import java.util.Map;

// Cambios de configuración de un tópico existente: valores a fijar y claves a
// volver a su valor por defecto del broker.
public record TopicConfigRequest(Map<String, String> set, List<String> unset) {

	public Map<String, String> setOVacio() {
		return set == null ? Map.of() : set;
	}

	public List<String> unsetOVacio() {
		return unset == null ? List.of() : unset;
	}
}
