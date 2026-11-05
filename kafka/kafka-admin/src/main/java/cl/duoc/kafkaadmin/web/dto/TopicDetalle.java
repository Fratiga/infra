package cl.duoc.kafkaadmin.web.dto;

import java.util.List;
import java.util.Map;

public record TopicDetalle(String name, boolean gestionado, long totalMensajes, Map<String, String> configs,
		List<Particion> particiones) {

	// offsetInicial..offsetFinal es el rango de mensajes que aún conserva la partición
	// (el inicial sube cuando la retención borra segmentos viejos).
	public record Particion(int particion, int lider, List<Integer> replicas, List<Integer> enSincronia,
			long offsetInicial, long offsetFinal) {
	}
}
