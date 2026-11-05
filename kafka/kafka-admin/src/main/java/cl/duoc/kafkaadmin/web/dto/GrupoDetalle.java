package cl.duoc.kafkaadmin.web.dto;

import java.util.List;

public record GrupoDetalle(String grupo, String estado, String estrategiaAsignacion, Integer coordinador,
		long lagTotal, List<Miembro> miembros, List<ParticionGrupo> particiones) {

	public record Miembro(String idMiembro, String clientId, String host, List<String> particionesAsignadas) {
	}

	public record ParticionGrupo(String topic, int particion, long offsetActual, long offsetFinal, long lag) {
	}
}
