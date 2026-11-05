package cl.duoc.kafkaadmin.service;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

// Qué configuraciones de tópico se pueden fijar por la API y con qué valores.
// Una lista cerrada evita que una petición cambie, por ejemplo, el log
// directory o parámetros internos del broker.
public final class TopicConfigRules {

	public static final String RETENTION_MS = "retention.ms";
	public static final String RETENTION_BYTES = "retention.bytes";
	public static final String CLEANUP_POLICY = "cleanup.policy";
	public static final String MIN_INSYNC_REPLICAS = "min.insync.replicas";
	public static final String MAX_MESSAGE_BYTES = "max.message.bytes";
	public static final String SEGMENT_MS = "segment.ms";
	public static final String SEGMENT_BYTES = "segment.bytes";
	public static final String COMPRESSION_TYPE = "compression.type";
	public static final String DELETE_RETENTION_MS = "delete.retention.ms";
	public static final String MIN_CLEANABLE_DIRTY_RATIO = "min.cleanable.dirty.ratio";

	// Las que se muestran siempre al describir un tópico, aunque estén con su valor por defecto.
	public static final List<String> CLAVES = List.of(RETENTION_MS, RETENTION_BYTES, CLEANUP_POLICY,
		MIN_INSYNC_REPLICAS, MAX_MESSAGE_BYTES, SEGMENT_MS, SEGMENT_BYTES, COMPRESSION_TYPE,
		DELETE_RETENTION_MS, MIN_CLEANABLE_DIRTY_RATIO);

	private static final Set<String> POLITICAS = Set.of("delete", "compact", "compact,delete", "delete,compact");
	private static final Set<String> COMPRESION = Set.of("producer", "uncompressed", "gzip", "snappy", "lz4", "zstd");

	private TopicConfigRules() {
	}

	// Devuelve la lista de errores (vacía si todo está bien).
	public static List<String> validar(Map<String, String> configs, Integer replicationFactor) {
		List<String> errores = new ArrayList<>();
		configs.forEach((clave, valor) -> {
			if (!CLAVES.contains(clave)) {
				errores.add(clave + ": configuración no permitida (permitidas: " + String.join(", ", CLAVES) + ")");
				return;
			}
			if (valor == null || valor.isBlank()) {
				errores.add(clave + ": el valor es obligatorio");
				return;
			}
			switch (clave) {
				case CLEANUP_POLICY -> {
					if (!POLITICAS.contains(valor.replace(" ", ""))) {
						errores.add(clave + ": debe ser delete, compact o compact,delete");
					}
				}
				case COMPRESSION_TYPE -> {
					if (!COMPRESION.contains(valor)) {
						errores.add(clave + ": debe ser uno de " + COMPRESION);
					}
				}
				case MIN_CLEANABLE_DIRTY_RATIO -> {
					try {
						double d = Double.parseDouble(valor);
						if (d < 0 || d > 1) {
							errores.add(clave + ": debe estar entre 0 y 1");
						}
					}
					catch (NumberFormatException e) {
						errores.add(clave + ": debe ser un número");
					}
				}
				case RETENTION_MS, RETENTION_BYTES -> numero(clave, valor, -1, errores);
				case MIN_INSYNC_REPLICAS -> {
					numero(clave, valor, 1, errores);
					if (replicationFactor != null && esEntero(valor) && Long.parseLong(valor) > replicationFactor) {
						errores.add(clave + ": no puede superar el factor de replicación (" + replicationFactor + ")");
					}
				}
				default -> numero(clave, valor, 0, errores);
			}
		});
		return errores;
	}

	private static void numero(String clave, String valor, long minimo, List<String> errores) {
		if (!esEntero(valor)) {
			errores.add(clave + ": debe ser un número entero");
		}
		else if (Long.parseLong(valor) < minimo) {
			errores.add(clave + ": debe ser mayor o igual que " + minimo);
		}
	}

	private static boolean esEntero(String valor) {
		try {
			Long.parseLong(valor);
			return true;
		}
		catch (NumberFormatException e) {
			return false;
		}
	}
}
