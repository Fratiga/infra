package cl.duoc.kafkaadmin.service;

import java.util.List;

public class SolicitudInvalidaException extends RuntimeException {

	private final List<String> detalles;

	public SolicitudInvalidaException(String mensaje) {
		this(mensaje, List.of());
	}

	public SolicitudInvalidaException(String mensaje, List<String> detalles) {
		super(mensaje);
		this.detalles = detalles;
	}

	public List<String> getDetalles() {
		return detalles;
	}
}
