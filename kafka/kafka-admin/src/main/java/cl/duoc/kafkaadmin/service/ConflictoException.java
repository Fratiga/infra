package cl.duoc.kafkaadmin.service;

public class ConflictoException extends RuntimeException {

	public ConflictoException(String mensaje) {
		super(mensaje);
	}
}
