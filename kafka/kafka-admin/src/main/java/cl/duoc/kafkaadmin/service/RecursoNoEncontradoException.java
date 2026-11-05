package cl.duoc.kafkaadmin.service;

public class RecursoNoEncontradoException extends RuntimeException {

	public RecursoNoEncontradoException(String mensaje) {
		super(mensaje);
	}
}
