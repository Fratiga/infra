package cl.duoc.rabbitmqadmin.service;

public class ConflictoException extends RuntimeException {

	public ConflictoException(String mensaje) {
		super(mensaje);
	}
}
