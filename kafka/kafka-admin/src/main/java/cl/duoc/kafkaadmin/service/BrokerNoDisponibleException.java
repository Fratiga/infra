package cl.duoc.kafkaadmin.service;

public class BrokerNoDisponibleException extends RuntimeException {

	public BrokerNoDisponibleException(String mensaje, Throwable causa) {
		super(mensaje, causa);
	}
}
