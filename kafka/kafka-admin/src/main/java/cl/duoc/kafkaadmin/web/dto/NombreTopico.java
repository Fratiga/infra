package cl.duoc.kafkaadmin.web.dto;

// Nombres de tópico válidos para Kafka (letras, dígitos, punto, guion y guion
// bajo) y sin el prefijo "__" que Kafka reserva para tópicos internos.
public final class NombreTopico {

	public static final String PATRON = "^(?!__)(?!\\.{1,2}$)[a-zA-Z0-9._-]{1,249}$";
	public static final String MENSAJE = "debe tener entre 1 y 249 caracteres (letras, dígitos, punto, guion o guion bajo) y no empezar con '__'";

	private NombreTopico() {
	}
}
