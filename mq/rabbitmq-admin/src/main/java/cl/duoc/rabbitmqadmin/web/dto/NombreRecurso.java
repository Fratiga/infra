package cl.duoc.rabbitmqadmin.web.dto;

// Regla única para nombres de colas y exchanges: 1 a 200 caracteres simples y
// sin el prefijo "amq.", que RabbitMQ reserva para uso interno.
public final class NombreRecurso {

	public static final String PATRON = "^(?!amq\\.)[A-Za-z0-9._-]{1,200}$";
	public static final String MENSAJE = "debe tener entre 1 y 200 caracteres (letras, números, punto, guion y guion bajo) y no empezar con amq.";

	private NombreRecurso() {
	}
}
