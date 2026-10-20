package cl.duoc.rabbitmqadmin.web.dto;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Pattern;
import jakarta.validation.constraints.Positive;

public record QueueRequest(
		@NotBlank(message = "el nombre es obligatorio")
		@Pattern(regexp = NombreRecurso.PATRON, message = NombreRecurso.MENSAJE)
		String name,

		// Si no se envía, la cola es durable.
		Boolean durable,

		@Positive(message = "el TTL debe ser mayor que 0 (milisegundos)")
		Long messageTtlMs,

		@Pattern(regexp = NombreRecurso.PATRON, message = NombreRecurso.MENSAJE)
		String deadLetterExchange) {

	public boolean isDurable() {
		return durable == null || durable;
	}
}
