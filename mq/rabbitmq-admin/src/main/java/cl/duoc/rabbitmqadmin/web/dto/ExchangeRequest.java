package cl.duoc.rabbitmqadmin.web.dto;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Pattern;

public record ExchangeRequest(
		@NotBlank(message = "el nombre es obligatorio")
		@Pattern(regexp = NombreRecurso.PATRON, message = NombreRecurso.MENSAJE)
		String name,

		@NotNull(message = "el tipo es obligatorio (DIRECT, TOPIC o FANOUT)")
		ExchangeType type,

		Boolean durable) {

	public boolean isDurable() {
		return durable == null || durable;
	}
}
