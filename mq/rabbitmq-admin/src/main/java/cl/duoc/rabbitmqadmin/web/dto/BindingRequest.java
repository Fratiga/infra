package cl.duoc.rabbitmqadmin.web.dto;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Pattern;

public record BindingRequest(
		@NotBlank(message = "la cola es obligatoria")
		@Pattern(regexp = NombreRecurso.PATRON, message = NombreRecurso.MENSAJE)
		String queue,

		@NotBlank(message = "el exchange es obligatorio")
		@Pattern(regexp = NombreRecurso.PATRON, message = NombreRecurso.MENSAJE)
		String exchange,

		// Puede ir vacía (por ejemplo en un exchange fanout), pero no ausente.
		@NotNull(message = "la ruta es obligatoria (puede ir vacía)")
		@Pattern(regexp = "^[A-Za-z0-9._*#-]{0,255}$", message = "solo letras, números, punto, guion, guion bajo, * y #")
		String routingKey) {
}
