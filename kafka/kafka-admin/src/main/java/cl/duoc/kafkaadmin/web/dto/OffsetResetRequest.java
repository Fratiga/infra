package cl.duoc.kafkaadmin.web.dto;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.PositiveOrZero;
import jakarta.validation.constraints.Pattern;

// Reposiciona un grupo en TODAS las particiones de un tópico.
public record OffsetResetRequest(
		@NotBlank(message = "el tópico es obligatorio")
		@Pattern(regexp = NombreTopico.PATRON, message = NombreTopico.MENSAJE)
		String topic,

		@NotNull(message = "el destino es obligatorio (EARLIEST, LATEST u OFFSET)")
		Destino to,

		// Solo con to=OFFSET: el mismo offset en cada partición.
		@PositiveOrZero(message = "el offset no puede ser negativo")
		Long offset) {

	public enum Destino {
		EARLIEST, LATEST, OFFSET
	}
}
