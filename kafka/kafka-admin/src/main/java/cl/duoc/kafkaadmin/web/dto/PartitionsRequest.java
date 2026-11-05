package cl.duoc.kafkaadmin.web.dto;

import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotNull;

public record PartitionsRequest(
		@NotNull(message = "el nuevo número de particiones es obligatorio")
		@Min(value = 1, message = "debe haber al menos 1 partición")
		@Max(value = 100, message = "máximo 100 particiones")
		Integer partitions) {
}
