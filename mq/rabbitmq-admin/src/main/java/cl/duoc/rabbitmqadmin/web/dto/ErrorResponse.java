package cl.duoc.rabbitmqadmin.web.dto;

import java.util.List;

public record ErrorResponse(String error, List<String> detalles) {

	public static ErrorResponse of(String error) {
		return new ErrorResponse(error, List.of());
	}
}
