package cl.duoc.rabbitmqadmin.web;

import java.util.List;

import org.springframework.amqp.AmqpException;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.http.converter.HttpMessageNotReadableException;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;
import org.springframework.web.method.annotation.HandlerMethodValidationException;

import cl.duoc.rabbitmqadmin.service.ConflictoException;
import cl.duoc.rabbitmqadmin.service.RecursoNoEncontradoException;
import cl.duoc.rabbitmqadmin.web.dto.ErrorResponse;

@RestControllerAdvice
public class ApiExceptionHandler {

	@ExceptionHandler(MethodArgumentNotValidException.class)
	ResponseEntity<ErrorResponse> cuerpoInvalido(MethodArgumentNotValidException e) {
		List<String> detalles = e.getBindingResult().getFieldErrors().stream()
			.map(f -> f.getField() + ": " + f.getDefaultMessage())
			.sorted()
			.toList();
		return ResponseEntity.badRequest().body(new ErrorResponse("Datos inválidos", detalles));
	}

	@ExceptionHandler(HandlerMethodValidationException.class)
	ResponseEntity<ErrorResponse> parametroInvalido(HandlerMethodValidationException e) {
		List<String> detalles = e.getParameterValidationResults().stream()
			.flatMap(r -> r.getResolvableErrors().stream())
			.map(err -> "name: " + err.getDefaultMessage())
			.toList();
		return ResponseEntity.badRequest().body(new ErrorResponse("Parámetro inválido", detalles));
	}

	@ExceptionHandler(HttpMessageNotReadableException.class)
	ResponseEntity<ErrorResponse> jsonInvalido(HttpMessageNotReadableException e) {
		return ResponseEntity.badRequest()
			.body(ErrorResponse.of("El cuerpo de la petición no es un JSON válido o tiene un valor no permitido"));
	}

	@ExceptionHandler(RecursoNoEncontradoException.class)
	ResponseEntity<ErrorResponse> noEncontrado(RecursoNoEncontradoException e) {
		return ResponseEntity.status(HttpStatus.NOT_FOUND).body(ErrorResponse.of(e.getMessage()));
	}

	@ExceptionHandler(ConflictoException.class)
	ResponseEntity<ErrorResponse> conflicto(ConflictoException e) {
		return ResponseEntity.status(HttpStatus.CONFLICT).body(ErrorResponse.of(e.getMessage()));
	}

	@ExceptionHandler(AmqpException.class)
	ResponseEntity<ErrorResponse> brokerNoDisponible(AmqpException e) {
		return ResponseEntity.status(HttpStatus.BAD_GATEWAY)
			.body(ErrorResponse.of("RabbitMQ no respondió o rechazó la operación"));
	}
}
