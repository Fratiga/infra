package cl.duoc.rabbitmqadmin.web;

import java.net.URI;

import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import cl.duoc.rabbitmqadmin.service.RabbitAdminService;
import cl.duoc.rabbitmqadmin.web.dto.ExchangeRequest;
import cl.duoc.rabbitmqadmin.web.dto.NombreRecurso;
import jakarta.validation.Valid;
import jakarta.validation.constraints.Pattern;

@RestController
@RequestMapping("/api/exchanges")
public class ExchangeController {

	private final RabbitAdminService service;

	public ExchangeController(RabbitAdminService service) {
		this.service = service;
	}

	// POST /api/exchanges
	@PostMapping
	public ResponseEntity<ExchangeRequest> crear(@Valid @RequestBody ExchangeRequest request) {
		service.crearExchange(request);
		return ResponseEntity.created(URI.create("/api/exchanges/" + request.name())).body(request);
	}

	// DELETE /api/exchanges/{name}
	@DeleteMapping("/{name}")
	public ResponseEntity<Void> eliminar(
			@PathVariable @Pattern(regexp = NombreRecurso.PATRON, message = NombreRecurso.MENSAJE) String name) {
		service.eliminarExchange(name);
		return ResponseEntity.noContent().build();
	}
}
