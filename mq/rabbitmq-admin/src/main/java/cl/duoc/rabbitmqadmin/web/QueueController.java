package cl.duoc.rabbitmqadmin.web;

import java.net.URI;
import java.util.Map;

import org.springframework.amqp.core.QueueInformation;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import cl.duoc.rabbitmqadmin.service.RabbitAdminService;
import cl.duoc.rabbitmqadmin.web.dto.NombreRecurso;
import cl.duoc.rabbitmqadmin.web.dto.QueueRequest;
import jakarta.validation.Valid;
import jakarta.validation.constraints.Pattern;

@RestController
@RequestMapping("/api/queues")
public class QueueController {

	private final RabbitAdminService service;

	public QueueController(RabbitAdminService service) {
		this.service = service;
	}

	// POST /api/queues
	@PostMapping
	public ResponseEntity<QueueInformation> crear(@Valid @RequestBody QueueRequest request) {
		QueueInformation info = service.crearCola(request);
		return ResponseEntity.created(URI.create("/api/queues/" + request.name())).body(info);
	}

	// GET /api/queues/{name}
	@GetMapping("/{name}")
	public QueueInformation obtener(
			@PathVariable @Pattern(regexp = NombreRecurso.PATRON, message = NombreRecurso.MENSAJE) String name) {
		return service.obtenerCola(name);
	}

	// DELETE /api/queues/{name}
	@DeleteMapping("/{name}")
	public ResponseEntity<Void> eliminar(
			@PathVariable @Pattern(regexp = NombreRecurso.PATRON, message = NombreRecurso.MENSAJE) String name) {
		service.eliminarCola(name);
		return ResponseEntity.noContent().build();
	}

	// POST /api/queues/{name}/purge
	@PostMapping("/{name}/purge")
	public Map<String, Integer> purgar(
			@PathVariable @Pattern(regexp = NombreRecurso.PATRON, message = NombreRecurso.MENSAJE) String name) {
		return Map.of("mensajesEliminados", service.purgarCola(name));
	}
}
