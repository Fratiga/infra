package cl.duoc.rabbitmqadmin.web;

import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import cl.duoc.rabbitmqadmin.service.RabbitAdminService;
import cl.duoc.rabbitmqadmin.web.dto.BindingRequest;
import jakarta.validation.Valid;

@RestController
@RequestMapping("/api/bindings")
public class BindingController {

	private final RabbitAdminService service;

	public BindingController(RabbitAdminService service) {
		this.service = service;
	}

	// POST /api/bindings
	@PostMapping
	public ResponseEntity<BindingRequest> crear(@Valid @RequestBody BindingRequest request) {
		service.crearBinding(request);
		return ResponseEntity.status(HttpStatus.CREATED).body(request);
	}

	// DELETE /api/bindings?queue=...&exchange=...&routingKey=...
	// Los datos van en la URL porque un DELETE con cuerpo no lo respetan todos los clientes.
	@DeleteMapping
	public ResponseEntity<Void> eliminar(@Valid BindingRequest request) {
		service.eliminarBinding(request);
		return ResponseEntity.noContent().build();
	}
}
