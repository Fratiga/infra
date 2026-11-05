package cl.duoc.kafkaadmin.web;

import java.util.List;

import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import cl.duoc.kafkaadmin.service.KafkaAdminService;
import cl.duoc.kafkaadmin.web.dto.GrupoDetalle;
import cl.duoc.kafkaadmin.web.dto.GrupoResumen;
import cl.duoc.kafkaadmin.web.dto.OffsetResetRequest;
import jakarta.validation.Valid;
import jakarta.validation.constraints.Pattern;

@RestController
@RequestMapping("/api/consumer-groups")
public class ConsumerGroupController {

	private static final String GRUPO = "^[a-zA-Z0-9._-]{1,255}$";
	private static final String MENSAJE = "debe tener entre 1 y 255 caracteres (letras, dígitos, punto, guion o guion bajo)";

	private final KafkaAdminService service;

	public ConsumerGroupController(KafkaAdminService service) {
		this.service = service;
	}

	// GET /api/consumer-groups
	@GetMapping
	public List<GrupoResumen> listar() {
		return service.listarGrupos();
	}

	// GET /api/consumer-groups/{id}: miembros, particiones asignadas y LAG
	@GetMapping("/{id}")
	public GrupoDetalle obtener(@PathVariable @Pattern(regexp = GRUPO, message = MENSAJE) String id) {
		return service.obtenerGrupo(id);
	}

	// POST /api/consumer-groups/{id}/offsets/reset: solo si el grupo no tiene consumidores activos
	@PostMapping("/{id}/offsets/reset")
	public GrupoDetalle reiniciarOffsets(@PathVariable @Pattern(regexp = GRUPO, message = MENSAJE) String id,
			@Valid @RequestBody OffsetResetRequest request) {
		return service.reiniciarOffsets(id, request);
	}

	// DELETE /api/consumer-groups/{id}: solo si el grupo está vacío
	@DeleteMapping("/{id}")
	public ResponseEntity<Void> eliminar(@PathVariable @Pattern(regexp = GRUPO, message = MENSAJE) String id) {
		service.eliminarGrupo(id);
		return ResponseEntity.noContent().build();
	}
}
