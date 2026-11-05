package cl.duoc.kafkaadmin.web;

import java.net.URI;
import java.util.List;

import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import cl.duoc.kafkaadmin.service.KafkaAdminService;
import cl.duoc.kafkaadmin.web.dto.NombreTopico;
import cl.duoc.kafkaadmin.web.dto.PartitionsRequest;
import cl.duoc.kafkaadmin.web.dto.TopicConfigRequest;
import cl.duoc.kafkaadmin.web.dto.TopicDetalle;
import cl.duoc.kafkaadmin.web.dto.TopicRequest;
import cl.duoc.kafkaadmin.web.dto.TopicResumen;
import jakarta.validation.Valid;
import jakarta.validation.constraints.Pattern;

@RestController
@RequestMapping("/api/topics")
public class TopicController {

	private final KafkaAdminService service;

	public TopicController(KafkaAdminService service) {
		this.service = service;
	}

	// GET /api/topics?includeInternal=false
	@GetMapping
	public List<TopicResumen> listar(@RequestParam(defaultValue = "false") boolean includeInternal) {
		return service.listarTopicos(includeInternal);
	}

	// GET /api/topics/{name}: particiones (líder, réplicas, ISR, offsets) y configuración
	@GetMapping("/{name}")
	public TopicDetalle obtener(
			@PathVariable @Pattern(regexp = NombreTopico.PATRON, message = NombreTopico.MENSAJE) String name) {
		return service.obtenerTopico(name);
	}

	// POST /api/topics
	@PostMapping
	public ResponseEntity<TopicDetalle> crear(@Valid @RequestBody TopicRequest request) {
		TopicDetalle detalle = service.crearTopico(request);
		return ResponseEntity.created(URI.create("/api/topics/" + request.name())).body(detalle);
	}

	// PUT /api/topics/{name}/config con {"set": {"retention.ms": "86400000"}, "unset": ["segment.ms"]}
	@PutMapping("/{name}/config")
	public TopicDetalle actualizarConfiguracion(
			@PathVariable @Pattern(regexp = NombreTopico.PATRON, message = NombreTopico.MENSAJE) String name,
			@RequestBody TopicConfigRequest request) {
		return service.actualizarConfiguracion(name, request);
	}

	// PUT /api/topics/{name}/partitions con {"partitions": 6} (solo aumentar)
	@PutMapping("/{name}/partitions")
	public TopicDetalle aumentarParticiones(
			@PathVariable @Pattern(regexp = NombreTopico.PATRON, message = NombreTopico.MENSAJE) String name,
			@Valid @RequestBody PartitionsRequest request) {
		return service.aumentarParticiones(name, request.partitions());
	}

	// DELETE /api/topics/{name}: los tópicos gestionados por la configuración no se pueden borrar
	@DeleteMapping("/{name}")
	public ResponseEntity<Void> eliminar(
			@PathVariable @Pattern(regexp = NombreTopico.PATRON, message = NombreTopico.MENSAJE) String name) {
		service.eliminarTopico(name);
		return ResponseEntity.noContent().build();
	}
}
