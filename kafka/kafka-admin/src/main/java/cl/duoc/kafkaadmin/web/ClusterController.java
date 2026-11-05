package cl.duoc.kafkaadmin.web;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import cl.duoc.kafkaadmin.service.KafkaAdminService;
import cl.duoc.kafkaadmin.web.dto.ClusterInfo;

@RestController
@RequestMapping("/api/cluster")
public class ClusterController {

	private final KafkaAdminService service;

	public ClusterController(KafkaAdminService service) {
		this.service = service;
	}

	// GET /api/cluster: id del clúster, controlador y brokers
	@GetMapping
	public ClusterInfo informacion() {
		return service.cluster();
	}
}
