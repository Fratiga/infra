package cl.duoc.kafkaadmin.loadtest;

import java.util.List;
import java.util.Map;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import cl.duoc.kafkaadmin.loadtest.LoadTestService.Perfil;
import cl.duoc.kafkaadmin.loadtest.LoadTestService.Resultado;

// Pruebas de carga y tuning (guía 3.3.2). Solo existe con edututor.loadtest.enabled=true.
@RestController
@RequestMapping("/api/loadtest")
@ConditionalOnProperty(prefix = "edututor.loadtest", name = "enabled", havingValue = "true")
public class LoadTestController {

	public record ConsumidorRequest(Integer concurrency, Integer delayMs) {
	}

	private final LoadTestService servicio;
	private final LoadTestConsumer consumidor;
	private final String grupo;

	public LoadTestController(LoadTestService servicio, LoadTestConsumer consumidor,
			@Value("${edututor.loadtest.group:loadtest-analytics-v1}") String grupo) {
		this.servicio = servicio;
		this.consumidor = consumidor;
		this.grupo = grupo;
	}

	// POST /api/loadtest?count=100000&payloadBytes=200&perfil=COMPARAR
	// perfil: DEFAULT (valores de fábrica de Kafka), TUNED (los de EduTutor) o COMPARAR
	// (los dos, uno tras otro). batchSize, lingerMs y compression sobrescriben el perfil.
	@PostMapping
	public List<Resultado> lanzar(@RequestParam(defaultValue = "100000") int count,
			@RequestParam(defaultValue = "200") int payloadBytes,
			@RequestParam(defaultValue = "COMPARAR") Perfil perfil,
			@RequestParam(required = false) Integer batchSize, @RequestParam(required = false) Integer lingerMs,
			@RequestParam(required = false) String compression) {
		return servicio.ejecutar(count, payloadBytes, perfil, batchSize, lingerMs, compression);
	}

	@GetMapping
	public Map<String, Object> estado() {
		int hilos = consumidor.concurrencia();
		int retardo = consumidor.retardoMs();
		return Map.of(
			"topico", servicio.topico(),
			"grupo", grupo,
			"consumidorRetardoMs", retardo,
			"consumidorHilos", hilos,
			"consumidorMaxMensajesPorSegundo", retardo == 0 ? -1 : hilos * (1000 / retardo),
			"procesados", consumidor.procesados());
	}

	// PUT /api/loadtest/consumer {"concurrency":3,"delayMs":10}: cambia el consumidor en caliente.
	@PutMapping("/consumer")
	public Map<String, Object> configurarConsumidor(@RequestBody ConsumidorRequest solicitud) {
		consumidor.configurar(solicitud.concurrency(), solicitud.delayMs());
		return estado();
	}
}
