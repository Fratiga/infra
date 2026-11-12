package cl.duoc.kafkaadmin.loadtest;

import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.kafka.autoconfigure.KafkaProperties;
import org.springframework.stereotype.Service;

import cl.duoc.kafkaadmin.service.ConflictoException;
import cl.duoc.kafkaadmin.service.SolicitudInvalidaException;

// Generador de carga para medir el efecto del tuning del productor (guía 3.3.2).
// Escribe en un tópico propio (sessions.loadtest) y nunca en los de negocio, para
// no llenar de eventos falsos la auditoría ni las métricas.
//
// Solo existe si edututor.loadtest.enabled=true. Cada prueba envía N mensajes de
// forma asíncrona (como lo hace sessions en producción), espera a que Kafka los
// confirme y mide cuánto tardó y cómo se agruparon en lotes.
@Service
@ConditionalOnProperty(prefix = "edututor.loadtest", name = "enabled", havingValue = "true")
public class LoadTestService {

	private static final Logger log = LoggerFactory.getLogger(LoadTestService.class);

	static final int MAX_MENSAJES = 2_000_000;

	// Valores de fábrica de Kafka frente a los que usa EduTutor (ver sessions).
	public enum Perfil {
		DEFAULT(new Config(16_384, 0, "none")),
		TUNED(new Config(32_768, 20, "lz4")),
		COMPARAR(null);

		final Config config;

		Perfil(Config config) {
			this.config = config;
		}
	}

	public record Config(int batchSize, int lingerMs, String compresion) {
	}

	public record Metricas(double tamanoLoteMedioBytes, double registrosPorPeticion, double tasaCompresion,
			double peticionesPorSegundo) {
	}

	public record Resultado(String perfil, Config config, int mensajes, int bytesPorMensaje, int errores,
			long duracionMs, double mensajesPorSegundo, double mbPorSegundo, Metricas metricas) {
	}

	private final KafkaProperties kafkaProperties;
	private final String topico;
	private final AtomicBoolean enCurso = new AtomicBoolean(false);
	// Texto aleatorio del que se recortan los mensajes: así se comprimen como datos
	// reales y no como un mismo texto repetido.
	private final String reserva;

	public LoadTestService(KafkaProperties kafkaProperties,
			@Value("${edututor.loadtest.topic:sessions.loadtest}") String topico) {
		this.kafkaProperties = kafkaProperties;
		this.topico = topico;
		Random azar = new Random(42);
		String alfabeto = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789 ";
		StringBuilder sb = new StringBuilder(16_384);
		for (int i = 0; i < 16_384; i++) {
			sb.append(alfabeto.charAt(azar.nextInt(alfabeto.length())));
		}
		this.reserva = sb.toString();
	}

	public String topico() {
		return topico;
	}

	public List<Resultado> ejecutar(int mensajes, int bytesPorMensaje, Perfil perfil, Integer batchSize,
			Integer lingerMs, String compresion) {
		if (mensajes < 1 || mensajes > MAX_MENSAJES) {
			throw new SolicitudInvalidaException("count debe estar entre 1 y " + MAX_MENSAJES);
		}
		if (bytesPorMensaje < 10 || bytesPorMensaje > 10_000) {
			throw new SolicitudInvalidaException("payloadBytes debe estar entre 10 y 10000");
		}
		if (!enCurso.compareAndSet(false, true)) {
			throw new ConflictoException("Ya hay una prueba de carga en ejecución");
		}
		try {
			if (perfil == Perfil.COMPARAR) {
				// primero el valor de fábrica y luego el ajustado, en las mismas condiciones
				return List.of(
					correr(Perfil.DEFAULT.name(), Perfil.DEFAULT.config, mensajes, bytesPorMensaje),
					correr(Perfil.TUNED.name(), Perfil.TUNED.config, mensajes, bytesPorMensaje));
			}
			Config base = perfil.config;
			Config config = new Config(batchSize != null ? batchSize : base.batchSize(),
				lingerMs != null ? lingerMs : base.lingerMs(), compresion != null ? compresion : base.compresion());
			return List.of(correr(perfil.name(), config, mensajes, bytesPorMensaje));
		}
		finally {
			enCurso.set(false);
		}
	}

	private Resultado correr(String nombre, Config config, int mensajes, int bytes) {
		Map<String, Object> props = kafkaProperties.buildProducerProperties();
		props.put(ProducerConfig.CLIENT_ID_CONFIG, "loadtest-" + nombre.toLowerCase());
		props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
		props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
		// Lo mismo en ambos perfiles: solo cambia lo que se quiere medir.
		props.put(ProducerConfig.ACKS_CONFIG, "all");
		props.put(ProducerConfig.BATCH_SIZE_CONFIG, config.batchSize());
		props.put(ProducerConfig.LINGER_MS_CONFIG, config.lingerMs());
		props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, config.compresion());
		props.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, 30_000);

		AtomicInteger errores = new AtomicInteger();
		try (KafkaProducer<String, String> productor = new KafkaProducer<>(props)) {
			long inicio = System.nanoTime();
			for (int i = 0; i < mensajes; i++) {
				int desde = (i * 31) % (reserva.length() - bytes);
				String valor = i + ":" + reserva.substring(desde, desde + bytes);
				productor.send(new ProducerRecord<>(topico, "est-" + (i % 50), valor), (meta, error) -> {
					if (error != null) {
						errores.incrementAndGet();
					}
				});
			}
			productor.flush();
			long duracionMs = Math.max(1, (System.nanoTime() - inicio) / 1_000_000);

			double segundos = duracionMs / 1000.0;
			double mensajesPorSegundo = mensajes / segundos;
			double mb = (double) mensajes * bytes / (1024 * 1024);
			Metricas metricas = new Metricas(
				metrica(productor.metrics(), "batch-size-avg"),
				metrica(productor.metrics(), "records-per-request-avg"),
				metrica(productor.metrics(), "compression-rate-avg"),
				metrica(productor.metrics(), "request-rate"));
			log.info("Prueba de carga {}: {} mensajes de {} B en {} ms ({} msg/s), {} registros por petición, "
				+ "{} errores", nombre, mensajes, bytes, duracionMs, Math.round(mensajesPorSegundo),
				Math.round(metricas.registrosPorPeticion()), errores.get());
			return new Resultado(nombre, config, mensajes, bytes, errores.get(), duracionMs,
				redondear(mensajesPorSegundo), redondear(mb / segundos), metricas);
		}
	}

	private static double metrica(Map<MetricName, ? extends Metric> metricas, String nombre) {
		return metricas.entrySet().stream()
			.filter(e -> e.getKey().name().equals(nombre) && e.getKey().group().equals("producer-metrics"))
			.map(e -> e.getValue().metricValue())
			.filter(Double.class::isInstance)
			.map(Double.class::cast)
			.filter(d -> !d.isNaN() && !d.isInfinite())
			.mapToDouble(LoadTestService::redondear)
			.findFirst().orElse(0);
	}

	private static double redondear(double d) {
		return Math.round(d * 100.0) / 100.0;
	}
}
