package cl.duoc.kafkaadmin.loadtest;

import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.core.annotation.Order;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.stereotype.Component;

import cl.duoc.kafkaadmin.service.SolicitudInvalidaException;

// Consumidor deliberadamente lento: cada mensaje tarda delay-ms (como si guardara
// en una base de datos), así que con 1 hilo procesa como máximo 1000/delay-ms por
// segundo. Ante una ráfaga se queda atrás y aparece el LAG.
//
// Se arregla repartiendo el trabajo: el tópico tiene 3 particiones, de modo que
// con concurrency=3 cada hilo lee una y el LAG baja tres veces más rápido. Se puede
// cambiar en caliente con PUT /api/loadtest/consumer.
@Component
@Order(20)
@ConditionalOnProperty(prefix = "edututor.loadtest", name = "enabled", havingValue = "true")
public class LoadTestConsumer implements ApplicationRunner {

	static final String ID = "loadtest-consumer";
	private static final Logger log = LoggerFactory.getLogger(LoadTestConsumer.class);

	private final KafkaListenerEndpointRegistry registro;
	private final AtomicLong procesados = new AtomicLong();
	private volatile int retardoMs;
	private volatile int concurrencia;

	public LoadTestConsumer(KafkaListenerEndpointRegistry registro,
			@Value("${edututor.loadtest.consumer-delay-ms:10}") int retardoMs,
			@Value("${edututor.loadtest.consumer-concurrency:1}") int concurrencia) {
		this.registro = registro;
		this.retardoMs = retardoMs;
		this.concurrencia = concurrencia;
	}

	// autoStartup=false: se arranca después de que kafka-admin haya creado el tópico
	// (ver run), así no se queda esperando un tópico que todavía no existe.
	@KafkaListener(id = ID, topics = "${edututor.loadtest.topic:sessions.loadtest}",
			groupId = "${edututor.loadtest.group:loadtest-analytics-v1}",
			concurrency = "${edututor.loadtest.consumer-concurrency:1}", autoStartup = "false",
			properties = { "auto.offset.reset=earliest", "metadata.max.age.ms=10000" })
	public void escuchar(String mensaje) {
		if (retardoMs > 0) {
			try {
				Thread.sleep(retardoMs);
			}
			catch (InterruptedException e) {
				Thread.currentThread().interrupt();
			}
		}
		procesados.incrementAndGet();
	}

	// Corre después del aprovisionamiento de tópicos (TopicProvisioner, @Order(10)).
	@Override
	public void run(ApplicationArguments args) {
		MessageListenerContainer contenedor = registro.getListenerContainer(ID);
		if (contenedor != null && !contenedor.isRunning()) {
			contenedor.start();
			log.info("Consumidor lento iniciado: {} ms por mensaje, {} hilo(s)", retardoMs, concurrencia);
		}
	}

	public synchronized void configurar(Integer nuevaConcurrencia, Integer nuevoRetardoMs) {
		if (nuevaConcurrencia != null && (nuevaConcurrencia < 1 || nuevaConcurrencia > 6)) {
			throw new SolicitudInvalidaException("concurrency debe estar entre 1 y 6 (más hilos que particiones no ayuda)");
		}
		if (nuevoRetardoMs != null && (nuevoRetardoMs < 0 || nuevoRetardoMs > 5_000)) {
			throw new SolicitudInvalidaException("delayMs debe estar entre 0 y 5000");
		}
		if (nuevoRetardoMs != null) {
			this.retardoMs = nuevoRetardoMs;
		}
		if (nuevaConcurrencia != null && nuevaConcurrencia != concurrencia) {
			MessageListenerContainer contenedor = registro.getListenerContainer(ID);
			if (contenedor instanceof ConcurrentMessageListenerContainer<?, ?> concurrente) {
				// reinicio breve: el grupo se reparte las particiones entre los hilos nuevos
				concurrente.stop();
				concurrente.setConcurrency(nuevaConcurrencia);
				concurrente.start();
			}
			this.concurrencia = nuevaConcurrencia;
		}
		log.info("Consumidor lento reconfigurado: {} ms por mensaje, {} hilo(s)", retardoMs, concurrencia);
	}

	public long procesados() {
		return procesados.get();
	}

	public int retardoMs() {
		return retardoMs;
	}

	public int concurrencia() {
		return concurrencia;
	}
}
