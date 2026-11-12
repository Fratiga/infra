package cl.duoc.kafkaadmin.service;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AlterConfigOp;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.NewPartitions;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.config.ConfigResource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;

import cl.duoc.kafkaadmin.config.KafkaTopicsProperties;
import cl.duoc.kafkaadmin.config.KafkaTopicsProperties.TopicSpec;

// Infraestructura como código: al arrancar deja el clúster como declara
// application.yml. Es idempotente: crea lo que falta y, en lo que ya existe,
// corrige las configuraciones que difieren y aumenta las particiones si la
// declaración pide más. Nunca borra nada ni reduce particiones.
@Component
@Order(10)
public class TopicProvisioner implements ApplicationRunner {

	private static final Logger log = LoggerFactory.getLogger(TopicProvisioner.class);
	private static final long TIMEOUT_SEGUNDOS = 15;
	private static final long ESPERA_REINTENTO_SEGUNDOS = 10;
	private static final int MAX_REINTENTOS = 30;

	private final Admin admin;
	private final KafkaTopicsProperties propiedades;

	public TopicProvisioner(Admin admin, KafkaTopicsProperties propiedades) {
		this.admin = admin;
		this.propiedades = propiedades;
	}

	@Override
	public void run(ApplicationArguments args) {
		if (!propiedades.provisionar()) {
			log.info("Aprovisionamiento de tópicos desactivado (edututor.kafka.provision-on-startup=false)");
			return;
		}
		try {
			provisionar();
		}
		catch (Exception e) {
			// Con Kafka caído (o todavía arrancando, como pasa al levantar el
			// clúster completo con compose) la API igual debe levantar: los
			// endpoints responden 502 y el aprovisionamiento se reintenta solo.
			log.warn("No se pudo aprovisionar los tópicos todavía ({}). Se reintenta cada {} s.", e.toString(),
				ESPERA_REINTENTO_SEGUNDOS);
			reintentarEnSegundoPlano();
		}
	}

	private void reintentarEnSegundoPlano() {
		Thread hilo = new Thread(() -> {
			for (int intento = 1; intento <= MAX_REINTENTOS; intento++) {
				try {
					TimeUnit.SECONDS.sleep(ESPERA_REINTENTO_SEGUNDOS);
					provisionar();
					log.info("Tópicos aprovisionados en el reintento {}", intento);
					return;
				}
				catch (InterruptedException e) {
					Thread.currentThread().interrupt();
					return;
				}
				catch (Exception e) {
					log.warn("Reintento {}/{} de aprovisionamiento fallido: {}", intento, MAX_REINTENTOS, e.toString());
				}
			}
			log.error("No se pudo aprovisionar los tópicos tras {} reintentos; reinicia el servicio cuando Kafka esté "
				+ "disponible.", MAX_REINTENTOS);
		}, "topic-provisioner-retry");
		hilo.setDaemon(true);
		hilo.start();
	}

	void provisionar() throws Exception {
		Set<String> existentes = admin.listTopics().names().get(TIMEOUT_SEGUNDOS, TimeUnit.SECONDS);
		for (TopicSpec spec : propiedades.topics()) {
			if (existentes.contains(spec.name())) {
				reconciliar(spec);
			}
			else {
				crear(spec);
			}
		}
	}

	private void crear(TopicSpec spec) throws Exception {
		admin.createTopics(List.of(new NewTopic(spec.name(), spec.partitions(), spec.replicationFactor())
			.configs(spec.configs()))).all().get(TIMEOUT_SEGUNDOS, TimeUnit.SECONDS);
		log.info("Tópico creado: {} ({} particiones, réplicas {}, configs {})", spec.name(), spec.partitions(),
			spec.replicationFactor(), spec.configs());
	}

	private void reconciliar(TopicSpec spec) throws Exception {
		var descripcion = admin.describeTopics(List.of(spec.name())).allTopicNames()
			.get(TIMEOUT_SEGUNDOS, TimeUnit.SECONDS).get(spec.name());
		int particiones = descripcion.partitions().size();
		int replicas = descripcion.partitions().get(0).replicas().size();

		if (spec.partitions() > particiones) {
			admin.createPartitions(Map.of(spec.name(), NewPartitions.increaseTo(spec.partitions())))
				.all().get(TIMEOUT_SEGUNDOS, TimeUnit.SECONDS);
			log.info("Tópico {}: particiones {} -> {}", spec.name(), particiones, spec.partitions());
		}
		else if (spec.partitions() < particiones) {
			log.warn("Tópico {} tiene {} particiones y la declaración pide {}: Kafka no permite reducirlas, se deja igual",
				spec.name(), particiones, spec.partitions());
		}
		if (spec.replicationFactor() != replicas) {
			log.warn("Tópico {} tiene factor de replicación {} y la declaración pide {}: cambiarlo requiere "
				+ "reasignar réplicas con kafka-reassign-partitions, no se hace por aquí",
				spec.name(), replicas, spec.replicationFactor());
		}

		ConfigResource recurso = new ConfigResource(ConfigResource.Type.TOPIC, spec.name());
		Config actual = admin.describeConfigs(List.of(recurso)).all().get(TIMEOUT_SEGUNDOS, TimeUnit.SECONDS).get(recurso);
		List<AlterConfigOp> cambios = new ArrayList<>();
		spec.configs().forEach((clave, valor) -> {
			ConfigEntry entrada = actual.get(clave);
			if (entrada == null || !valor.equals(entrada.value())) {
				cambios.add(new AlterConfigOp(new ConfigEntry(clave, valor), AlterConfigOp.OpType.SET));
			}
		});
		if (!cambios.isEmpty()) {
			admin.incrementalAlterConfigs(Map.of(recurso, cambios)).all().get(TIMEOUT_SEGUNDOS, TimeUnit.SECONDS);
			log.info("Tópico {}: configuración corregida ({})", spec.name(),
				cambios.stream().map(c -> c.configEntry().name() + "=" + c.configEntry().value()).toList());
		}
		else {
			log.info("Tópico {} ya está como declara la configuración", spec.name());
		}
	}
}
