package cl.duoc.kafkaadmin.service;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AlterConfigOp;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.ConsumerGroupDescription;
import org.apache.kafka.clients.admin.ConsumerGroupListing;
import org.apache.kafka.clients.admin.ListOffsetsResult.ListOffsetsResultInfo;
import org.apache.kafka.clients.admin.MemberDescription;
import org.apache.kafka.clients.admin.NewPartitions;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.TopicPartitionInfo;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.errors.GroupIdNotFoundException;
import org.apache.kafka.common.errors.GroupNotEmptyException;
import org.apache.kafka.common.errors.InvalidConfigurationException;
import org.apache.kafka.common.errors.InvalidPartitionsException;
import org.apache.kafka.common.errors.InvalidReplicationFactorException;
import org.apache.kafka.common.errors.InvalidRequestException;
import org.apache.kafka.common.errors.InvalidTopicException;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import cl.duoc.kafkaadmin.config.KafkaTopicsProperties;
import cl.duoc.kafkaadmin.web.dto.ClusterInfo;
import cl.duoc.kafkaadmin.web.dto.GrupoDetalle;
import cl.duoc.kafkaadmin.web.dto.GrupoResumen;
import cl.duoc.kafkaadmin.web.dto.OffsetResetRequest;
import cl.duoc.kafkaadmin.web.dto.TopicConfigRequest;
import cl.duoc.kafkaadmin.web.dto.TopicDetalle;
import cl.duoc.kafkaadmin.web.dto.TopicRequest;
import cl.duoc.kafkaadmin.web.dto.TopicResumen;

// Operaciones de administración sobre el clúster: tópicos (crear, listar,
// describir, cambiar configuración, aumentar particiones, eliminar) y consumer
// groups (listar, describir con su LAG, reposicionar offsets, eliminar).
@Service
public class KafkaAdminService {

	private static final Logger log = LoggerFactory.getLogger(KafkaAdminService.class);
	private static final long TIMEOUT_SEGUNDOS = 10;

	private final Admin admin;
	private final KafkaTopicsProperties gestionados;

	public KafkaAdminService(Admin admin, KafkaTopicsProperties gestionados) {
		this.admin = admin;
		this.gestionados = gestionados;
	}

	// ------------------------------------------------------------------ cluster

	public ClusterInfo cluster() {
		var resultado = admin.describeCluster();
		String id = esperar(resultado.clusterId());
		Node controlador = esperar(resultado.controller());
		List<ClusterInfo.Broker> brokers = esperar(resultado.nodes()).stream()
			.sorted(Comparator.comparingInt(Node::id))
			.map(n -> new ClusterInfo.Broker(n.id(), n.host(), n.port(), n.rack()))
			.toList();
		return new ClusterInfo(id, controlador == null ? null : controlador.id(), brokers);
	}

	// ------------------------------------------------------------------ tópicos

	public List<TopicResumen> listarTopicos(boolean incluirInternos) {
		Set<String> nombres = esperar(admin.listTopics().names());
		List<String> visibles = nombres.stream()
			.filter(n -> incluirInternos || !n.startsWith("__"))
			.sorted()
			.toList();
		if (visibles.isEmpty()) {
			return List.of();
		}
		Map<String, TopicDescription> descripciones = esperar(admin.describeTopics(visibles).allTopicNames());
		Map<String, Map<String, String>> configs = configsDe(visibles);
		return visibles.stream().map(n -> {
			TopicDescription d = descripciones.get(n);
			int replicas = d.partitions().isEmpty() ? 0 : d.partitions().get(0).replicas().size();
			return new TopicResumen(n, d.partitions().size(), replicas, gestionados.esGestionado(n),
				configs.getOrDefault(n, Map.of()));
		}).toList();
	}

	public TopicDetalle obtenerTopico(String nombre) {
		TopicDescription d = describir(nombre);
		List<TopicPartition> tps = d.partitions().stream()
			.map(p -> new TopicPartition(nombre, p.partition())).toList();
		Map<TopicPartition, ListOffsetsResultInfo> inicio = offsets(tps, OffsetSpec.earliest());
		Map<TopicPartition, ListOffsetsResultInfo> fin = offsets(tps, OffsetSpec.latest());

		List<TopicDetalle.Particion> particiones = d.partitions().stream()
			.sorted(Comparator.comparingInt(TopicPartitionInfo::partition))
			.map(p -> {
				TopicPartition tp = new TopicPartition(nombre, p.partition());
				return new TopicDetalle.Particion(p.partition(), p.leader() == null ? -1 : p.leader().id(),
					p.replicas().stream().map(Node::id).toList(), p.isr().stream().map(Node::id).toList(),
					inicio.get(tp).offset(), fin.get(tp).offset());
			}).toList();
		long total = particiones.stream().mapToLong(p -> p.offsetFinal() - p.offsetInicial()).sum();
		return new TopicDetalle(nombre, gestionados.esGestionado(nombre), total,
			configsDe(List.of(nombre)).getOrDefault(nombre, Map.of()), particiones);
	}

	public TopicDetalle crearTopico(TopicRequest solicitud) {
		List<String> errores = TopicConfigRules.validar(solicitud.configsOVacio(), solicitud.replicationFactor());
		if (!errores.isEmpty()) {
			throw new SolicitudInvalidaException("Configuración de tópico inválida", errores);
		}
		NewTopic nuevo = new NewTopic(solicitud.name(), solicitud.partitions(), solicitud.replicationFactor().shortValue())
			.configs(solicitud.configsOVacio());
		esperar(admin.createTopics(List.of(nuevo)).all());
		log.info("Tópico creado: {} ({} particiones, réplicas {})", solicitud.name(), solicitud.partitions(),
			solicitud.replicationFactor());
		return obtenerCuandoEsteVisible(solicitud.name(), solicitud.partitions());
	}

	public TopicDetalle actualizarConfiguracion(String nombre, TopicConfigRequest solicitud) {
		Map<String, String> fijar = solicitud.setOVacio();
		List<String> quitar = solicitud.unsetOVacio();
		if (fijar.isEmpty() && quitar.isEmpty()) {
			throw new SolicitudInvalidaException("No hay nada que cambiar: envía 'set' o 'unset'");
		}
		TopicDescription actual = describir(nombre);
		int replicas = actual.partitions().get(0).replicas().size();
		List<String> errores = new ArrayList<>(TopicConfigRules.validar(fijar, replicas));
		quitar.stream().filter(k -> !TopicConfigRules.CLAVES.contains(k))
			.forEach(k -> errores.add(k + ": configuración no permitida"));
		if (!errores.isEmpty()) {
			throw new SolicitudInvalidaException("Configuración de tópico inválida", errores);
		}

		List<AlterConfigOp> operaciones = new ArrayList<>();
		fijar.forEach((k, v) -> operaciones.add(new AlterConfigOp(new ConfigEntry(k, v), AlterConfigOp.OpType.SET)));
		quitar.forEach(k -> operaciones.add(new AlterConfigOp(new ConfigEntry(k, null), AlterConfigOp.OpType.DELETE)));
		ConfigResource recurso = new ConfigResource(ConfigResource.Type.TOPIC, nombre);
		esperar(admin.incrementalAlterConfigs(Map.of(recurso, operaciones)).all());
		log.info("Configuración de {} actualizada: set={} unset={}", nombre, fijar.keySet(), quitar);
		return obtenerTopico(nombre);
	}

	public TopicDetalle aumentarParticiones(String nombre, int nuevoTotal) {
		int actual = describir(nombre).partitions().size();
		if (nuevoTotal <= actual) {
			throw new SolicitudInvalidaException("Las particiones solo pueden aumentar: ahora tiene " + actual
				+ " y se pidió " + nuevoTotal);
		}
		esperar(admin.createPartitions(Map.of(nombre, NewPartitions.increaseTo(nuevoTotal))).all());
		log.info("Tópico {}: particiones {} -> {}", nombre, actual, nuevoTotal);
		return obtenerCuandoEsteVisible(nombre, nuevoTotal);
	}

	// Kafka confirma la creación antes de que todos los brokers conozcan el cambio:
	// una consulta inmediata puede decir que el tópico no existe o mostrar las
	// particiones de antes. Se reintenta unos instantes hasta ver el estado nuevo.
	private TopicDetalle obtenerCuandoEsteVisible(String nombre, int particionesEsperadas) {
		RuntimeException ultimo = null;
		for (int intento = 0; intento < 20; intento++) {
			try {
				TopicDetalle detalle = obtenerTopico(nombre);
				if (detalle.particiones().size() >= particionesEsperadas) {
					return detalle;
				}
			}
			catch (RecursoNoEncontradoException | BrokerNoDisponibleException e) {
				ultimo = e;
			}
			try {
				Thread.sleep(250);
			}
			catch (InterruptedException e) {
				Thread.currentThread().interrupt();
				break;
			}
		}
		if (ultimo != null) {
			throw ultimo;
		}
		return obtenerTopico(nombre);
	}

	public void eliminarTopico(String nombre) {
		if (nombre.startsWith("__")) {
			throw new ConflictoException("'" + nombre + "' es un tópico interno de Kafka y no se puede eliminar");
		}
		if (gestionados.esGestionado(nombre)) {
			throw new ConflictoException("'" + nombre + "' está gestionado por la configuración de EduTutor "
				+ "(application.yml) y no se puede eliminar por la API");
		}
		describir(nombre);
		esperar(admin.deleteTopics(List.of(nombre)).all());
		log.info("Tópico eliminado: {}", nombre);
	}

	// ------------------------------------------------------------ consumer groups

	public List<GrupoResumen> listarGrupos() {
		Collection<ConsumerGroupListing> grupos = esperar(admin.listConsumerGroups().all());
		return grupos.stream()
			.map(g -> new GrupoResumen(g.groupId(), g.groupState().map(Enum::toString).orElse("UNKNOWN")))
			.sorted(Comparator.comparing(GrupoResumen::grupo))
			.toList();
	}

	public GrupoDetalle obtenerGrupo(String grupo) {
		ConsumerGroupDescription d = describirGrupo(grupo);

		Map<TopicPartition, OffsetAndMetadata> confirmados =
			esperar(admin.listConsumerGroupOffsets(grupo).partitionsToOffsetAndMetadata());
		Set<TopicPartition> particiones = new HashSet<>(confirmados.keySet());
		d.members().forEach(m -> particiones.addAll(m.assignment().topicPartitions()));

		Map<TopicPartition, ListOffsetsResultInfo> fin = offsets(List.copyOf(particiones), OffsetSpec.latest());
		List<GrupoDetalle.ParticionGrupo> filas = particiones.stream()
			.sorted(Comparator.comparing(TopicPartition::topic).thenComparingInt(TopicPartition::partition))
			.map(tp -> {
				OffsetAndMetadata c = confirmados.get(tp);
				long actual = c == null ? 0 : c.offset();
				long ultimo = fin.get(tp).offset();
				return new GrupoDetalle.ParticionGrupo(tp.topic(), tp.partition(), actual, ultimo, Math.max(0, ultimo - actual));
			}).toList();

		List<GrupoDetalle.Miembro> miembros = d.members().stream().map(this::miembro).toList();
		return new GrupoDetalle(grupo, d.groupState().toString(), d.partitionAssignor(),
			d.coordinator() == null ? null : d.coordinator().id(),
			filas.stream().mapToLong(GrupoDetalle.ParticionGrupo::lag).sum(), miembros, filas);
	}

	public GrupoDetalle reiniciarOffsets(String grupo, OffsetResetRequest solicitud) {
		ConsumerGroupDescription d = describirGrupo(grupo);
		if (!d.members().isEmpty()) {
			throw new ConflictoException("El grupo '" + grupo + "' tiene consumidores activos: detenlos antes de "
				+ "reposicionar sus offsets");
		}
		if (solicitud.to() == OffsetResetRequest.Destino.OFFSET && solicitud.offset() == null) {
			throw new SolicitudInvalidaException("Con destino OFFSET hay que indicar 'offset'");
		}

		TopicDescription topico = describir(solicitud.topic());
		List<TopicPartition> tps = topico.partitions().stream()
			.map(p -> new TopicPartition(solicitud.topic(), p.partition())).toList();
		Map<TopicPartition, ListOffsetsResultInfo> inicio = offsets(tps, OffsetSpec.earliest());
		Map<TopicPartition, ListOffsetsResultInfo> fin = offsets(tps, OffsetSpec.latest());

		Map<TopicPartition, OffsetAndMetadata> destino = new HashMap<>();
		List<String> errores = new ArrayList<>();
		for (TopicPartition tp : tps) {
			long minimo = inicio.get(tp).offset();
			long maximo = fin.get(tp).offset();
			long elegido = switch (solicitud.to()) {
				case EARLIEST -> minimo;
				case LATEST -> maximo;
				case OFFSET -> solicitud.offset();
			};
			if (elegido < minimo || elegido > maximo) {
				errores.add("partición " + tp.partition() + ": el offset " + elegido + " está fuera del rango ["
					+ minimo + ", " + maximo + "]");
			}
			destino.put(tp, new OffsetAndMetadata(elegido));
		}
		if (!errores.isEmpty()) {
			throw new SolicitudInvalidaException("Offset fuera de rango", errores);
		}
		esperar(admin.alterConsumerGroupOffsets(grupo, destino).all());
		log.info("Grupo {} reposicionado en {} a {}", grupo, solicitud.topic(), solicitud.to());
		return obtenerGrupo(grupo);
	}

	public void eliminarGrupo(String grupo) {
		describirGrupo(grupo);
		esperar(admin.deleteConsumerGroups(List.of(grupo)).all());
		log.info("Grupo eliminado: {}", grupo);
	}

	// ------------------------------------------------------------------ auxiliares

	private GrupoDetalle.Miembro miembro(MemberDescription m) {
		List<String> asignadas = m.assignment().topicPartitions().stream()
			.sorted(Comparator.comparing(TopicPartition::topic).thenComparingInt(TopicPartition::partition))
			.map(tp -> tp.topic() + "-" + tp.partition()).toList();
		return new GrupoDetalle.Miembro(m.consumerId(), m.clientId(), m.host(), asignadas);
	}

	private TopicDescription describir(String nombre) {
		return esperar(admin.describeTopics(List.of(nombre)).allTopicNames()).get(nombre);
	}

	private ConsumerGroupDescription describirGrupo(String grupo) {
		ConsumerGroupDescription d = esperar(admin.describeConsumerGroups(List.of(grupo)).all()).get(grupo);
		// Un grupo que nunca existió responde como un grupo DEAD sin miembros.
		if (d == null || "DEAD".equals(d.groupState().toString())) {
			throw new RecursoNoEncontradoException("El grupo '" + grupo + "' no existe");
		}
		return d;
	}

	private Map<TopicPartition, ListOffsetsResultInfo> offsets(List<TopicPartition> tps, OffsetSpec spec) {
		if (tps.isEmpty()) {
			return Map.of();
		}
		Map<TopicPartition, OffsetSpec> pedido = new LinkedHashMap<>();
		tps.forEach(tp -> pedido.put(tp, spec));
		return esperar(admin.listOffsets(pedido).all());
	}

	private Map<String, Map<String, String>> configsDe(List<String> topicos) {
		List<ConfigResource> recursos = topicos.stream()
			.map(t -> new ConfigResource(ConfigResource.Type.TOPIC, t)).toList();
		Map<ConfigResource, Config> configs = esperar(admin.describeConfigs(recursos).all());
		Map<String, Map<String, String>> salida = new HashMap<>();
		configs.forEach((recurso, config) -> {
			Map<String, String> valores = new LinkedHashMap<>();
			for (String clave : TopicConfigRules.CLAVES) {
				ConfigEntry entrada = config.get(clave);
				if (entrada != null && entrada.value() != null) {
					valores.put(clave, entrada.value());
				}
			}
			salida.put(recurso.name(), valores);
		});
		return salida;
	}

	// Espera el resultado de Kafka y traduce sus errores a excepciones de la API.
	private <T> T esperar(KafkaFuture<T> futuro) {
		try {
			return futuro.get(TIMEOUT_SEGUNDOS, TimeUnit.SECONDS);
		}
		catch (ExecutionException e) {
			throw traducir(e.getCause());
		}
		catch (TimeoutException e) {
			throw new BrokerNoDisponibleException("Kafka no respondió a tiempo", e);
		}
		catch (InterruptedException e) {
			Thread.currentThread().interrupt();
			throw new BrokerNoDisponibleException("Consulta a Kafka interrumpida", e);
		}
	}

	private RuntimeException traducir(Throwable causa) {
		if (causa instanceof TopicExistsException) {
			return new ConflictoException("El tópico ya existe");
		}
		if (causa instanceof UnknownTopicOrPartitionException) {
			return new RecursoNoEncontradoException("El tópico no existe");
		}
		if (causa instanceof GroupIdNotFoundException) {
			return new RecursoNoEncontradoException("El grupo no existe");
		}
		if (causa instanceof GroupNotEmptyException) {
			return new ConflictoException("El grupo tiene consumidores activos");
		}
		if (causa instanceof InvalidReplicationFactorException || causa instanceof InvalidPartitionsException
				|| causa instanceof InvalidConfigurationException || causa instanceof InvalidRequestException
				|| causa instanceof InvalidTopicException) {
			return new SolicitudInvalidaException(causa.getMessage());
		}
		return new BrokerNoDisponibleException("Kafka rechazó la operación: " + causa.getClass().getSimpleName(), causa);
	}

	// Para el aprovisionamiento al arrancar.
	Set<String> nombresExistentes() {
		return esperar(admin.listTopics().names()).stream().collect(Collectors.toSet());
	}
}
