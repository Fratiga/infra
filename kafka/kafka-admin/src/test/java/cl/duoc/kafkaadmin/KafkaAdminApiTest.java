package cl.duoc.kafkaadmin;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.delete;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.put;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import java.time.Duration;
import java.util.List;
import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledOnOs;
import org.junit.jupiter.api.condition.OS;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.MediaType;
import org.springframework.kafka.test.EmbeddedKafkaKraftBroker;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;
import org.springframework.web.context.WebApplicationContext;

// La API completa contra un broker Kafka real (embebido, sin Docker): el
// aprovisionamiento declarativo, la administración de tópicos y de consumer groups.
@SpringBootTest
class KafkaAdminApiTest {

	private static final EmbeddedKafkaKraftBroker BROKER = new EmbeddedKafkaKraftBroker(1, 3);

	static {
		BROKER.afterPropertiesSet();
	}

	@DynamicPropertySource
	static void propiedades(DynamicPropertyRegistry registry) {
		registry.add("spring.kafka.bootstrap-servers", BROKER::getBrokersAsString);
	}

	@Autowired
	WebApplicationContext contexto;

	MockMvc mvc;

	@BeforeEach
	void montar() {
		mvc = MockMvcBuilders.webAppContextSetup(contexto).build();
	}

	private static String json(String texto) {
		return texto.replace('\'', '"');
	}

	// ----------------------------------------------------------- aprovisionamiento

	@Test
	void alArrancarCreaLosTopicosDeclaradosConSusParametros() throws Exception {
		mvc.perform(get("/api/topics"))
			.andExpect(status().isOk())
			.andExpect(jsonPath("$[*].name", hasItem("sessions.events")))
			.andExpect(jsonPath("$[*].name", hasItem("sessions.alerts")))
			.andExpect(jsonPath("$[*].name", hasItem("sessions.events.DLT")))
			.andExpect(jsonPath("$[*].name", hasItem("sessions.alerts.DLT")));

		// sessions.events: 3 particiones, se borra por tiempo (7 días)
		mvc.perform(get("/api/topics/sessions.events"))
			.andExpect(status().isOk())
			.andExpect(jsonPath("$.gestionado", is(true)))
			.andExpect(jsonPath("$.particiones", hasSize(3)))
			.andExpect(jsonPath("$.configs['retention.ms']", is("604800000")))
			.andExpect(jsonPath("$.configs['cleanup.policy']", is("delete")));

		// sessions.alerts: compactado por key y acotado a 30 días
		mvc.perform(get("/api/topics/sessions.alerts"))
			.andExpect(status().isOk())
			.andExpect(jsonPath("$.configs['cleanup.policy']", is("compact,delete")))
			.andExpect(jsonPath("$.configs['retention.ms']", is("2592000000")));

		// los DLT conservan los mensajes 14 días
		mvc.perform(get("/api/topics/sessions.events.DLT"))
			.andExpect(jsonPath("$.configs['retention.ms']", is("1209600000")));
	}

	@Test
	void noListaLosTopicosInternosSalvoQueSePida() throws Exception {
		mvc.perform(get("/api/topics"))
			.andExpect(jsonPath("$[*].name", org.hamcrest.Matchers.not(hasItem(containsString("__")))));
		// __consumer_offsets solo existe después de que algún grupo confirma offsets,
		// así que aquí basta con que la opción se acepte y no oculte los demás.
		mvc.perform(get("/api/topics").param("includeInternal", "true"))
			.andExpect(status().isOk())
			.andExpect(jsonPath("$[*].name", hasItem("sessions.events")));
	}

	@Test
	void informaElClusterYSusBrokers() throws Exception {
		mvc.perform(get("/api/cluster"))
			.andExpect(status().isOk())
			.andExpect(jsonPath("$.brokers", hasSize(1)))
			.andExpect(jsonPath("$.clusterId").isNotEmpty());
	}

	// ---------------------------------------------------------------- tópicos

	@Test
	void creaUnTopicoConSusConfiguraciones() throws Exception {
		mvc.perform(post("/api/topics").contentType(MediaType.APPLICATION_JSON).content(json(
				"{'name':'t.nuevo','partitions':2,'replicationFactor':1,'configs':{'retention.ms':'3600000','cleanup.policy':'delete'}}")))
			.andExpect(status().isCreated())
			.andExpect(jsonPath("$.name", is("t.nuevo")))
			.andExpect(jsonPath("$.gestionado", is(false)))
			.andExpect(jsonPath("$.particiones", hasSize(2)))
			.andExpect(jsonPath("$.configs['retention.ms']", is("3600000")));
	}

	@Test
	void rechazaUnTopicoDuplicado() throws Exception {
		String cuerpo = json("{'name':'t.duplicado','partitions':1,'replicationFactor':1}");
		mvc.perform(post("/api/topics").contentType(MediaType.APPLICATION_JSON).content(cuerpo)).andExpect(status().isCreated());
		mvc.perform(post("/api/topics").contentType(MediaType.APPLICATION_JSON).content(cuerpo)).andExpect(status().isConflict());
	}

	@Test
	void rechazaFactorDeReplicacionMayorQueLosBrokers() throws Exception {
		mvc.perform(post("/api/topics").contentType(MediaType.APPLICATION_JSON)
				.content(json("{'name':'t.rf3','partitions':1,'replicationFactor':3}")))
			.andExpect(status().isBadRequest());
	}

	@Test
	void validaNombreParticionesYConfiguraciones() throws Exception {
		mvc.perform(post("/api/topics").contentType(MediaType.APPLICATION_JSON)
				.content(json("{'name':'__interno','partitions':1,'replicationFactor':1}")))
			.andExpect(status().isBadRequest());
		mvc.perform(post("/api/topics").contentType(MediaType.APPLICATION_JSON)
				.content(json("{'name':'ok','partitions':0,'replicationFactor':1}")))
			.andExpect(status().isBadRequest());
		mvc.perform(post("/api/topics").contentType(MediaType.APPLICATION_JSON)
				.content(json("{'name':'t.cfg1','partitions':1,'replicationFactor':1,'configs':{'log.dirs':'/tmp'}}")))
			.andExpect(status().isBadRequest())
			.andExpect(jsonPath("$.detalles[0]", containsString("no permitida")));
		mvc.perform(post("/api/topics").contentType(MediaType.APPLICATION_JSON)
				.content(json("{'name':'t.cfg2','partitions':1,'replicationFactor':1,'configs':{'cleanup.policy':'borrar'}}")))
			.andExpect(status().isBadRequest());
		mvc.perform(post("/api/topics").contentType(MediaType.APPLICATION_JSON)
				.content(json("{'name':'t.cfg3','partitions':1,'replicationFactor':1,'configs':{'min.insync.replicas':'2'}}")))
			.andExpect(status().isBadRequest())
			.andExpect(jsonPath("$.detalles[0]", containsString("factor de replicación")));
	}

	@Test
	void cambiaYRestauraLaConfiguracionDeUnTopico() throws Exception {
		mvc.perform(post("/api/topics").contentType(MediaType.APPLICATION_JSON)
				.content(json("{'name':'t.config','partitions':1,'replicationFactor':1,'configs':{'retention.ms':'1000000'}}")))
			.andExpect(status().isCreated());

		mvc.perform(put("/api/topics/t.config/config").contentType(MediaType.APPLICATION_JSON)
				.content(json("{'set':{'retention.ms':'86400000','cleanup.policy':'compact,delete'}}")))
			.andExpect(status().isOk())
			.andExpect(jsonPath("$.configs['retention.ms']", is("86400000")))
			.andExpect(jsonPath("$.configs['cleanup.policy']", is("compact,delete")));

		// unset: vuelve al valor por defecto del broker (7 días)
		mvc.perform(put("/api/topics/t.config/config").contentType(MediaType.APPLICATION_JSON)
				.content(json("{'unset':['retention.ms']}")))
			.andExpect(status().isOk())
			.andExpect(jsonPath("$.configs['retention.ms']", is("604800000")));

		mvc.perform(put("/api/topics/t.config/config").contentType(MediaType.APPLICATION_JSON).content("{}"))
			.andExpect(status().isBadRequest());
		mvc.perform(put("/api/topics/no.existe/config").contentType(MediaType.APPLICATION_JSON)
				.content(json("{'set':{'retention.ms':'1000'}}")))
			.andExpect(status().isNotFound());
	}

	@Test
	void aumentaLasParticionesPeroNoLasReduce() throws Exception {
		mvc.perform(post("/api/topics").contentType(MediaType.APPLICATION_JSON)
				.content(json("{'name':'t.part','partitions':2,'replicationFactor':1}")))
			.andExpect(status().isCreated());

		mvc.perform(put("/api/topics/t.part/partitions").contentType(MediaType.APPLICATION_JSON).content(json("{'partitions':4}")))
			.andExpect(status().isOk())
			.andExpect(jsonPath("$.particiones", hasSize(4)));
		mvc.perform(put("/api/topics/t.part/partitions").contentType(MediaType.APPLICATION_JSON).content(json("{'partitions':3}")))
			.andExpect(status().isBadRequest())
			.andExpect(jsonPath("$.error", containsString("solo pueden aumentar")));
	}

	// Borrar un tópico en un broker de prueba sobre Windows lo apaga: Kafka no puede
	// renombrar el directorio de log de la partición mientras está abierto. En
	// Linux (CI, Docker) funciona; ahí corre esta prueba.
	@Test
	@DisabledOnOs(value = OS.WINDOWS, disabledReason = "Kafka embebido no puede borrar tópicos en Windows")
	void eliminaUnTopicoQueNoEstaGestionado() throws Exception {
		mvc.perform(post("/api/topics").contentType(MediaType.APPLICATION_JSON)
				.content(json("{'name':'t.borrar','partitions':1,'replicationFactor':1}")))
			.andExpect(status().isCreated());
		mvc.perform(delete("/api/topics/t.borrar")).andExpect(status().isNoContent());
		mvc.perform(get("/api/topics/t.borrar")).andExpect(status().isNotFound());
		mvc.perform(delete("/api/topics/t.borrar")).andExpect(status().isNotFound());
	}

	@Test
	void protegeDelBorradoALosTopicosGestionadosYLosInternos() throws Exception {
		// declarado en application.yml: solo se cambia ahí
		mvc.perform(delete("/api/topics/sessions.events"))
			.andExpect(status().isConflict())
			.andExpect(jsonPath("$.error", containsString("gestionado")));
		mvc.perform(delete("/api/topics/sessions.alerts.DLT")).andExpect(status().isConflict());
		mvc.perform(delete("/api/topics/__consumer_offsets")).andExpect(status().isBadRequest());
		mvc.perform(delete("/api/topics/no.existe")).andExpect(status().isNotFound());
	}

	// ---------------------------------------------------------- consumer groups

	// Publica `cantidad` mensajes en el tópico y hace que `grupo` lea y confirme `leer` de ellos.
	private void prepararGrupo(String topico, String grupo, int cantidad, int leer) throws Exception {
		mvc.perform(post("/api/topics").contentType(MediaType.APPLICATION_JSON)
				.content(json("{'name':'" + topico + "','partitions':1,'replicationFactor':1}")))
			.andExpect(status().isCreated());

		try (KafkaProducer<String, String> productor = new KafkaProducer<>(Map.of(
				ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BROKER.getBrokersAsString(),
				ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class,
				ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class))) {
			for (int i = 0; i < cantidad; i++) {
				productor.send(new ProducerRecord<>(topico, "k", "v" + i)).get();
			}
		}
		try (KafkaConsumer<String, String> consumidor = new KafkaConsumer<>(Map.of(
				ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BROKER.getBrokersAsString(),
				ConsumerConfig.GROUP_ID_CONFIG, grupo,
				ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest",
				ConsumerConfig.MAX_POLL_RECORDS_CONFIG, leer,
				ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
				ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class))) {
			consumidor.subscribe(List.of(topico));
			int leidos = 0;
			long limite = System.currentTimeMillis() + 15_000;
			while (leidos < leer && System.currentTimeMillis() < limite) {
				leidos += consumidor.poll(Duration.ofMillis(500)).count();
			}
			consumidor.commitSync();
		}
	}

	@Test
	void listaYDescribeUnGrupoConSuLag() throws Exception {
		prepararGrupo("t.grupo", "g-lag", 5, 3);

		mvc.perform(get("/api/consumer-groups"))
			.andExpect(status().isOk())
			.andExpect(jsonPath("$[*].grupo", hasItem("g-lag")));

		mvc.perform(get("/api/consumer-groups/g-lag"))
			.andExpect(status().isOk())
			.andExpect(jsonPath("$.grupo", is("g-lag")))
			.andExpect(jsonPath("$.lagTotal", is(2)))
			.andExpect(jsonPath("$.particiones[0].topic", is("t.grupo")))
			.andExpect(jsonPath("$.particiones[0].offsetActual", is(3)))
			.andExpect(jsonPath("$.particiones[0].offsetFinal", is(5)));

		mvc.perform(get("/api/consumer-groups/no-existe")).andExpect(status().isNotFound());
	}

	@Test
	void reposicionaLosOffsetsDeUnGrupoInactivo() throws Exception {
		prepararGrupo("t.reset", "g-reset", 4, 4);

		mvc.perform(post("/api/consumer-groups/g-reset/offsets/reset").contentType(MediaType.APPLICATION_JSON)
				.content(json("{'topic':'t.reset','to':'EARLIEST'}")))
			.andExpect(status().isOk())
			.andExpect(jsonPath("$.lagTotal", is(4)));

		mvc.perform(post("/api/consumer-groups/g-reset/offsets/reset").contentType(MediaType.APPLICATION_JSON)
				.content(json("{'topic':'t.reset','to':'LATEST'}")))
			.andExpect(jsonPath("$.lagTotal", is(0)));

		mvc.perform(post("/api/consumer-groups/g-reset/offsets/reset").contentType(MediaType.APPLICATION_JSON)
				.content(json("{'topic':'t.reset','to':'OFFSET','offset':99}")))
			.andExpect(status().isBadRequest());
		mvc.perform(post("/api/consumer-groups/g-reset/offsets/reset").contentType(MediaType.APPLICATION_JSON)
				.content(json("{'topic':'t.reset','to':'OFFSET'}")))
			.andExpect(status().isBadRequest());
		mvc.perform(post("/api/consumer-groups/g-reset/offsets/reset").contentType(MediaType.APPLICATION_JSON)
				.content(json("{'topic':'t.reset','to':'MITAD'}")))
			.andExpect(status().isBadRequest());
	}

	@Test
	void eliminaUnGrupoVacio() throws Exception {
		prepararGrupo("t.delgrupo", "g-borrar", 2, 2);

		mvc.perform(delete("/api/consumer-groups/g-borrar")).andExpect(status().isNoContent());
		mvc.perform(get("/api/consumer-groups/g-borrar")).andExpect(status().isNotFound());
		mvc.perform(delete("/api/consumer-groups/g-borrar")).andExpect(status().isNotFound());
	}
}
