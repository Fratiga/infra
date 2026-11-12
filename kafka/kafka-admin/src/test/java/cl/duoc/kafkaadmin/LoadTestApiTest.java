package cl.duoc.kafkaadmin;

import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.put;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.MediaType;
import org.springframework.kafka.test.EmbeddedKafkaKraftBroker;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;
import org.springframework.web.context.WebApplicationContext;
import org.junit.jupiter.api.BeforeEach;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

// Pruebas de carga contra un broker real: el tuning del productor tiene efecto
// medible y el consumidor lento se puede reconfigurar en caliente.
@SpringBootTest(properties = {
	"edututor.loadtest.enabled=true",
	"edututor.loadtest.consumer-delay-ms=1"
})
class LoadTestApiTest {

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
	void preparar() {
		mvc = MockMvcBuilders.webAppContextSetup(contexto).build();
	}

	private final ObjectMapper json = new ObjectMapper();

	@Test
	void elTuningAgrupaMasMensajesPorPeticionQueElValorDeFabrica() throws Exception {
		String cuerpo = mvc.perform(post("/api/loadtest?count=20000&payloadBytes=200&perfil=COMPARAR"))
			.andExpect(status().isOk())
			.andExpect(jsonPath("$.length()").value(2))
			.andReturn().getResponse().getContentAsString();

		JsonNode porDefecto = json.readTree(cuerpo).get(0);
		JsonNode ajustado = json.readTree(cuerpo).get(1);

		assertThat(porDefecto.get("perfil").asText()).isEqualTo("DEFAULT");
		assertThat(ajustado.get("perfil").asText()).isEqualTo("TUNED");
		assertThat(ajustado.get("config").get("batchSize").asInt()).isEqualTo(32_768);
		assertThat(ajustado.get("config").get("lingerMs").asInt()).isEqualTo(20);
		// ninguno perdió mensajes
		assertThat(porDefecto.get("errores").asInt()).isZero();
		assertThat(ajustado.get("errores").asInt()).isZero();
		// con linger.ms=20 y lotes de 32 KB cada petición lleva bastantes más mensajes
		double registrosDefault = porDefecto.get("metricas").get("registrosPorPeticion").asDouble();
		double registrosTuned = ajustado.get("metricas").get("registrosPorPeticion").asDouble();
		assertThat(registrosTuned).as("registros por petición: default %s, tuned %s", registrosDefault, registrosTuned)
			.isGreaterThan(registrosDefault);
		// y la compresión reduce lo que viaja por la red
		assertThat(ajustado.get("metricas").get("tasaCompresion").asDouble()).isLessThan(1.0);
	}

	@Test
	void rechazaCantidadesFueraDeRango() throws Exception {
		mvc.perform(post("/api/loadtest?count=0")).andExpect(status().isBadRequest());
		mvc.perform(post("/api/loadtest?count=100&payloadBytes=5")).andExpect(status().isBadRequest());
		mvc.perform(post("/api/loadtest?perfil=INVENTADO")).andExpect(status().isBadRequest());
	}

	@Test
	void elConsumidorLentoSeReconfiguraEnCaliente() throws Exception {
		mvc.perform(get("/api/loadtest"))
			.andExpect(status().isOk())
			.andExpect(jsonPath("$.topico").value("sessions.loadtest"))
			.andExpect(jsonPath("$.grupo").value("loadtest-analytics-v1"))
			.andExpect(jsonPath("$.consumidorHilos").value(1));

		mvc.perform(put("/api/loadtest/consumer").contentType(MediaType.APPLICATION_JSON)
				.content("{\"concurrency\":3,\"delayMs\":2}"))
			.andExpect(status().isOk())
			.andExpect(jsonPath("$.consumidorHilos").value(3))
			.andExpect(jsonPath("$.consumidorRetardoMs").value(2));

		mvc.perform(put("/api/loadtest/consumer").contentType(MediaType.APPLICATION_JSON)
				.content("{\"concurrency\":99}"))
			.andExpect(status().isBadRequest());
	}

	@Test
	void elTopicoDeCargaTieneRetencionAgresivaYElDeEventosUnTopeDeDisco() throws Exception {
		mvc.perform(get("/api/topics/sessions.loadtest"))
			.andExpect(status().isOk())
			.andExpect(jsonPath("$.configs['retention.ms']").value("300000"))
			.andExpect(jsonPath("$.configs['retention.bytes']").value("52428800"))
			.andExpect(jsonPath("$.configs['segment.ms']").value("60000"));
		mvc.perform(get("/api/topics/sessions.events"))
			.andExpect(status().isOk())
			.andExpect(jsonPath("$.configs['retention.bytes']").value("1073741824"));
	}
}
