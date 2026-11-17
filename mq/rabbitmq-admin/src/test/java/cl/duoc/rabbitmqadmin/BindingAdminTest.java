package cl.duoc.rabbitmqadmin;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.delete;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.amqp.core.Binding;
import org.springframework.amqp.core.QueueInformation;
import org.springframework.amqp.rabbit.core.ChannelCallback;
import org.springframework.amqp.rabbit.core.RabbitAdmin;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;

import cl.duoc.rabbitmqadmin.config.RabbitNames;
import cl.duoc.rabbitmqadmin.service.ConflictoException;
import cl.duoc.rabbitmqadmin.service.RabbitAdminService;
import cl.duoc.rabbitmqadmin.service.RecursoNoEncontradoException;
import cl.duoc.rabbitmqadmin.web.ApiExceptionHandler;
import cl.duoc.rabbitmqadmin.web.BindingController;
import cl.duoc.rabbitmqadmin.web.dto.BindingRequest;

// Eliminación de bindings, sin necesidad de un RabbitMQ levantado: el broker se
// reemplaza por un doble y se comprueba qué se le pide.
class BindingAdminTest {

	private static final RabbitNames NOMBRES = new RabbitNames(
		new RabbitNames.Exchange("cmd.direct", "cmd.topic", "cmd.dlx"), ".dlq",
		new RabbitNames.Command("notify.email", "email", "cmd.email.#", "email.dlq"),
		new RabbitNames.Command("notify.session", "session", "cmd.session.#", "session.dlq"),
		new RabbitNames.Command("notify.cert", "cert", "cmd.cert.#", "cert.dlq"));

	private RabbitAdmin rabbitAdmin;
	private RabbitAdminService servicio;
	private MockMvc mvc;

	@BeforeEach
	@SuppressWarnings("unchecked")
	void preparar() {
		rabbitAdmin = mock(RabbitAdmin.class);
		RabbitTemplate plantilla = mock(RabbitTemplate.class);
		when(rabbitAdmin.getRabbitTemplate()).thenReturn(plantilla);
		// el exchange existe
		when(plantilla.execute(any(ChannelCallback.class))).thenReturn(true);
		when(rabbitAdmin.getQueueInfo(any())).thenReturn(new QueueInformation("q", 0, 0));
		servicio = new RabbitAdminService(rabbitAdmin, NOMBRES);
		mvc = MockMvcBuilders.standaloneSetup(new BindingController(servicio))
			.setControllerAdvice(new ApiExceptionHandler()).build();
	}

	@Test
	void eliminaUnBindingPropioDelUsuario() {
		servicio.eliminarBinding(new BindingRequest("cola.extra", "cmd.direct", "ruta.extra"));

		verify(rabbitAdmin).removeBinding(any(Binding.class));
	}

	@Test
	void noPermiteQuitarLosBindingsDeLaTopologiaBase() {
		assertThatThrownBy(() -> servicio.eliminarBinding(new BindingRequest("notify.email", "cmd.direct", "email")))
			.isInstanceOf(ConflictoException.class).hasMessageContaining("topología base");
		assertThatThrownBy(() -> servicio.eliminarBinding(new BindingRequest("notify.session.dlq", "cmd.dlx", "session.dlq")))
			.isInstanceOf(ConflictoException.class);

		verify(rabbitAdmin, never()).removeBinding(any(Binding.class));
	}

	@Test
	void unaColaQueNoExisteDaNoEncontrado() {
		when(rabbitAdmin.getQueueInfo("fantasma")).thenReturn(null);

		assertThatThrownBy(() -> servicio.eliminarBinding(new BindingRequest("fantasma", "cmd.direct", "x")))
			.isInstanceOf(RecursoNoEncontradoException.class);
	}

	@Test
	void laTopologiaBaseTieneNueveBindings() {
		// por cada caso de uso: ruta direct, ruta topic y ruta a su DLQ
		assertThat(NOMBRES.managedBindings()).hasSize(9)
			.contains(new RabbitNames.BindingBase("cmd.topic", "notify.cert", "cmd.cert.#"));
	}

	@Test
	void elEndpointDevuelve204ConDatosValidos() throws Exception {
		mvc.perform(delete("/api/bindings").param("queue", "cola.extra").param("exchange", "cmd.direct")
				.param("routingKey", "ruta.extra"))
			.andExpect(status().isNoContent());
		verify(rabbitAdmin).removeBinding(any(Binding.class));
	}

	@Test
	void elEndpointDevuelve409ConUnBindingBase() throws Exception {
		mvc.perform(delete("/api/bindings").param("queue", "notify.email").param("exchange", "cmd.direct")
				.param("routingKey", "email"))
			.andExpect(status().isConflict());
	}

	@Test
	void elEndpointDevuelve400SiFaltaLaCola() throws Exception {
		mvc.perform(delete("/api/bindings").param("exchange", "cmd.direct").param("routingKey", "x"))
			.andExpect(status().isBadRequest());
	}
}
