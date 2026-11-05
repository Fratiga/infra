package cl.duoc.kafkaadmin;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;
import org.springframework.mock.web.MockFilterChain;
import org.springframework.mock.web.MockHttpServletRequest;
import org.springframework.mock.web.MockHttpServletResponse;

import cl.duoc.kafkaadmin.web.AdminTokenFilter;

class AdminTokenFilterTest {

	private MockHttpServletResponse llamar(String tokenConfigurado, String cabecera) throws Exception {
		MockHttpServletRequest request = new MockHttpServletRequest("DELETE", "/api/topics/x");
		if (cabecera != null) {
			request.addHeader("X-Admin-Token", cabecera);
		}
		MockHttpServletResponse response = new MockHttpServletResponse();
		MockFilterChain cadena = new MockFilterChain();
		new AdminTokenFilter(tokenConfigurado).doFilter(request, response, cadena);
		// si pasó, la cadena recibió la petición
		if (cadena.getRequest() != null) {
			response.setStatus(200);
		}
		return response;
	}

	@Test
	void sinTokenConfiguradoNoExigeNada() throws Exception {
		assertThat(llamar("", null).getStatus()).isEqualTo(200);
	}

	@Test
	void conTokenConfiguradoRechazaSinCabecera() throws Exception {
		assertThat(llamar("secreto", null).getStatus()).isEqualTo(401);
	}

	@Test
	void conTokenConfiguradoRechazaUnValorIncorrecto() throws Exception {
		assertThat(llamar("secreto", "otro").getStatus()).isEqualTo(401);
	}

	@Test
	void conTokenConfiguradoAceptaElValorCorrecto() throws Exception {
		assertThat(llamar("secreto", "secreto").getStatus()).isEqualTo(200);
	}
}
