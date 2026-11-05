package cl.duoc.kafkaadmin.web;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import org.springframework.web.filter.OncePerRequestFilter;

import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;

// Esta API puede borrar tópicos y mover offsets, así que no se deja abierta:
// si se configura edututor.admin.token (variable KAFKA_ADMIN_TOKEN), toda
// llamada debe traer el mismo valor en la cabecera X-Admin-Token.
// Sin token configurado el filtro no exige nada (desarrollo local).
@Component
public class AdminTokenFilter extends OncePerRequestFilter {

	static final String CABECERA = "X-Admin-Token";

	private final byte[] token;

	public AdminTokenFilter(@Value("${edututor.admin.token:}") String token) {
		this.token = token.getBytes(StandardCharsets.UTF_8);
	}

	@Override
	protected void doFilterInternal(HttpServletRequest request, HttpServletResponse response, FilterChain chain)
			throws ServletException, IOException {
		if (token.length > 0) {
			String recibido = request.getHeader(CABECERA);
			// comparación en tiempo constante
			if (recibido == null || !MessageDigest.isEqual(token, recibido.getBytes(StandardCharsets.UTF_8))) {
				response.sendError(HttpServletResponse.SC_UNAUTHORIZED, "Falta o es incorrecta la cabecera " + CABECERA);
				return;
			}
		}
		chain.doFilter(request, response);
	}
}
