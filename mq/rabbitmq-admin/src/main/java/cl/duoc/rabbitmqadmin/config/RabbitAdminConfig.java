package cl.duoc.rabbitmqadmin.config;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitAdmin;
import org.springframework.boot.CommandLineRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class RabbitAdminConfig {

	private static final Logger log = LoggerFactory.getLogger(RabbitAdminConfig.class);

	@Bean
	RabbitAdmin rabbitAdmin(ConnectionFactory connectionFactory) {
		return new RabbitAdmin(connectionFactory);
	}

	// RabbitAdmin declara los beans Queue/Exchange/Binding de forma perezosa,
	// solo al abrir una conexión. Este servicio no publica ni consume, así que
	// se fuerza la declaración al arrancar.
	@Bean
	CommandLineRunner declararTopologia(RabbitAdmin rabbitAdmin) {
		return args -> {
			rabbitAdmin.initialize();
			log.info("Topología de RabbitMQ declarada (exchanges, colas y DLQ de EduTutor)");
		};
	}
}
