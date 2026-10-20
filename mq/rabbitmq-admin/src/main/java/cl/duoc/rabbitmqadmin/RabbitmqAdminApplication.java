package cl.duoc.rabbitmqadmin;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;

// Microservicio administrador de RabbitMQ: declara la topología de EduTutor al
// arrancar y expone una API REST para gestionar colas, exchanges y bindings.
@SpringBootApplication
@ConfigurationPropertiesScan
public class RabbitmqAdminApplication {

	public static void main(String[] args) {
		SpringApplication.run(RabbitmqAdminApplication.class, args);
	}
}
