package cl.duoc.rabbitmqadmin;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitAdmin;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;

@SpringBootApplication
public class RabbitmqAdminApplication {

	private static final Logger log = LoggerFactory.getLogger(RabbitmqAdminApplication.class);

	public static void main(String[] args) {
		SpringApplication.run(RabbitmqAdminApplication.class, args);
	}

	// RabbitAdmin declara los beans Queue/Exchange/Binding de forma perezosa,
	// solo cuando se abre una conexión real. Este admin no publica ni
	// consume nada, así que forzamos la declaración explícitamente al
	// arrancar — es justo el "provisionamiento declarativo" que pide el caso.
	@Bean
	RabbitAdmin rabbitAdmin(ConnectionFactory connectionFactory) {
		return new RabbitAdmin(connectionFactory);
	}

	@Bean
	CommandLineRunner declararTopologia(RabbitAdmin rabbitAdmin) {
		return args -> {
			rabbitAdmin.initialize();
			log.info("Topología de RabbitMQ declarada: exchanges cmd.direct, cmd.topic, cmd.dead.dlx; "
				+ "colas q.cmd.email, q.cmd.session, q.cmd.certificate + sus DLQ.");
		};
	}
}
