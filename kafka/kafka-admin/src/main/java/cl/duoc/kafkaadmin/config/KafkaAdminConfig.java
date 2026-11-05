package cl.duoc.kafkaadmin.config;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.KafkaAdmin;

@Configuration
@EnableConfigurationProperties(KafkaTopicsProperties.class)
public class KafkaAdminConfig {

	// Cliente de administración compartido. Los timeouts son cortos a propósito:
	// con el clúster caído la API debe responder un error en segundos, no colgarse.
	@Bean(destroyMethod = "close")
	Admin adminClient(KafkaAdmin kafkaAdmin) {
		Map<String, Object> config = new HashMap<>(kafkaAdmin.getConfigurationProperties());
		config.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, 5000);
		config.put(AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, 8000);
		config.put(AdminClientConfig.CLIENT_ID_CONFIG, "kafka-admin-edututor");
		return Admin.create(config);
	}
}
