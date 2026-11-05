package cl.duoc.kafkaadmin.web.dto;

import java.util.List;

public record ClusterInfo(String clusterId, Integer controlador, List<Broker> brokers) {

	public record Broker(int id, String host, int puerto, String rack) {
	}
}
