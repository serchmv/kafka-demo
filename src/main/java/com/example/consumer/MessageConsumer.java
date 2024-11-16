package com.example.consumer;

import com.blautech.o2.commons.shared.order.OrderContext;
import com.blautech.o2.commons.shared.order.OrderFlow;
import com.blautech.o2.commons.shared.order.OrderFlowStep;
import com.example.config.KafkaConfig;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class MessageConsumer {
    private static final Logger logger = LoggerFactory.getLogger(MessageConsumer.class);
    private static final String TOPIC = "liberate";
    private final ObjectMapper objectMapper;

    public MessageConsumer() {
        this.objectMapper = new ObjectMapper();
    }

    private void processMessage(String message) {
        try {
            // Convertir el mensaje a Map
            Map<String, Object> originalRequest = objectMapper.readValue(message, Map.class);

            // Crear el OrderContext
            OrderContext<Map<String, Object>> context = new OrderContext<>(originalRequest);

            // Configurar el RequestID
            context.setRequestID(originalRequest.getOrDefault("requestId", "REQ-" + System.currentTimeMillis()).toString());

            // Si hay información de flujo, configurarla
            if (originalRequest.containsKey("orderFlow")) {
                Map<String, Object> flowData = (Map<String, Object>) originalRequest.get("orderFlow");
                OrderFlow<Map<String, Object>> orderFlow = new OrderFlow<>();

                // Configurar el paso actual si existe
                if (flowData.containsKey("currentStep")) {
                    Map<String, Object> stepData = (Map<String, Object>) flowData.get("currentStep");
                    OrderFlowStep<Map<String, Object>> currentStep = createOrderFlowStep(stepData);
                    orderFlow.setCurrentStep(currentStep);
                }

                context.setOrderFlow(orderFlow);
            }

            logger.info("Procesando OrderContext con RequestID: {}", context.getRequestID());
            if (context.getOrderFlow() != null && context.getOrderFlow().getCurrentStep() != null) {
                logger.info("Paso actual: Servicio = {}, Acción = {}",
                        context.getOrderFlow().getCurrentStep().getServiceName(),
                        context.getOrderFlow().getCurrentStep().getAction());
            }

        } catch (Exception e) {
            logger.error("Error procesando mensaje: {}", e.getMessage(), e);
        }
    }

    private OrderFlowStep<Map<String, Object>> createOrderFlowStep(Map<String, Object> stepData) {
        OrderFlowStep<Map<String, Object>> step = new OrderFlowStep<>();
        step.setServiceName((String) stepData.get("service_name"));
        step.setAction((String) stepData.get("action"));
        step.setOptions((Map<String, Object>) stepData.getOrDefault("options", new HashMap<>()));
        return step;
    }

    public static void main(String[] args) {
        MessageConsumer messageConsumer = new MessageConsumer();

        try (Consumer<String, String> consumer = new KafkaConsumer<>(KafkaConfig.getConsumerProperties())) {
            consumer.subscribe(Collections.singletonList(TOPIC));
            logger.info("Iniciando consumo de mensajes del topic: {}", TOPIC);

            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

                records.forEach(record -> {
                    logger.info("Mensaje recibido: Topic = {}, Partición = {}, Offset = {}",
                            record.topic(), record.partition(), record.offset());
                    messageConsumer.processMessage(record.value());
                });
            }
        }
    }
}