package com.example.producer;

import com.blautech.o2.commons.shared.order.OrderContext;
import com.blautech.o2.commons.shared.order.OrderFlow;
import com.blautech.o2.commons.shared.order.OrderFlowStep;
import com.example.config.KafkaConfig;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public class MessageProducer {
    private static final Logger logger = LoggerFactory.getLogger(MessageProducer.class);
    private static final String TOPIC = "workflow-events-resthandler-default";
    private static final ObjectMapper objectMapper = new ObjectMapper();

    private static String createTestMessage(String requestId) throws Exception {
        // Crear el paso actual
        OrderFlowStep<Map<String, Object>> currentStep = new OrderFlowStep<>();
        currentStep.setServiceName("liberate");
        currentStep.setAction("GetBillingAccountByIdentity");

        Map<String, Object> options = new HashMap<>();
        options.put("step_number", 1);
        options.put("break_on_fail", false);
        currentStep.setOptions(options);

        // Crear pasos siguientes
        List<OrderFlowStep<Map<String, Object>>> steps = new ArrayList<>();

        // Paso 2: TNM
        OrderFlowStep<Map<String, Object>> tnmStep = new OrderFlowStep<>();
        tnmStep.setServiceName("tnm");
        tnmStep.setAction("GetReservedSubscriberId");
        tnmStep.setOptions(Map.of("step_number", 2, "break_on_fail", false));
        steps.add(tnmStep);

        // Paso 3: Tertio
        OrderFlowStep<Map<String, Object>> tertioStep = new OrderFlowStep<>();
        tertioStep.setServiceName("tertio");
        tertioStep.setAction("ProvisionData");
        tertioStep.setOptions(Map.of("step_number", 3, "break_on_fail", false));
        steps.add(tertioStep);

        // Paso 4: Liberate ValidateRenew
        OrderFlowStep<Map<String, Object>> validateStep = new OrderFlowStep<>();
        validateStep.setServiceName("liberate");
        validateStep.setAction("ValidateRenew");
        validateStep.setOptions(Map.of("step_number", 4, "break_on_fail", false));
        steps.add(validateStep);

        // Paso 5: Final
        OrderFlowStep<Map<String, Object>> finalStep = new OrderFlowStep<>();
        finalStep.setServiceName("final");
        finalStep.setAction("finishOrder");
        finalStep.setOptions(Map.of("step_number", 5, "break_on_fail", false));
        steps.add(finalStep);

        // Crear el flujo
        OrderFlow<Map<String, Object>> orderFlow = new OrderFlow<>();
        orderFlow.setCurrentStep(currentStep);
        orderFlow.setSteps(steps);
        orderFlow.setPrevSteps(new ArrayList<>());

        // Crear request original
        Map<String, Object> originalRequest = new HashMap<>();
        originalRequest.put("requestId", requestId);
        originalRequest.put("documentNumber", "12345678");
        originalRequest.put("documentType", "DNI");
        originalRequest.put("phoneNumber", "999888777");
        originalRequest.put("email", "test@example.com");
        originalRequest.put("workflow", "renovacion");
        originalRequest.put("subworkflow", "postpago");

        // Crear y configurar el contexto
        OrderContext<Map<String, Object>> context = new OrderContext<>(originalRequest);
        context.setRequestID(requestId);
        context.setOrderFlow(orderFlow);

        return objectMapper.writeValueAsString(context);
    }

    public static void main(String[] args) {
        String requestId = "TEST-" + System.currentTimeMillis();
        logger.info("Iniciando envío de mensaje de prueba. RequestID: {}", requestId);

        // Obtener las propiedades del productor
        Properties props = KafkaConfig.getProducerProperties();
        logger.info("Conectando a Kafka en: {}", props.getProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG));

        try (Producer<String, String> producer = new KafkaProducer<>(props)) {
            String message = createTestMessage(requestId);

            ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC, message);

            producer.send(record, (metadata, exception) -> {
                if (exception == null) {
                    logger.info("Mensaje enviado exitosamente:");
                    logger.info("Topic: {}", metadata.topic());
                    logger.info("Partición: {}", metadata.partition());
                    logger.info("Offset: {}", metadata.offset());
                    logger.info("RequestID: {}", requestId);
                    logger.debug("Contenido del mensaje: {}", message);
                } else {
                    logger.error("Error al enviar el mensaje: {}", exception.getMessage(), exception);
                }
            });

            producer.flush();
            logger.info("Mensaje enviado y flush completado. RequestID: {}", requestId);

            // Esperar un momento para asegurar que el callback se ejecute
            Thread.sleep(2000);

        } catch (Exception e) {
            logger.error("Error en el productor: {}", e.getMessage(), e);
        }
    }
}