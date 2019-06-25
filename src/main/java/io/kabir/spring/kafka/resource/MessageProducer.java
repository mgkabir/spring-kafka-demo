package io.kabir.spring.kafka.resource;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("kafka")
public class MessageProducer {


    @Autowired
    KafkaTemplate<String, String> kafkaTemplate;

    private static final String TOPIC = "test_topic";

    @GetMapping("/{message}")
    public String message(@PathVariable("message") final String message) {

        ListenableFuture<SendResult<String, String>> future = kafkaTemplate.send(TOPIC, message);
        future.addCallback(success -> {
            System.out.println("Sent message [ " + message + " ] with offset [ " + success.getRecordMetadata().offset() + " ]");
        }, failure -> {
            failure.printStackTrace();
        });
        return "Published : "+message;
    }
}
