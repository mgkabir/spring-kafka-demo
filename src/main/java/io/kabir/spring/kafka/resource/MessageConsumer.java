package io.kabir.spring.kafka.resource;

import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.PartitionOffset;
import org.springframework.kafka.annotation.TopicPartition;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

@EnableKafka
@Component
public class MessageConsumer {

    //@KafkaListener(topics = "test_topic", containerFactory = "filteredKafkaListenerContainerFactory")
    @KafkaListener(containerFactory = "filteredKafkaListenerContainerFactory",topicPartitions = @TopicPartition(topic = "test_topic", partitionOffsets = {
            @PartitionOffset(partition = "0", initialOffset = "${initial.offset}")
    }))
    public void filteredListen(@Payload String message) {
        System.out.println("Filtered message : " + message);
    }
}
