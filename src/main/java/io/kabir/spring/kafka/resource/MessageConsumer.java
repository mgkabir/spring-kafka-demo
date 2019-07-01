package io.kabir.spring.kafka.resource;

import org.springframework.http.MediaType;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.PartitionOffset;
import org.springframework.kafka.annotation.TopicPartition;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;
import reactor.core.publisher.Flux;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.stream.Stream;

@EnableKafka
@RestController
public class MessageConsumer {

    private static BlockingQueue<String> messageQueue = new LinkedBlockingDeque<>();

    @KafkaListener(containerFactory = "filteredKafkaListenerContainerFactory", topicPartitions = @TopicPartition(topic = "test_topic", partitionOffsets = {
            @PartitionOffset(partition = "0", initialOffset = "${initial.offset}")
    }))
    public void filteredListen(@Payload String message) {
        System.out.println("filteredListen : " + message);
        try {
            messageQueue.put(message);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    @GetMapping(value = "/stream-flux", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    public Flux<String> streamFlux() {
        return Flux.fromStream(Stream.generate(() -> {
            try {
                return messageQueue.take();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            return "Interrupted";
        }));

    }

/*    @GetMapping("/stream-sse")
    public Flux<ServerSentEvent<String>> streamEvents() {
        Stream<String> streamFromQueue = Stream.generate(() -> {
            try {
                return messageQueue.take();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            return "Interrupted";
        });
        return Flux
                .fromStream(streamFromQueue)
                .map(item -> ServerSentEvent.<String>builder().id(item).event("periodic-events").data(item).build());
    }*/

/*    @GetMapping("/stream-sse")
    public Flux<ServerSentEvent<String>> streamEvents() {
        return Flux.interval(Duration.ofSeconds(1))
                .map(sequence -> ServerSentEvent.<String> builder()
                        .id(String.valueOf(sequence))
                        .event("periodic-event")
                        .data("SSE - " + LocalTime.now().toString())
                        .build());
    }*/

    @GetMapping(value = "/stream-sse-message", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    public SseEmitter streamSseMessage() {
        SseEmitter emitter = new SseEmitter();
        ExecutorService executor = Executors.newSingleThreadExecutor();
        executor.execute(() -> {
            try {
                for (int i = 0; true; i++) {
                    SseEmitter.SseEventBuilder event = SseEmitter.event()
                            //.data("SSE MVC - " + LocalTime.now().toString())
                            //.data(prevMessage)
                            .id(String.valueOf(i))
                            .name("sse event - mvc");
                    emitter.send(event);
                    Thread.sleep(3000);
                }
            } catch (Exception ex) {
                emitter.completeWithError(ex);
            }
        });
        return emitter;
    }
}
