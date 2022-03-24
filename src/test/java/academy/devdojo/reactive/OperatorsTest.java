package academy.devdojo.reactive;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;
import reactor.test.StepVerifier;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import lombok.extern.slf4j.Slf4j;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

@Slf4j
public class OperatorsTest {

    @Test
    void subscribeOn() {
        Flux<Integer> flux = Flux.range(1, 5)
            .map(value -> {
                log.info("Map 1 - Thread: {}, Value: {}", Thread.currentThread().getName(), value);
                return value;
            })
            .subscribeOn(Schedulers.boundedElastic())
            .map(value -> {
                log.info("Map 2 - Thread: {}, Value: {}", Thread.currentThread().getName(), value);
                return value;
            });
        StepVerifier.create(flux)
            .expectNext(1, 2, 3, 4, 5)
            .verifyComplete();
    }

    @Test
    void publishOn() {
        Flux<Integer> flux = Flux.range(1, 5)
            .map(value -> {
                log.info("Map 1 - Thread: {}, Value: {}", Thread.currentThread().getName(), value);
                return value;
            })
            .publishOn(Schedulers.boundedElastic())
            .map(value -> {
                log.info("Map 2 - Thread: {}, Value: {}", Thread.currentThread().getName(), value);
                return value;
            });
        StepVerifier.create(flux)
            .expectNext(1, 2, 3, 4, 5)
            .verifyComplete();
    }

    @Test
    void multipleSubscribeOn() {
        Flux<Integer> flux = Flux.range(1, 5)
            .subscribeOn(Schedulers.single())
            .map(value -> {
                log.info("Map 1 - Thread: {}, Value: {}", Thread.currentThread().getName(), value);
                return value;
            })
            .subscribeOn(Schedulers.boundedElastic())
            .map(value -> {
                log.info("Map 2 - Thread: {}, Value: {}", Thread.currentThread().getName(), value);
                return value;
            });
        StepVerifier.create(flux)
            .expectNext(1, 2, 3, 4, 5)
            .verifyComplete();
    }

    @Test
    void multiplePublishOn() {
        Flux<Integer> flux = Flux.range(1, 5)
            .publishOn(Schedulers.single())
            .map(value -> {
                log.info("Map 1 - Thread: {}, Value: {}", Thread.currentThread().getName(), value);
                return value;
            })
            .publishOn(Schedulers.boundedElastic())
            .map(value -> {
                log.info("Map 2 - Thread: {}, Value: {}", Thread.currentThread().getName(), value);
                return value;
            });
        StepVerifier.create(flux)
            .expectNext(1, 2, 3, 4, 5)
            .verifyComplete();
    }

    @Test
    void publishAndSubscribeOn() {
        Flux<Integer> flux = Flux.range(1, 5)
            .publishOn(Schedulers.single())
            .map(value -> {
                log.info("Map 1 - Thread: {}, Value: {}", Thread.currentThread().getName(), value);
                return value;
            })
            .subscribeOn(Schedulers.boundedElastic())
            .map(value -> {
                log.info("Map 2 - Thread: {}, Value: {}", Thread.currentThread().getName(), value);
                return value;
            });
        StepVerifier.create(flux)
            .expectNext(1, 2, 3, 4, 5)
            .verifyComplete();
    }

    @Test
    void subscribeAndPublishOn() {
        Flux<Integer> flux = Flux.range(1, 5)
            .subscribeOn(Schedulers.single())
            .map(value -> {
                log.info("Map 1 - Thread: {}, Value: {}", Thread.currentThread().getName(), value);
                return value;
            })
            .publishOn(Schedulers.boundedElastic())
            .map(value -> {
                log.info("Map 2 - Thread: {}, Value: {}", Thread.currentThread().getName(), value);
                return value;
            });
        StepVerifier.create(flux)
            .expectNext(1, 2, 3, 4, 5)
            .verifyComplete();
    }

    @Test
    void subscribeOnIO() {
        Mono<List<String>> mono = Mono.fromCallable(() -> Files.readAllLines(Path.of("text")))
            .log()
            .subscribeOn(Schedulers.boundedElastic());

        // mono.subscribe(s -> log.info("{}", s));

        StepVerifier.create(mono)
            .expectSubscription()
            .thenConsumeWhile(l -> {
                Assertions.assertFalse(l.isEmpty());
                log.info("Value: {}", l);
                return true;
            })
            .verifyComplete();
    }

    @Test
    void switchIfEmptyOperator() {
        String message = "Not empty anymore";
        Flux<Object> flux = Flux.empty()
            .switchIfEmpty(Flux.just(message))
            .log();

        StepVerifier.create(flux)
            .expectSubscription()
            .expectNext(message)
            .verifyComplete();
    }

    @Test
    void deferOperator() throws Exception {
        Mono<Long> defer = Mono.defer(() -> Mono.just(System.currentTimeMillis()));

        defer.subscribe(currentMillis -> log.info("Time: {}", currentMillis));
        Thread.sleep(200);

        defer.subscribe(currentMillis -> log.info("Time: {}", currentMillis));
        Thread.sleep(200);

        defer.subscribe(currentMillis -> log.info("Time: {}", currentMillis));
        Thread.sleep(200);

        defer.subscribe(currentMillis -> log.info("Time: {}", currentMillis));

        AtomicLong atomicLong = new AtomicLong();
        defer.subscribe(atomicLong::set);

        Assertions.assertTrue(atomicLong.get() > 0);
    }
}
