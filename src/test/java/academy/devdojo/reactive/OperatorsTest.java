package academy.devdojo.reactive;

import reactor.core.publisher.Flux;
import reactor.core.scheduler.Schedulers;
import reactor.test.StepVerifier;

import lombok.extern.slf4j.Slf4j;

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
}
