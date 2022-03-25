package academy.devdojo.reactive;

import reactor.blockhound.BlockHound;
import reactor.core.publisher.BaseSubscriber;
import reactor.core.publisher.ConnectableFlux;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;

import java.time.Duration;
import java.util.List;

import lombok.extern.slf4j.Slf4j;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

@Slf4j
public class FluxTest {

    @BeforeAll
    static void setup() {
        BlockHound.install();
    }

    @Test
    void fluxSubscriber() {
        Flux<String> flux = Flux.just("Levi", "Ferrera", "Thoughtworks").log();
        StepVerifier.create(flux)
            .expectNext("Levi", "Ferrera", "Thoughtworks")
            .verifyComplete();
    }

    @Test
    void fluxSubscriberNumbers() {
        Flux<Integer> flux = Flux.range(1, 5);
        flux.subscribe(i -> log.info("Value: {}", i));
        StepVerifier.create(flux)
            .expectNext(1, 2, 3, 4, 5)
            .verifyComplete();
    }

    @Test
    void fluxSubscriberFromList() {
        Flux<Integer> flux = Flux.fromIterable(List.of(1, 2, 3, 4, 5));
        flux.subscribe(i -> log.info("Value: {}", i));
        StepVerifier.create(flux)
            .expectNext(1, 2, 3, 4, 5)
            .verifyComplete();
    }

    @Test
    void fluxSubscriberNumbersWithError() {
        Flux<Integer> flux = Flux.range(1, 5)
            .map(i -> {
                if (i == 4) {
                    throw new IllegalArgumentException("Number equal 4");
                }
                return i;
            });

        flux.subscribe(
            new Subscriber<>() {
                @Override
                public void onSubscribe(Subscription subscription) {
                    subscription.request(4);
                }

                @Override
                public void onNext(Integer integer) {
                    log.info("Value: {}", integer);
                }

                @Override
                public void onError(Throwable throwable) {
                    throwable.printStackTrace();
                }

                @Override
                public void onComplete() {
                    log.info("Should not log this");
                }
            }
        );

        StepVerifier.create(flux)
            .expectNext(1, 2, 3)
            .expectError(IllegalArgumentException.class)
            .verify();
    }

    @Test
    void fluxSubscriberNumbersBackPressure() {
        Flux<Integer> flux = Flux.range(1, 10)
            .log();

        flux.subscribe(
            new Subscriber<>() {
                private int count = 0;
                private Subscription subscription;
                private final int subsRequestSize = 2;

                @Override
                public void onSubscribe(Subscription subscription) {
                    this.subscription = subscription;
                    this.subscription.request(subsRequestSize);
                }

                @Override
                public void onNext(Integer integer) {
                    count++;
                    if (count == subsRequestSize) {
                        count = 0;
                        subscription.request(subsRequestSize);
                    }
                }

                @Override
                public void onError(Throwable throwable) {
                    throwable.printStackTrace();
                }

                @Override
                public void onComplete() {
                    log.info("onComplete");
                }
            }
        );

        StepVerifier.create(flux)
            .expectNext(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
            .verifyComplete();
    }

    @Test
    void fluxSubscriberNumbersBackPressureV2() {
        Flux<Integer> flux = Flux.range(1, 10)
            .log();

        flux.subscribe(
            new BaseSubscriber<>() {
                private int count = 0;
                private final int subsRequestSize = 2;

                @Override
                protected void hookOnSubscribe(Subscription subscription) {
                    request(subsRequestSize);
                }

                @Override
                protected void hookOnNext(Integer value) {
                    count++;
                    if (count == subsRequestSize) {
                        count = 0;
                        request(subsRequestSize);
                    }
                }
            }
        );

        StepVerifier.create(flux)
            .expectNext(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
            .verifyComplete();
    }

    @Test
    void fluxSubscriberNumbersBackPressureV3() {
        Flux<Integer> flux = Flux.range(1, 10)
            .log()
            .limitRate(3);

        flux.subscribe(value -> log.info("Value: {}", value));

        StepVerifier.create(flux)
            .expectNext(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
            .verifyComplete();
    }

    @Test
    void fluxSubscriberInterval() throws InterruptedException {
        Flux<Long> interval = Flux.interval(Duration.ofMillis(200))
            .take(10)
            .log();

        interval.subscribe(i -> log.info("Value: {}", i));

        Thread.sleep(3000);
    }

    @Test
    void fluxSubscriberIntervalV2() {
        StepVerifier.withVirtualTime(this::createIntervalFlux)
            .expectSubscription()
            .expectNoEvent(Duration.ofDays(1))
            .thenAwait(Duration.ofDays(1))
            .expectNext(0L)
            .thenAwait(Duration.ofDays(1))
            .expectNext(1L)
            .thenCancel()
            .verify();
    }

    private Flux<Long> createIntervalFlux() {
        return Flux.interval(Duration.ofDays(1))
            .take(10)
            .log();
    }

    @Test
    void connectableFlux() throws InterruptedException {
        ConnectableFlux<Integer> flux = Flux.range(1, 10)
//            .log()
            .delayElements(Duration.ofMillis(100))
            .publish();

        flux.connect();

        log.info("Thread sleeping for 300ms");
        Thread.sleep(300);
        flux.subscribe(value -> log.info("Subscription 1 - VALUE {}", value));

        log.info("Thread sleeping for 200ms");
        Thread.sleep(200);
        flux.subscribe(value -> log.info("Subscription 2 - VALUE {}", value));

        StepVerifier.create(flux)
            .then(flux::connect)
            .expectNext(5, 6, 7, 8, 9, 10)
            .verifyComplete();
    }

    @Test
    void connectableFluxAutoConnect() throws InterruptedException {
        Flux<Integer> flux = Flux.range(5, 5)
            .delayElements(Duration.ofMillis(100))
            .publish()
            .autoConnect(2);

        StepVerifier.create(flux)
            .then(flux::subscribe)
            .expectNext(5, 6, 7, 8, 9)
            .verifyComplete();
    }
}
