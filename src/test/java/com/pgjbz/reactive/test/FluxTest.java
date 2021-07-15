package com.pgjbz.reactive.test;

import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.publisher.BaseSubscriber;
import reactor.core.publisher.ConnectableFlux;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;

import java.time.Duration;
import java.util.List;

@Slf4j
class FluxTest {

    @Test
    void fluxSubscriber() {
        String[] strings = {"Project", "Reactor", "2021", "pgjbz"};
        Flux<String> fluxStream = Flux.just(strings)
                .log();

        StepVerifier.create(fluxStream)
                .expectNext(strings)
                .verifyComplete();
    }

    @Test
    void fluxSubscriberNumber() {
        Flux<Integer> fluxStream = Flux.range(1, 5)
                .log();

        fluxStream.subscribe(i -> log.info("Number {}", i));


        log.info("------------------------------------------------");
        StepVerifier.create(fluxStream)
                .expectNext(1,2,3,4,5)
                .verifyComplete();
    }

    @Test
    void fluxSubscriberFromList() {
        Flux<Integer> fluxStream = Flux.fromIterable(List.of(1, 2, 3, 4, 5))
                .log();

        fluxStream.subscribe(i -> log.info("Number {}", i));

        log.info("------------------------------------------------");
        StepVerifier.create(fluxStream)
                .expectNext(1,2,3,4,5)
                .verifyComplete();
    }

    @Test
    void fluxSubscriberFromNumbersError() {
        Flux<Integer> fluxStream = Flux.range(1, 5)
                .log()
                .map(i -> {
                    if(i == 4) throw new RuntimeException("Invalid number");
                    return i;
                });

        fluxStream.subscribe(i -> log.info("Number {}", i), Throwable::printStackTrace,
                () -> log.info("DONE!"), subscription -> subscription.request(3));

        log.info("------------------------------------------------");
        StepVerifier.create(fluxStream)
                .expectNext(1,2,3)
                .expectError(RuntimeException.class)
                .verify();
    }

    @Test
    void fluxSubscriberFromNumbersUglyBackpressure() {
        Flux<Integer> fluxStream = Flux.range(1, 10)
                .log();

        fluxStream.subscribe(new Subscriber<Integer>() {

            private int count = 0;
            private Subscription subscription;
            private int requestCount = 2;

            @Override
            public void onSubscribe(Subscription subscription) {
                this.subscription = subscription;
                subscription.request(2);
            }

            @Override
            public void onNext(Integer integer) {
                count++;
                if(count >= requestCount) {
                    count = 0;
                    subscription.request(2);
                }
            }

            @Override
            public void onError(Throwable throwable) {

            }

            @Override
            public void onComplete() {
                log.info("Finish!");
            }
        });

        log.info("------------------------------------------------");
        StepVerifier.create(fluxStream)
                .expectNext(1,2,3,4,5,6,7,8,9,10)
                .verifyComplete();
    }

    @Test
    void fluxSubscriberFromNumbersNotSoUglyBackpressure() {
        Flux<Integer> fluxStream = Flux.range(1, 10)
                .log();

        fluxStream.subscribe(new BaseSubscriber<Integer>() {

            private int count = 0;
            private final int REQUEST_COUNT = 2;

            @Override
            protected void hookOnSubscribe(Subscription subscription) {
                request(REQUEST_COUNT);
            }

            @Override
            protected void hookOnNext(Integer value) {
                count++;
                if(count >= REQUEST_COUNT) {
                    count = 0;
                    request(2);
                }
            }

            @Override
            protected void hookOnComplete() {
                log.info("Finish!");
            }
        });

        log.info("------------------------------------------------");
        StepVerifier.create(fluxStream)
                .expectNext(1,2,3,4,5,6,7,8,9,10)
                .verifyComplete();
    }

    @Test
    void fluxSubscriberPrettyBackPressure() {
        Flux<Integer> fluxStream = Flux.range(1, 10)
                .log()
                .limitRate(3);

        fluxStream.subscribe(i -> log.info("Number {}", i));


        log.info("------------------------------------------------");
        StepVerifier.create(fluxStream)
                .expectNext(1,2,3,4,5,6,7,8,9,10)
                .verifyComplete();
    }

    @Test
    void fluxSubscriberIntervalOne() throws Exception {
        Flux<Long> interval = Flux.interval(Duration.ofMillis(100))
                .take(10)
                .log();

        interval.subscribe(i -> log.info("Number {}", i));

        Thread.sleep(3000);
    }

    @Test
    void fluxSubscriberIntervalTwo() {

        StepVerifier.withVirtualTime(this::createInterval)
                .expectSubscription()
                .expectNoEvent(Duration.ofDays(1))
                .thenAwait(Duration.ofDays(1))
                .expectNext(0L)
                .thenAwait(Duration.ofDays(1))
                .expectNext(1L)
                .thenCancel()
                .verify();
    }

    private Flux<Long> createInterval() {
        return Flux.interval(Duration.ofDays(1))
                .log();
    }

    @Test
    void connectableFlux() throws Exception {
        ConnectableFlux<Integer> connectableFlux = Flux.range(1, 10)
                .log()
                .delayElements(Duration.ofMillis(100))
                .publish();

//        connectableFlux.connect();
//
//        log.info("Thread sleepping for 300ms");
//
//        Thread.sleep(300);
//
//        connectableFlux.subscribe(i -> log.info("Sub1 number {}", i));
//
//        log.info("Thread sleepping for 200ms");
//
//        Thread.sleep(200);
//
//        connectableFlux.subscribe(i -> log.info("Sub2 number {}", i));

        StepVerifier
                .create(connectableFlux)
                .then(connectableFlux::connect)
                .thenConsumeWhile(i -> i<=5)
                .expectNext(6,7,8,9,10)
                .expectComplete()
                .verify();

    }

}
