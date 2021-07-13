package com.pgjbz.reactive.test;

import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

@Slf4j
/*
* Reactive Streams
* 1 .Asynchronous
* 2. Non-blocking
* 3. Backpressure
* Publisher <- (subscribe) Subscriber
* Subscription is created
* Publisher (onSubscribe with the subscription) -> subscriber
* Subscription <- (request N) Subscriber
* Publisher -> (onNext) Subscriber
* until:
* 1. Publisher sends all the objects requested.
* 2. Publisher sends all the objects it has. (onComplete) subscriber and subscription will be canceled
* 3. There is and error. (onError) -> subscriber and subscription will be canceled
*/
class MonoTest {

    @Test
    void monoSubscriber(){
        String name = "Project Reactor";

        Mono<String> mono = Mono.just(name)
                .log();

        mono.subscribe();

        log.info("----------------------------------");

        StepVerifier.create(mono)
                .expectNext("Project Reactor")
                .verifyComplete();

    }

}
