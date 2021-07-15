package com.pgjbz.reactive.test;

import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;

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
}
