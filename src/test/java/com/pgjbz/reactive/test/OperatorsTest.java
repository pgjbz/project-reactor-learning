package com.pgjbz.reactive.test;

import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;
import reactor.test.StepVerifier;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

@Slf4j
class OperatorsTest {

    @Test
    void subscribeOnSimple() {
        Flux<Integer> flux = Flux.range(1, 4)
                .map(i -> {
                    log.info("Map 1 - number {} on Thread {}", i, Thread.currentThread().getName());
                    return i;
                })
                .subscribeOn(Schedulers.boundedElastic()) //affect all
                .map(i -> {
                    log.info("Map 2 - number {} on Thread {}", i, Thread.currentThread().getName());
                    return i;
                });

        StepVerifier.create(flux)
                .expectSubscription()
                .expectNext(1,2,3,4)
                .verifyComplete();
    }

    @Test
    void publishOnSimple() {
        Flux<Integer> flux = Flux.range(1, 4)
                .map(i -> {
                    log.info("Map 1 - number {} on Thread {}", i, Thread.currentThread().getName());
                    return i;
                })
                .publishOn(Schedulers.boundedElastic()) //affect only bellow
                .map(i -> {
                    log.info("Map 2 - number {} on Thread {}", i, Thread.currentThread().getName());
                    return i;
                });

        flux.subscribe();
        flux.subscribe();

        StepVerifier.create(flux)
                .expectSubscription()
                .expectNext(1,2,3,4)
                .verifyComplete();
    }

    @Test
    void multipleSubscribeOnSimple() {
        Flux<Integer> flux = Flux.range(1, 4)
                .subscribeOn(Schedulers.boundedElastic()) //used
                .map(i -> {
                    log.info("Map 1 - number {} on Thread {}", i, Thread.currentThread().getName());
                    return i;
                })
                .subscribeOn(Schedulers.single()) //not used
                .map(i -> {
                    log.info("Map 2 - number {} on Thread {}", i, Thread.currentThread().getName());
                    return i;
                });

        StepVerifier.create(flux)
                .expectSubscription()
                .expectNext(1,2,3,4)
                .verifyComplete();
    }

    @Test
    void multiplePublishOnSimple() {
        Flux<Integer> flux = Flux.range(1, 4)
                .publishOn(Schedulers.single()) //used
                .map(i -> {
                    log.info("Map 1 - number {} on Thread {}", i, Thread.currentThread().getName());
                    return i;
                })
                .publishOn(Schedulers.boundedElastic()) //used
                .map(i -> {
                    log.info("Map 2 - number {} on Thread {}", i, Thread.currentThread().getName());
                    return i;
                });

        flux.subscribe();
        flux.subscribe();

        StepVerifier.create(flux)
                .expectSubscription()
                .expectNext(1,2,3,4)
                .verifyComplete();
    }

    @Test
    void publishOnAndSubscribeOnSimple() {
        Flux<Integer> flux = Flux.range(1, 4)
                .publishOn(Schedulers.single()) //used
                .map(i -> {
                    log.info("Map 1 - number {} on Thread {}", i, Thread.currentThread().getName());
                    return i;
                })
                .subscribeOn(Schedulers.boundedElastic()) //not used
                .map(i -> {
                    log.info("Map 2 - number {} on Thread {}", i, Thread.currentThread().getName());
                    return i;
                });

        StepVerifier.create(flux)
                .expectSubscription()
                .expectNext(1,2,3,4)
                .verifyComplete();
    }

    @Test
    void subscribeOnAndPublishOnSimple() {
        Flux<Integer> flux = Flux.range(1, 4)
                .subscribeOn(Schedulers.single()) //used
                .map(i -> {
                    log.info("Map 1 - number {} on Thread {}", i, Thread.currentThread().getName());
                    return i;
                })
                .publishOn(Schedulers.boundedElastic()) //used only for bellow
                .map(i -> {
                    log.info("Map 2 - number {} on Thread {}", i, Thread.currentThread().getName());
                    return i;
                });

        StepVerifier.create(flux)
                .expectSubscription()
                .expectNext(1,2,3,4)
                .verifyComplete();
    }

    @Test
    void subscribeOnIO() throws Exception{
        Mono<List<String>> list = Mono.fromCallable(() -> Files.readAllLines(Path.of("text-file")))
                .log()
                .subscribeOn(Schedulers.boundedElastic());

        list.subscribe(s -> log.info("{}", s));

//        Thread.sleep(1000);

        StepVerifier.create(list)
                .expectSubscription()
                .thenConsumeWhile(l -> {
                    Assertions.assertFalse(l.isEmpty());
                    log.info("Size {}", l.size());
                    return true;
                })
                .verifyComplete();
    }

    @Test
    void switchIfEmptyOperator() {
        Flux<Object> flux = emptyFlux().switchIfEmpty(Flux.just("not empty anymore"))
                .log();

        StepVerifier.create(flux)
                .expectSubscription()
                .expectNext("not empty anymore")
                .expectComplete()
                .verify();
    }

    @Test
    void deferOperator() throws Exception {
        Mono<Long> just = Mono.just(System.nanoTime());
        Mono<Long> defer = Mono.defer(() -> Mono.just(System.nanoTime()));

        just.subscribe(l -> log.info("time {}", l));
        Thread.sleep(100);
        just.subscribe(l -> log.info("time {}", l));
        Thread.sleep(100);
        just.subscribe(l -> log.info("time {}", l));
        Thread.sleep(100);
        just.subscribe(l -> log.info("time {}", l));
        Thread.sleep(100);
        just.subscribe(l -> log.info("time {}", l));

        defer.subscribe(l -> log.info("time {}", l));
        Thread.sleep(100);
        defer.subscribe(l -> log.info("time {}", l));
        Thread.sleep(100);
        defer.subscribe(l -> log.info("time {}", l));
        Thread.sleep(100);
        defer.subscribe(l -> log.info("time {}", l));
        Thread.sleep(100);
        defer.subscribe(l -> log.info("time {}", l));

        AtomicLong atomicLong = new AtomicLong(0);
        defer.subscribe(atomicLong::set);
        Assertions.assertTrue(atomicLong.get() > 0);

    }

    @Test
    void concatOperator() {
        Flux<String> flux1 = Flux.just("a", "b");
        Flux<String> flux2 = Flux.just("c", "d");

        Flux<String> concat = Flux.concat(flux1, flux2).log();

        StepVerifier.create(concat)
                .expectSubscription()
                .expectNext("a", "b", "c", "d")
                .expectComplete()
                .verify();

    }

    @Test
    void concatOperatorError() {
        Flux<String> flux1 = Flux.just("a", "b")
                .map(s -> {
                    if(s.equals("b"))
                        throw new IllegalArgumentException();
                    return s;
                });
        Flux<String> flux2 = Flux.just("c", "d");

        Flux<String> concat = Flux.concatDelayError(flux1, flux2).log();

        StepVerifier.create(concat)
                .expectSubscription()
                .expectNext("a", "c", "d")
                .expectError(IllegalArgumentException.class)
                .verify();
    }

    @Test
    void concatWithOperator() {
        Flux<String> flux1 = Flux.just("a", "b");
        Flux<String> flux2 = Flux.just("c", "d");

        Flux<String> concatWith = flux1.concatWith(flux2).log();

        StepVerifier.create(concatWith)
                .expectSubscription()
                .expectNext("a", "b", "c", "d")
                .expectComplete()
                .verify();
    }

    @Test
    void concatLatestOperator() {
        Flux<String> flux1 = Flux.just("a", "b");
        Flux<String> flux2 = Flux.just("c", "d");

        Flux<String> combineLatest = Flux.combineLatest(flux1, flux2, (s1, s2) -> s1.toUpperCase() + s2.toUpperCase())
                .log();


        StepVerifier.create(combineLatest)
                .expectSubscription()
                .expectNext("BC", "BD")
                .expectComplete()
                .verify();
    }

    @Test
    void mergeOperatorError()  {
        Flux<String> flux1 = Flux.just("a", "b").map(s -> {
            if(s.equals("b"))
                throw new IllegalArgumentException();
            return s;
        }).doOnError(t -> log.error("We could do something with this"));
        Flux<String> flux2 = Flux.just("c", "d");
        Flux<String> merge = Flux.mergeDelayError(1, flux1, flux2).log();

        StepVerifier.create(merge)
                .expectSubscription()
                .expectNext("a",  "c", "d")
                .expectError(IllegalArgumentException.class)
                .verify();

    }

    @Test
    void mergeOperatorErrorExampleTwo() {
        Flux<String> flux1 = Flux.just("a", "b").map(s -> {
            if(s.equals("b"))
                throw new IllegalArgumentException();
            return s;
        }).doOnError(t -> log.error("We could do something with this"));
        Flux<String> flux2 = Flux.just("c", "d");

        Flux<String> mergeWith = Flux.mergeDelayError(1, flux1, flux2, flux1)
                .log();

        StepVerifier.create(mergeWith)
                .expectSubscription()
                .expectNext("a", "c", "d", "a")
                .expectError()
                .verify();
    }

    @Test
    void mergeOperator() throws Exception {
        Flux<String> flux1 = Flux.just("a", "b").delayElements(Duration.ofMillis(200));
        Flux<String> flux2 = Flux.just("c", "d").delayElements(Duration.ofMillis(220));
        Flux<String> merge = Flux.merge(flux1, flux2).log();

        StepVerifier.create(merge)
                .expectSubscription()
                .expectNext("a", "c", "b", "d")
                .expectComplete()
                .verify();

    }

    @Test
    void mergeWithOperator() throws Exception {
        Flux<String> flux1 = Flux.just("a", "b").delayElements(Duration.ofMillis(200));
        Flux<String> flux2 = Flux.just("c", "d").delayElements(Duration.ofMillis(220));

        Flux<String> mergeWith = flux1.mergeWith(flux2);

        StepVerifier.create(mergeWith)
                .expectSubscription()
                .expectNext("a", "c", "b", "d")
                .expectComplete()
                .verify();

    }

    @Test
    void mergeSequentialOperator() {
        Flux<String> flux1 = Flux.just("a", "b").delayElements(Duration.ofMillis(200));
        Flux<String> flux2 = Flux.just("c", "d").delayElements(Duration.ofMillis(220));

        Flux<String> mergeWith = Flux.mergeSequential(flux1, flux2, flux1)
                .log();

        StepVerifier.create(mergeWith)
                .expectSubscription()
                .expectNext("a", "b", "c", "d", "a", "b")
                .expectComplete()
                .verify();
    }


    private Flux<Object> emptyFlux() {
        return Flux.empty();
    }
}
