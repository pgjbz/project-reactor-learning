package com.pgjbz.reactive.test;

import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.reactivestreams.Subscription;
import reactor.blockhound.BlockHound;
import reactor.blockhound.BlockingOperationError;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;
import reactor.test.StepVerifier;

import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;

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

    @BeforeAll
    static void setup(){
        BlockHound.install();
    }

    @Test
    void blockHoundWorks(){
        try {
            FutureTask<?> task = new FutureTask<>(() -> {
                Thread.sleep(0);
                return "";
            });

            Schedulers.parallel().schedule(task);

            task.get(10, TimeUnit.SECONDS);
            Assertions.fail("should fail");
        } catch (Exception e) {
            Assertions.assertTrue(e.getCause() instanceof BlockingOperationError);
        }
    }

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

    @Test
    void monoSubscriberConsumer(){
        String name = "Project Reactor";

        Mono<String> mono = Mono.just(name)
                .log();

        mono.subscribe(s -> log.info("Value: {}", s));

        log.info("----------------------------------");

        StepVerifier.create(mono)
                .expectNext("Project Reactor")
                .verifyComplete();

    }

    @Test
    void monoSubscriberConsumerError(){
        String name = "Project Reactor";

        Mono<String> mono = Mono.just(name)
                .map(s -> { throw new RuntimeException("Testing mono with error");});

        mono.subscribe(s -> log.info("Value: {}", s), s -> log.error("Error: {}", s.getMessage()));
        mono.subscribe(s -> log.info("Value: {}", s), Throwable::printStackTrace);

        log.info("----------------------------------");

        StepVerifier.create(mono)
                .expectError(RuntimeException.class)
                .verify();

    }

    @Test
    void monoSubscriberConsumerComplete(){
        String name = "Project Reactor";

        Mono<String> mono = Mono.just(name)
                .log()
                .map(String::toUpperCase);

        mono.subscribe(s -> log.info("Value: {}", s),
                Throwable::printStackTrace,
                () -> log.info("FINISHED!"));

        log.info("----------------------------------");

        StepVerifier.create(mono)
                .expectNext("Project Reactor".toUpperCase())
                .verifyComplete();

    }

    @Test
    void monoSubscriberConsumerSubscriptionCancel(){
        String name = "Project Reactor";

        Mono<String> mono = Mono.just(name)
                .log()
                .map(String::toUpperCase);

        mono.subscribe(s -> log.info("Value: {}", s),
                Throwable::printStackTrace,
                () -> log.info("FINISHED!"),
                Subscription::cancel);

        log.info("----------------------------------");

        StepVerifier.create(mono)
                .expectNext("Project Reactor".toUpperCase())
                .verifyComplete();
    }

    @Test
    void monoSubscriberConsumerSubscription(){
        String name = "Project Reactor";

        Mono<String> mono = Mono.just(name)
                .log()
                .map(String::toUpperCase);

        mono.subscribe(s -> log.info("Value: {}", s),
                Throwable::printStackTrace,
                () -> log.info("FINISHED!"),
                subscription -> subscription.request(1));

        log.info("----------------------------------");

        StepVerifier.create(mono)
                .expectNext("Project Reactor".toUpperCase())
                .verifyComplete();
    }

    @Test
    void monoDoOnMethods(){
        String name = "Project Reactor";

        Mono<Object> mono = Mono.just(name)
                .log()
                .map(String::toUpperCase)
                .doOnSubscribe(subscription -> log.info("Subscribed"))
                .doOnRequest(longNumber -> log.info("Request received, starting doing something..."))
                .doOnNext(s -> log.info("Value is here. Executing doOnNext {}", s))
                .flatMap(s -> Mono.empty())
                .doOnNext(s -> log.info("Value is here. Executing doOnNext {}", s)) //will not be executed
                .doOnSuccess(s -> log.info("doOnSuccess executed {}", s));

        mono.subscribe(s -> log.info("Value: {}", s),
                Throwable::printStackTrace,
                () -> log.info("FINISHED!"));

        log.info("----------------------------------");

    }

    @Test
    void monoDoOnError(){

        Mono<Object> mono = Mono.error(new IllegalArgumentException("Error testing"))
                .doOnError(e -> MonoTest.log.error("Error message {}", e.getMessage()))
                .doOnNext(s -> log.info("Executing this doOnNext"))  //will not be executed
                .log();

        StepVerifier.create(mono)
                .expectError(IllegalArgumentException.class)
                .verify();

    }

    @Test
    void monoDoOnErrorResume(){

        String name = "Project Reactor";

        Mono<Object> mono = Mono.error(new IllegalArgumentException("Error testing"))
                .doOnError(e -> MonoTest.log.error("Error message {}", e.getMessage()))
                .onErrorResume(s -> {
                    log.info("Inside on error resume");
                    return Mono.just(name);
                })  //will not be executed
                .log();

        StepVerifier.create(mono)
                .expectNext(name)
                .verifyComplete();

    }

    @Test
    void monoDoOnErrorReturn(){

        String name = "Project Reactor";

        Mono<Object> mono = Mono.error(new IllegalArgumentException("Error testing"))
                .doOnError(e -> MonoTest.log.error("Error message {}", e.getMessage()))
                .onErrorReturn("EMPTY")
                .onErrorResume(s -> { //will not be executed
                    log.info("Inside on error resume");
                    return Mono.just(name);
                })  //will not be executed
                .log();

        StepVerifier.create(mono)
                .expectNext("EMPTY")
                .verifyComplete();

    }

}
