/*
 * Copyright 2020-Present The Serverless Workflow Specification Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.serverlessworkflow.impl.test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import io.cloudevents.CloudEvent;
import io.cloudevents.core.builder.CloudEventBuilder;
import io.cloudevents.jackson.JsonCloudEventData;
import io.serverlessworkflow.api.types.Workflow;
import io.serverlessworkflow.fluent.spec.WorkflowBuilder;
import io.serverlessworkflow.fluent.spec.dsl.DSL;
import io.serverlessworkflow.impl.ContextPropagator;
import io.serverlessworkflow.impl.ExecutorServiceFactory;
import io.serverlessworkflow.impl.WorkflowApplication;
import io.serverlessworkflow.impl.WorkflowInstance;
import io.serverlessworkflow.impl.events.EventPublisher;
import io.serverlessworkflow.impl.jackson.JsonUtils;
import io.serverlessworkflow.impl.test.ctx.ContextProbe;
import java.net.URI;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.AbstractExecutorService;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

/**
 * Proves that the {@link ContextPropagator} hook keeps the starting thread's context alive across
 * the asynchronous tasks that complete on threads the SDK does not own: {@code wait} (timer
 * thread), {@code emit} (external publisher thread) and {@code listen} (event-consumer thread).
 *
 * <p>{@link ContextProbe#CURRENT} stands in for the Quarkus request context / {@code
 * SecurityIdentity}. Each test runs the same flow twice: once with {@link ContextPropagator#NOOP}
 * (documenting that a {@code ManagedExecutor} alone loses the context) and once with {@link
 * ContextProbe#PROPAGATOR} (the fix).
 */
class ContextPropagationTest {

  private static final String CONTEXT = "tenant-42";

  /**
   * Mimics a MicroProfile {@code ManagedExecutor}: capture-at-submit, apply-on-worker, restore
   * after. Used to show that even a correct context-propagating executor cannot, on its own,
   * survive a hop completed by a foreign thread.
   */
  static final class CaptureAtSubmitExecutor extends AbstractExecutorService {
    private final ExecutorService delegate = Executors.newCachedThreadPool();

    @Override
    public void execute(Runnable command) {
      final String captured = ContextProbe.CURRENT.get();
      delegate.execute(
          () -> {
            final String previous = ContextProbe.CURRENT.get();
            ContextProbe.CURRENT.set(captured);
            try {
              command.run();
            } finally {
              if (previous == null) {
                ContextProbe.CURRENT.remove();
              } else {
                ContextProbe.CURRENT.set(previous);
              }
            }
          });
    }

    @Override
    public void shutdown() {
      delegate.shutdown();
    }

    @Override
    public java.util.List<Runnable> shutdownNow() {
      return delegate.shutdownNow();
    }

    @Override
    public boolean isShutdown() {
      return delegate.isShutdown();
    }

    @Override
    public boolean isTerminated() {
      return delegate.isTerminated();
    }

    @Override
    public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
      return delegate.awaitTermination(timeout, unit);
    }
  }

  private static ExecutorServiceFactory factoryFor(ExecutorService executor) {
    return new ExecutorServiceFactory() {
      @Override
      public ExecutorService get() {
        return executor;
      }

      @Override
      public void close() {
        executor.shutdownNow();
      }
    };
  }

  // ===================== wait =====================

  private static Map<String, String> runWaitFlow(ContextPropagator propagator) {
    Map<String, String> seen = new ConcurrentHashMap<>();
    Workflow workflow =
        WorkflowBuilder.workflow("wait-ctx", "test", "0.1.0")
            .tasks(
                DSL.set("before", s -> s.put("a", 1)),
                DSL.waitMillis(100),
                DSL.set("after", s -> s.put("b", 2)))
            .build();

    WorkflowApplication app =
        WorkflowApplication.builder()
            .withContextPropagator(propagator)
            .withExecutorFactory(factoryFor(new CaptureAtSubmitExecutor()))
            .withListener(ContextProbe.onTaskStarted(seen))
            .build();
    try {
      ContextProbe.CURRENT.set(CONTEXT);
      app.workflowDefinition(workflow).instance(Map.of()).start().join();
    } finally {
      ContextProbe.CURRENT.remove();
      app.close();
    }
    return seen;
  }

  @Test
  void withoutPropagatorContextIsLostAfterAWait() {
    Map<String, String> seen = runWaitFlow(ContextPropagator.NOOP);
    assertThat(seen.get("before")).isEqualTo(CONTEXT);
    assertThat(seen.get("after")).isEqualTo("null"); // documents the gap
  }

  @Test
  void withPropagatorContextSurvivesAWait() {
    Map<String, String> seen = runWaitFlow(ContextProbe.PROPAGATOR);
    assertThat(seen.get("before")).isEqualTo(CONTEXT);
    assertThat(seen.get("after")).isEqualTo(CONTEXT); // the fix
  }

  // ===================== emit =====================

  private static Map<String, String> runEmitFlow(ContextPropagator propagator) throws Exception {
    Map<String, String> seen = new ConcurrentHashMap<>();
    // A publisher that completes on a thread the SDK does not own (e.g. a Kafka producer callback).
    ExecutorService foreign =
        Executors.newSingleThreadExecutor(r -> new Thread(r, "foreign-publisher"));
    EventPublisher foreignPublisher =
        new EventPublisher() {
          @Override
          public CompletableFuture<Void> publish(CloudEvent event) {
            return CompletableFuture.runAsync(() -> {}, foreign);
          }

          @Override
          public void close() {}
        };

    Workflow workflow =
        WorkflowBuilder.workflow("emit-ctx", "test", "0.1.0")
            .tasks(DSL.emit("com.example.test"), DSL.set("afterEmit", s -> s.put("done", true)))
            .build();

    WorkflowApplication app =
        WorkflowApplication.builder()
            .disableLifeCycleCEPublishing()
            .withContextPropagator(propagator)
            .withEventPublisher(foreignPublisher)
            .withListener(ContextProbe.onTaskStarted(seen))
            .build();
    try {
      ContextProbe.CURRENT.set(CONTEXT);
      app.workflowDefinition(workflow).instance(Map.of()).start().join();
    } finally {
      ContextProbe.CURRENT.remove();
      app.close();
      foreign.shutdownNow();
    }
    return seen;
  }

  @Test
  void withoutPropagatorContextIsLostAfterAnEmit() throws Exception {
    Map<String, String> seen = runEmitFlow(ContextPropagator.NOOP);
    assertThat(seen.get("afterEmit")).isEqualTo("null"); // documents the gap
  }

  @Test
  void withPropagatorContextSurvivesAnEmit() throws Exception {
    Map<String, String> seen = runEmitFlow(ContextProbe.PROPAGATOR);
    assertThat(seen.get("afterEmit")).isEqualTo(CONTEXT); // the fix
  }

  // ===================== listen =====================

  private static Map<String, String> runListenFlow(ContextPropagator propagator) {
    Map<String, String> seen = new ConcurrentHashMap<>();
    Workflow workflow =
        WorkflowBuilder.workflow("listen-ctx", "test", "0.1.0")
            .tasks(
                DSL.listen(
                    "waitForEvent",
                    l -> l.to(to -> to.one(f -> f.with(p -> p.type("com.example.test"))))),
                DSL.set("afterListen", s -> s.put("done", true)))
            .build();

    WorkflowApplication app =
        WorkflowApplication.builder()
            .disableLifeCycleCEPublishing()
            .withContextPropagator(propagator)
            .withListener(ContextProbe.onTaskStarted(seen))
            .build();
    try {
      // The listener instance is started with the request context...
      ContextProbe.CURRENT.set(CONTEXT);
      WorkflowInstance instance = app.workflowDefinition(workflow).instance(Map.of());
      CompletableFuture<?> future = instance.start();
      // ...but the event is delivered later, from a thread WITHOUT that context.
      ContextProbe.CURRENT.remove();
      await().atMost(Duration.ofSeconds(3)).until(() -> seen.containsKey("waitForEvent"));
      app.eventPublishers().forEach(p -> p.publish(cloudEvent("com.example.test")));
      future.join();
    } finally {
      ContextProbe.CURRENT.remove();
      app.close();
    }
    return seen;
  }

  @Test
  void withoutPropagatorContextIsLostAfterAListen() {
    Map<String, String> seen = runListenFlow(ContextPropagator.NOOP);
    assertThat(seen.get("afterListen")).isEqualTo("null"); // documents the gap
  }

  @Test
  void withPropagatorContextSurvivesAListen() {
    Map<String, String> seen = runListenFlow(ContextProbe.PROPAGATOR);
    // The restored context is the listener's start context, not the (empty) deliverer's.
    assertThat(seen.get("afterListen")).isEqualTo(CONTEXT); // the fix
  }

  private static CloudEvent cloudEvent(String type) {
    return CloudEventBuilder.v1()
        .withId("1")
        .withType(type)
        .withSource(URI.create("https://www.example.com"))
        .withData(JsonCloudEventData.wrap(JsonUtils.fromValue(Map.of("hello", "world"))))
        .build();
  }
}
