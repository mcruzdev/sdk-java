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
package io.serverlessworkflow.impl;

import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;

/**
 * A reified capture of the context of the thread that started a workflow instance, re-applicable on
 * any other thread.
 *
 * <p>Implementations only need to provide {@link #wrap(Supplier)}; the {@link #wrap(Runnable)} and
 * {@link #wrap(CompletableFuture)} helpers are derived from it. {@link #wrap(Supplier)} must
 * capture its context eagerly (at {@link ContextPropagator#capture()} time) so that it
 * re-establishes the starting thread's context regardless of which - possibly foreign - thread
 * later invokes it.
 */
public interface ContextSnapshot {

  /** Decorates {@code supplier} so it runs with the captured context applied, then restored. */
  <T> Supplier<T> wrap(Supplier<T> supplier);

  /** Decorates {@code task} so it runs with the captured context applied, then restored. */
  default Runnable wrap(Runnable task) {
    Supplier<Void> wrapped =
        wrap(
            () -> {
              task.run();
              return null;
            });
    return wrapped::get;
  }

  /**
   * Re-anchors the continuation of {@code source} onto the captured context.
   *
   * <p>The returned future completes with the same value (or exception) as {@code source}, but its
   * dependent stages run with the captured context applied - even when {@code source} is completed
   * by a thread the SDK does not own (a timer thread for {@code wait}, an event-consumer thread for
   * {@code listen}, an external publisher thread for {@code emit}, or a gRPC I/O thread). Because
   * {@link CompletableFuture#complete(Object)} drives dependent stages synchronously, the whole
   * downstream pipeline (output filters, lifecycle listeners, the transition to the next task)
   * inherits the context.
   */
  default <T> CompletableFuture<T> wrap(CompletableFuture<T> source) {
    CompletableFuture<T> target = new CompletableFuture<>();
    source.whenComplete(
        (value, error) ->
            wrap(() -> {
                  if (error != null) {
                    target.completeExceptionally(error);
                  } else {
                    target.complete(value);
                  }
                })
                .run());
    return target;
  }

  ContextSnapshot NOOP =
      new ContextSnapshot() {
        @Override
        public <T> Supplier<T> wrap(Supplier<T> supplier) {
          return supplier;
        }

        @Override
        public Runnable wrap(Runnable task) {
          return task;
        }

        @Override
        public <T> CompletableFuture<T> wrap(CompletableFuture<T> source) {
          return source;
        }
      };
}
