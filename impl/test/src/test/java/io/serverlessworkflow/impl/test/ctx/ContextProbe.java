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
package io.serverlessworkflow.impl.test.ctx;

import io.serverlessworkflow.impl.ContextPropagator;
import io.serverlessworkflow.impl.ContextSnapshot;
import io.serverlessworkflow.impl.lifecycle.TaskCompletedEvent;
import io.serverlessworkflow.impl.lifecycle.TaskStartedEvent;
import io.serverlessworkflow.impl.lifecycle.WorkflowExecutionListener;
import java.util.Map;
import java.util.function.Supplier;

/**
 * Shared test helpers for context-propagation tests. {@link #CURRENT} stands in for Quarkus
 * request-scoped state (CDI request context + {@code SecurityIdentity}) carried on a {@link
 * ThreadLocal}; {@link #PROPAGATOR} mimics the Quarkus MicroProfile-backed {@link
 * ContextPropagator}: it captures the value eagerly at {@link ContextPropagator#capture()} time and
 * re-applies it on whatever thread later runs the wrapped work.
 */
public final class ContextProbe {

  private ContextProbe() {}

  public static final ThreadLocal<String> CURRENT = new ThreadLocal<>();

  public static final ContextPropagator PROPAGATOR =
      () -> {
        final String captured = CURRENT.get(); // reified at capture() time (the start thread)
        return new ContextSnapshot() {
          @Override
          public <T> Supplier<T> wrap(Supplier<T> supplier) {
            return () -> {
              final String previous = CURRENT.get();
              CURRENT.set(captured);
              try {
                return supplier.get();
              } finally {
                if (previous == null) {
                  CURRENT.remove();
                } else {
                  CURRENT.set(previous);
                }
              }
            };
          }
        };
      };

  /** Records {@link #CURRENT} as seen when each task <em>starts</em>, keyed by task name. */
  public static WorkflowExecutionListener onTaskStarted(Map<String, String> seen) {
    return new WorkflowExecutionListener() {
      @Override
      public void onTaskStarted(TaskStartedEvent ev) {
        seen.put(ev.taskContext().taskName(), String.valueOf(CURRENT.get()));
      }
    };
  }

  /** Records {@link #CURRENT} as seen when each task <em>completes</em>, keyed by task name. */
  public static WorkflowExecutionListener onTaskCompleted(Map<String, String> seen) {
    return new WorkflowExecutionListener() {
      @Override
      public void onTaskCompleted(TaskCompletedEvent ev) {
        seen.put(ev.taskContext().taskName(), String.valueOf(CURRENT.get()));
      }
    };
  }
}
