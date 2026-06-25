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
package io.serverlessworkflow.impl.test.grpc;

import static org.assertj.core.api.Assertions.assertThat;

import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.serverlessworkflow.api.WorkflowReader;
import io.serverlessworkflow.api.types.Workflow;
import io.serverlessworkflow.impl.ContextPropagator;
import io.serverlessworkflow.impl.WorkflowApplication;
import io.serverlessworkflow.impl.WorkflowDefinition;
import io.serverlessworkflow.impl.test.ctx.ContextProbe;
import io.serverlessworkflow.impl.test.grpc.handlers.PersonUnaryHandler;
import io.serverlessworkflow.impl.test.junit.DisabledIfProtocUnavailable;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Proves that the {@link ContextPropagator} hook keeps the starting thread's context alive across a
 * gRPC call, whose result future is completed on a gRPC I/O thread. The {@code greet} task in
 * {@code get-person-call.yaml} completes on that foreign thread, so its {@code onTaskCompleted}
 * (and everything downstream) only sees the context when it is restored by the propagator.
 */
@DisabledIfProtocUnavailable
public class GrpcContextPropagationTest {

  private static final int PORT = 5011; // must match the port in get-person-call.yaml
  private static final String CONTEXT = "tenant-42";

  private Server server;

  @BeforeEach
  void setUp() throws IOException {
    server = ServerBuilder.forPort(PORT).addService(new PersonUnaryHandler()).build();
    server.start();
  }

  @AfterEach
  void cleanup() throws InterruptedException {
    server.shutdown().awaitTermination();
  }

  private Map<String, String> runGrpcFlow(ContextPropagator propagator) throws IOException {
    Map<String, String> seen = new ConcurrentHashMap<>();
    Workflow workflow =
        WorkflowReader.readWorkflowFromClasspath("workflows-samples/grpc/get-person-call.yaml");
    String protoFile =
        getClass()
            .getClassLoader()
            .getResource("workflows-samples/grpc/proto/person.proto")
            .getFile();

    WorkflowApplication app =
        WorkflowApplication.builder()
            .withContextPropagator(propagator)
            .withListener(ContextProbe.onTaskCompleted(seen))
            .build();
    try {
      WorkflowDefinition definition = app.workflowDefinition(workflow);
      ContextProbe.CURRENT.set(CONTEXT);
      definition.instance(Map.of("protoFilePath", "file://" + protoFile)).start().join();
    } finally {
      ContextProbe.CURRENT.remove();
      app.close();
    }
    return seen;
  }

  @Test
  void withoutPropagatorContextIsLostAfterAGrpcCall() throws IOException {
    Map<String, String> seen = runGrpcFlow(ContextPropagator.NOOP);
    assertThat(seen.get("greet")).isEqualTo("null"); // documents the gap
  }

  @Test
  void withPropagatorContextSurvivesAGrpcCall() throws IOException {
    Map<String, String> seen = runGrpcFlow(ContextProbe.PROPAGATOR);
    assertThat(seen.get("greet")).isEqualTo(CONTEXT); // the fix
  }
}
