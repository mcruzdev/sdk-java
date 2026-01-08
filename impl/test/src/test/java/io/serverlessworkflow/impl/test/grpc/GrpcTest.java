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

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.serverlessworkflow.api.WorkflowReader;
import io.serverlessworkflow.api.types.Workflow;
import io.serverlessworkflow.impl.WorkflowApplication;
import io.serverlessworkflow.impl.WorkflowDefinition;
import io.serverlessworkflow.impl.WorkflowInstance;
import io.serverlessworkflow.impl.test.generated.Person;
import java.io.IOException;
import java.util.Map;
import java.util.Optional;
import org.grpcmock.GrpcMock;
import org.grpcmock.junit5.GrpcMockExtension;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(GrpcMockExtension.class)
public class GrpcTest {

  private static final int PORT_FOR_EXAMPLES = 5011;
  private static WorkflowApplication app;

  private ManagedChannel channel;

  @BeforeAll
  static void setUpApp() {
    app = WorkflowApplication.builder().build();
  }

  @BeforeEach
  void setUp() {
    channel =
        ManagedChannelBuilder.forAddress("localhost", PORT_FOR_EXAMPLES).usePlaintext().build();

    io.serverlessworkflow.impl.test.generated.PersonServiceGrpc.newBlockingStub(channel);
  }

  @AfterEach
  void cleanup() {
    Optional.ofNullable(channel).ifPresent(ManagedChannel::shutdownNow);
  }

  @Test
  void grpcPerson() throws IOException {

    Workflow workflow =
        WorkflowReader.readWorkflowFromClasspath("workflows-samples/grpc/get-user-call.yaml");

    WorkflowDefinition workflowDefinition = app.workflowDefinition(workflow);

    String output = workflowDefinition.instance(Map.of())
            .start().join().asText().orElseThrow();

    GrpcMock.verifyThat(
        GrpcMock.calledMethod(
                io.serverlessworkflow.impl.test.generated.PersonServiceGrpc.getSayHelloMethod())
            .withRequest(Person.GetPersonRequest.newBuilder().build()));
  }
}
