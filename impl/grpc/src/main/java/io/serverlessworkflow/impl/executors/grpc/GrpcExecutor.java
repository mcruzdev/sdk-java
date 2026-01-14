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
package io.serverlessworkflow.impl.executors.grpc;

import io.serverlessworkflow.impl.TaskContext;
import io.serverlessworkflow.impl.WorkflowContext;
import io.serverlessworkflow.impl.WorkflowModel;
import io.serverlessworkflow.impl.executors.CallableTask;
import java.util.concurrent.CompletableFuture;

public class GrpcExecutor implements CallableTask {

  private final GrpcRequestContext requestContext;
  private final GrpcCallExecutor grpcCallExecutor;
  private final FileDescriptorContextSupplier fileDescriptorContextSupplier;

  public GrpcExecutor(
      GrpcRequestContext builder,
      GrpcCallExecutor grpcCallExecutor,
      FileDescriptorContextSupplier fileDescriptorContextSupplier) {
    this.requestContext = builder;
    this.grpcCallExecutor = grpcCallExecutor;
    this.fileDescriptorContextSupplier = fileDescriptorContextSupplier;
  }

  @Override
  public CompletableFuture<WorkflowModel> apply(
      WorkflowContext workflowContext, TaskContext taskContext, WorkflowModel input) {

    FileDescriptorContext fileDescriptorContext =
        this.fileDescriptorContextSupplier.get(workflowContext, taskContext, input);

    return CompletableFuture.supplyAsync(
        () ->
            this.grpcCallExecutor.apply(
                fileDescriptorContext, requestContext, workflowContext, taskContext, input));
  }
}
