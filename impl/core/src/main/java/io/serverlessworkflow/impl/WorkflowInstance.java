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

import static io.serverlessworkflow.impl.json.JsonUtils.toJavaValue;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.NullNode;
import java.time.Instant;

public class WorkflowInstance {
  private WorkflowState state;
  private TaskContext<?> taskContext;
  private final String id;
  private final JsonNode input;
  private final Instant startedAt;
  private JsonNode context = NullNode.getInstance();

  WorkflowInstance(WorkflowDefinition definition, JsonNode input) {
    this.id = definition.idFactory().get();
    this.input = input;
    definition.inputSchemaValidator().ifPresent(v -> v.validate(input));
    this.startedAt = Instant.now();
    WorkflowContext workflowContext = new WorkflowContext(definition, this);
    taskContext = new TaskContext<>(input, definition.positionFactory().get());
    definition
        .inputFilter()
        .ifPresent(f -> taskContext.input(f.apply(workflowContext, taskContext, input)));
    state = WorkflowState.STARTED;
    taskContext.rawOutput(
        WorkflowUtils.processTaskList(definition.workflow().getDo(), workflowContext, taskContext));
    definition
        .outputFilter()
        .ifPresent(
            f ->
                taskContext.output(f.apply(workflowContext, taskContext, taskContext.rawOutput())));
    definition.outputSchemaValidator().ifPresent(v -> v.validate(taskContext.output()));
  }

  public String id() {
    return id;
  }

  public Instant startedAt() {
    return startedAt;
  }

  public JsonNode input() {
    return input;
  }

  public JsonNode context() {
    return context;
  }

  public WorkflowState state() {
    return state;
  }

  public Object output() {
    return toJavaValue(taskContext.output());
  }

  public Object outputAsJsonNode() {
    return taskContext.output();
  }

  void context(JsonNode context) {
    this.context = context;
  }
}
