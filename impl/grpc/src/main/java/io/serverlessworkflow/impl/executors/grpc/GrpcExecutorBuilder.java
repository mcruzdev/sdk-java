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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.NullNode;
import com.google.protobuf.DescriptorProtos;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import io.grpc.CallOptions;
import io.grpc.Channel;
import io.grpc.ClientCall;
import io.grpc.MethodDescriptor;
import io.grpc.protobuf.ProtoUtils;
import io.grpc.stub.ClientCalls;
import io.serverlessworkflow.api.types.CallGRPC;
import io.serverlessworkflow.api.types.ExternalResource;
import io.serverlessworkflow.api.types.GRPCArguments;
import io.serverlessworkflow.api.types.TaskBase;
import io.serverlessworkflow.api.types.WithGRPCService;
import io.serverlessworkflow.impl.TaskContext;
import io.serverlessworkflow.impl.WorkflowContext;
import io.serverlessworkflow.impl.WorkflowDefinition;
import io.serverlessworkflow.impl.WorkflowModel;
import io.serverlessworkflow.impl.WorkflowMutablePosition;
import io.serverlessworkflow.impl.WorkflowValueResolver;
import io.serverlessworkflow.impl.executors.CallableTask;
import io.serverlessworkflow.impl.executors.CallableTaskBuilder;
import io.serverlessworkflow.impl.jackson.JsonUtils;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GrpcExecutorBuilder implements CallableTaskBuilder<CallGRPC> {

  private static final Logger LOGGER = LoggerFactory.getLogger(GrpcExecutorBuilder.class);

  private ExternalResource proto;
  private GrpcRequestContext grpcRequestContext;
  private Map<String, Object> arguments;
  private GrpcCallExecutor callExecutor;
  private FileDescriptorContextSupplier fileDescriptorContextSupplier;
  private WorkflowValueResolver<URI> protoUriSupplier;

  @Override
  public boolean accept(Class<? extends TaskBase> clazz) {
    return clazz.equals(CallGRPC.class);
  }

  @Override
  public void init(CallGRPC task, WorkflowDefinition definition, WorkflowMutablePosition position) {

    GRPCArguments with = task.getWith();
    WithGRPCService service = with.getService();
    this.proto = with.getProto();

    this.arguments =
        with.getArguments() != null && with.getArguments().getAdditionalProperties() != null
            ? with.getArguments().getAdditionalProperties()
            : Map.of();

    this.grpcRequestContext =
        new GrpcRequestContext(
            service.getHost(), service.getPort(), with.getMethod(), service.getName(), arguments);

    FileDescriptorReader fileDescriptorReader = new FileDescriptorReader();

    this.fileDescriptorContextSupplier =
        (WorkflowContext workflowContext, TaskContext taskContext, WorkflowModel workflowModel) ->
            definition
                .resourceLoader()
                .load(
                    with.getProto().getEndpoint(),
                    fileDescriptorReader::readDescriptor,
                    workflowContext,
                    taskContext,
                    workflowModel);
    this.callExecutor =
        (fileDescriptorContext, requestContext, workflowContext, taskContext, model) -> {
          Channel channel =
              GrpcChannelResolver.channel(workflowContext, taskContext, this.grpcRequestContext);
          String protoName = fileDescriptorContext.inputProto();

          DescriptorProtos.FileDescriptorProto fileDescriptorProto =
              fileDescriptorContext.fileDescriptorSet().getFileList().stream()
                  .filter(
                      file ->
                          file.getName()
                              .equals(
                                  this.proto.getName() != null ? this.proto.getName() : protoName))
                  .findFirst()
                  .orElseThrow(
                      () -> new IllegalStateException("Proto file not found in descriptor set"));

          try {
            Descriptors.FileDescriptor fileDescriptor =
                Descriptors.FileDescriptor.buildFrom(
                    fileDescriptorProto, new Descriptors.FileDescriptor[] {});
            Descriptors.ServiceDescriptor serviceDescriptor =
                fileDescriptor.findServiceByName(this.grpcRequestContext.service());

            Objects.requireNonNull(
                serviceDescriptor, "Service not found: " + this.grpcRequestContext.service());

            Descriptors.MethodDescriptor methodDescriptor =
                serviceDescriptor.findMethodByName(this.grpcRequestContext.method());

            Objects.requireNonNull(
                methodDescriptor, "Method not found: " + this.grpcRequestContext.method());

            MethodDescriptor.MethodType methodType =
                ProtobufMessageUtils.getMethodType(methodDescriptor);

            ClientCall<Message, Message> call =
                channel.newCall(
                    io.grpc.MethodDescriptor.<Message, Message>newBuilder()
                        .setType(methodType)
                        .setFullMethodName(
                            io.grpc.MethodDescriptor.generateFullMethodName(
                                serviceDescriptor.getFullName(), methodDescriptor.getName()))
                        .setRequestMarshaller(
                            ProtoUtils.marshaller(
                                DynamicMessage.newBuilder(methodDescriptor.getInputType())
                                    .buildPartial()))
                        .setResponseMarshaller(
                            ProtoUtils.marshaller(
                                DynamicMessage.newBuilder(methodDescriptor.getOutputType())
                                    .buildPartial()))
                        .build(),
                    CallOptions.DEFAULT.withWaitForReady());

            return switch (methodType) {
              case CLIENT_STREAMING ->
                  handleClientStreaming(workflowContext, arguments, methodDescriptor, call);
              case BIDI_STREAMING ->
                  handleBidiStreaming(workflowContext, arguments, methodDescriptor, call);
              case SERVER_STREAMING ->
                  handleServerStreaming(workflowContext, methodDescriptor, arguments, call);
              case UNARY, UNKNOWN ->
                  handleBlockingUnary(workflowContext, methodDescriptor, arguments, call);
            };

          } catch (Descriptors.DescriptorValidationException
              | InvalidProtocolBufferException
              | JsonProcessingException e) {
            throw new RuntimeException(e);
          }
        };
  }

  private static WorkflowModel handleClientStreaming(
      WorkflowContext workflowContext,
      Map<String, Object> parameters,
      Descriptors.MethodDescriptor methodDescriptor,
      ClientCall<Message, Message> call) {
    JsonNode jsonNode =
        ProtobufMessageUtils.asyncStreamingCall(
            parameters,
            methodDescriptor,
            responseObserver -> ClientCalls.asyncClientStreamingCall(call, responseObserver),
            nodes -> nodes.isEmpty() ? NullNode.instance : nodes.get(0));
    return workflowContext.definition().application().modelFactory().fromAny(jsonNode);
  }

  private static WorkflowModel handleServerStreaming(
      WorkflowContext workflowContext,
      Descriptors.MethodDescriptor methodDescriptor,
      Map<String, Object> parameters,
      ClientCall<Message, Message> call)
      throws InvalidProtocolBufferException, JsonProcessingException {
    Message.Builder builder = ProtobufMessageUtils.buildMessage(methodDescriptor, parameters);
    List<JsonNode> nodes = new ArrayList<>();
    ClientCalls.blockingServerStreamingCall(call, builder.build())
        .forEachRemaining(message -> nodes.add(ProtobufMessageUtils.convert(message)));
    return workflowContext.definition().application().modelFactory().fromAny(nodes);
  }

  private static WorkflowModel handleBlockingUnary(
      WorkflowContext workflowContext,
      Descriptors.MethodDescriptor methodDescriptor,
      Map<String, Object> parameters,
      ClientCall<Message, Message> call)
      throws InvalidProtocolBufferException, JsonProcessingException {
    Message.Builder builder = ProtobufMessageUtils.buildMessage(methodDescriptor, parameters);

    Message message = ClientCalls.blockingUnaryCall(call, builder.build());
    return workflowContext
        .definition()
        .application()
        .modelFactory()
        .fromAny(ProtobufMessageUtils.convert(message));
  }

  private static WorkflowModel handleBidiStreaming(
      WorkflowContext workflowContext,
      Map<String, Object> parameters,
      Descriptors.MethodDescriptor methodDescriptor,
      ClientCall<Message, Message> call) {
    return workflowContext
        .definition()
        .application()
        .modelFactory()
        .fromAny(
            ProtobufMessageUtils.asyncStreamingCall(
                parameters,
                methodDescriptor,
                responseObserver -> ClientCalls.asyncBidiStreamingCall(call, responseObserver),
                v -> {
                  Collection<JsonNode> nodes = v;
                  List<JsonNode> list = new ArrayList<>(nodes);
                  return JsonUtils.fromValue(list);
                }));
  }

  @Override
  public CallableTask build() {
    return new GrpcExecutor(
        this.grpcRequestContext, this.callExecutor, this.fileDescriptorContextSupplier);
  }
}
