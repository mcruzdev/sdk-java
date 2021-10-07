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
package io.serverlessworkflow.utils;

import io.serverlessworkflow.api.Workflow;
import io.serverlessworkflow.api.events.EventDefinition;
import io.serverlessworkflow.api.interfaces.State;
import io.serverlessworkflow.api.start.Start;
import io.serverlessworkflow.api.states.DefaultState;
import java.util.List;
import java.util.stream.Collectors;

/** Provides common utility methods to provide most often needed answers from a workflow */
public final class WorkflowUtils {
  private static final int DEFAULT_STARTING_STATE_POSITION = 0;

  /**
   * Gets State matching Start state.If start is not present returns first state otherwise returns
   * null
   *
   * @param workflow workflow
   * @return {@code state} when present else returns {@code null}
   */
  public static State getStartingState(Workflow workflow) {
    if (workflow == null || workflow.getStates() == null || workflow.getStates().isEmpty()) {
      return null;
    }

    Start start = workflow.getStart();
    if (start == null) {
      return workflow.getStates().get(DEFAULT_STARTING_STATE_POSITION);
    } else {
      return workflow.getStates().stream()
          .filter(state -> state.getName().equals(start.getStateName()))
          .findFirst()
          .get();
    }
  }

  /**
   * Gets List of States matching stateType
   *
   * @param workflow
   * @param stateType
   * @return {@code List<State>}. Returns {@code null} when workflow is null.
   */
  public static List<State> getStates(Workflow workflow, DefaultState.Type stateType) {
    if (workflow == null || workflow.getStates() == null) {
      return null;
    }

    return workflow.getStates().stream()
        .filter(state -> state.getType() == stateType)
        .collect(Collectors.toList());
  }

  /**
   * @return {@code List<io.serverlessworkflow.api.events.EventDefinition>}. Returns {@code NULL}
   *     when workflow is null or when workflow does not contain events
   */
  public static List<EventDefinition> getDefinedConsumedEvents(Workflow workflow) {
    return getDefinedEvents(workflow, EventDefinition.Kind.CONSUMED);
  }

  /**
   * @return {@code List<io.serverlessworkflow.api.events.EventDefinition>}. Returns {@code NULL}
   *     when workflow is null or when workflow does not contain events
   */
  public static List<EventDefinition> getDefinedProducedEvents(Workflow workflow) {
    return getDefinedEvents(workflow, EventDefinition.Kind.PRODUCED);
  }

  /**
   * Gets list of event definition matching eventKind
   *
   * @param workflow
   * @return {@code List<io.serverlessworkflow.api.events.EventDefinition>}. Returns {@code NULL}
   *     when workflow is null or when workflow does not contain events
   */
  public static List<EventDefinition> getDefinedEvents(
      Workflow workflow, EventDefinition.Kind eventKind) {
    if (workflow == null || workflow.getEvents() == null) {
      return null;
    }
    List<EventDefinition> eventDefs = workflow.getEvents().getEventDefs();
    if (eventDefs == null) {
      return null;
    }

    return eventDefs.stream()
        .filter(eventDef -> eventDef.getKind() == eventKind)
        .collect(Collectors.toList());
  }

  /** @return {@code int} Returns count of defined event count matching eventKind */
  public static int getDefinedEventsCount(Workflow workflow, EventDefinition.Kind eventKind) {
    List<EventDefinition> definedEvents = getDefinedEvents(workflow, eventKind);
    return definedEvents == null ? 0 : definedEvents.size();
  }

  /** @return {@code int} Returns count of Defined Consumed Event Count */
  public static int getDefinedConsumedEventsCount(Workflow workflow) {
    return getDefinedEventsCount(workflow, EventDefinition.Kind.CONSUMED);
  }

  /** @return {@code int} Returns count of Defined Produced Event Count */
  public static int getDefinedProducedEventsCount(Workflow workflow) {
    return getDefinedEventsCount(workflow, EventDefinition.Kind.PRODUCED);
  }
}
