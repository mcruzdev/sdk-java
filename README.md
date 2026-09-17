![Verify JAVA SDK](https://github.com/serverlessworkflow/sdk-java/workflows/Verify%20JAVA%20SDK/badge.svg)
![Deploy JAVA SDK](https://github.com/serverlessworkflow/sdk-java/workflows/Deploy%20JAVA%20SDK/badge.svg) [![Gitpod ready-to-code](https://img.shields.io/badge/Gitpod-ready--to--code-blue?logo=gitpod)](https://gitpod.io/#https://github.com/serverlessworkflow/sdk-java)

# Serverless Workflow Specification - Java SDK

Provides the Java API/SPI and Model Validation for the [Serverless Workflow Specification](https://github.com/serverlessworkflow/specification)

With the SDK you can:

* Parse workflow JSON and YAML definitions
* Programmatically build workflow definitions
* Validate workflow definitions (both schema and workflow integrity validation)
* Set of utilities to help runtimes interpret the Serverless Workflow object model

Serverless Workflow Java SDK is **not** a workflow runtime implementation but can be used by Java runtime implementations
to parse and validate workflow definitions.

> **Note:** The `serverlessworkflow-diagram` module (workflow-to-SVG diagram generation) and the `diagram-rest` addon
> have been removed. They depended on [PlantUML](https://plantuml.com/), which is GPL-3.0-licensed and incompatible
> with distributing this project's Apache-2.0-licensed artifacts.

### Status

| Latest Releases | Conformance to spec version |
| :---: | :---: |
| [5.1.0.Final](https://github.com/serverlessworkflow/sdk-java/releases/tag/5.1.0.Final) | [v0.8](https://github.com/serverlessworkflow/specification/tree/0.8.x) |
| [4.0.5.Final](https://github.com/serverlessworkflow/sdk-java/releases/tag/4.0.5.Final) | [v0.8](https://github.com/serverlessworkflow/specification/tree/0.8.x) |
| [3.0.0.Final](https://github.com/serverlessworkflow/sdk-java/releases/tag/3.0.0.Final) | [v0.7](https://github.com/serverlessworkflow/specification/tree/0.7.x) |
| [2.0.0.Final](https://github.com/serverlessworkflow/sdk-java/releases/tag/2.0.0.Final) | [v0.6](https://github.com/serverlessworkflow/specification/tree/0.6.x) |
| [1.0.3.Final](https://github.com/serverlessworkflow/sdk-java/releases/tag/1.0.3.Final) | [v0.5](https://github.com/serverlessworkflow/specification/tree/0.5.x) |

### JDK Version

| SDK Version | JDK Version |
| :---: |:-----------:|
| 5.0.0 and after |     17      |
| 4.0.x and before |      8      | 

### Getting Started

#### Using the latest release

See instructions how to define dependencies for the latest SDK release
for both Maven and Gradle [here](https://github.com/serverlessworkflow/sdk-java/blob/4.0.x/README.md).

#### Building SNAPSHOT locally

To build project and run tests locally:

```
git clone https://github.com/serverlessworkflow/sdk-java.git
mvn clean install
```

The project uses [Google's code styleguide](https://google.github.io/styleguide/javaguide.html).
Your changes should be automatically formatted during the build.

#### Maven projects:

a) Add the following repository to your pom.xml `repositories` section:

```xml
<repository>
    <id>oss.sonatype.org-snapshot</id>
    <url>http://oss.sonatype.org/content/repositories/snapshots</url>
    <releases>
        <enabled>false</enabled>
    </releases>
    <snapshots>
        <enabled>true</enabled>
    </snapshots>
</repository>
```

b) Add the following dependencies to your pom.xml `dependencies` section:

```xml
<dependency>
    <groupId>io.serverlessworkflow</groupId>
    <artifactId>serverlessworkflow-api</artifactId>
    <version>5.1.0.Final</version>
</dependency>

<dependency>
    <groupId>io.serverlessworkflow</groupId>
    <artifactId>serverlessworkflow-spi</artifactId>
    <version>5.1.0.Final</version>
</dependency>

<dependency>
    <groupId>io.serverlessworkflow</groupId>
    <artifactId>serverlessworkflow-validation</artifactId>
    <version>5.1.0.Final</version>
</dependency>

<dependency>
    <groupId>io.serverlessworkflow</groupId>
    <artifactId>serverlessworkflow-util</artifactId>
    <version>5.1.0.Final</version>
</dependency>
```

#### Gradle projects:

a) Add the following repositories to your build.gradle `repositories` section:

```text
maven { url "https://oss.sonatype.org/content/repositories/snapshots" }
```

b) Add the following dependencies to your build.gradle `dependencies` section:

```text
implementation("io.serverlessworkflow:serverlessworkflow-api:5.1.0.Final")
implementation("io.serverlessworkflow:serverlessworkflow-spi:5.1.0.Final")
implementation("io.serverlessworkflow:serverlessworkflow-validation:5.1.0.Final")
implementation("io.serverlessworkflow:serverlessworkflow-util:5.1.0.Final")
```

### How to Use 

#### Creating from JSON/YAML source

You can create a Workflow instance from JSON/YAML source:

Let's say you have a simple YAML based workflow definition:

```yaml
id: greeting
version: '1.0'
name: Greeting Workflow
start: Greet
description: Greet Someone
functions:
  - name: greetingFunction
    operation: file://myapis/greetingapis.json#greeting
states:
- name: Greet
  type: operation
  actions:
  - functionRef:
      refName: greetingFunction
      arguments:
        name: "${ .greet.name }"
    actionDataFilter:
      results: "${ .payload.greeting }"
  stateDataFilter:
    output: "${ .greeting }"
  end: true
```

To parse it and create a Workflow instance you can do:

``` java
Workflow workflow = Workflow.fromSource(source);
```

where 'source' is the above mentioned YAML definition.

The fromSource static method can take in definitions in both JSON and YAML formats.

Once you have the Workflow instance you can use its API to inspect it, for example:

``` java
assertNotNull(workflow);
assertEquals("greeting", workflow.getId());
assertEquals("Greeting Workflow", workflow.getName());

assertNotNull(workflow.getFunctions());
assertEquals(1, workflow.getFunctions().size());
assertEquals("greetingFunction", workflow.getFunctions().get(0).getName());

assertNotNull(workflow.getStates());
assertEquals(1, workflow.getStates().size());
assertTrue(workflow.getStates().get(0) instanceof OperationState);

OperationState operationState = (OperationState) workflow.getStates().get(0);
assertEquals("Greet", operationState.getName());
assertEquals(DefaultState.Type.OPERATION, operationState.getType());
...
```

#### Using builder API

You can also programmatically create Workflow instances, for example:

``` java
Workflow workflow = new Workflow()
                .withId("test-workflow")
                .withName("test-workflow-name")
                .withVersion("1.0")
                .withStart(new Start().withStateName("MyDelayState"))
                .withFunctions(new Functions(Arrays.asList(
                        new FunctionDefinition().withName("testFunction")
                                .withOperation("testSwaggerDef#testOperationId")))
                )
                .withStates(Arrays.asList(
                        new DelayState().withName("MyDelayState").withType(DELAY)
                                .withTimeDelay("PT1M")
                                .withEnd(
                                        new End().withTerminate(true)
                                )
                        )
                );
```

This will create a test workflow that defines an event, a function and a single Delay State.

You can use the workflow instance to get its JSON/YAML definition as well:

``` java
assertNotNull(Workflow.toJson(testWorkflow));
assertNotNull(Workflow.toYaml(testWorkflow));
```

#### Using Workflow Validation

Validation allows you to perform Json Schema validation against the JSON/YAML workflow definitions.
Once you have a `Workflow` instance, you can also run integrity checks.

You can validate a Workflow JSON/YAML definition to get validation errors:

``` java
WorkflowValidator workflowValidator = new WorkflowValidatorImpl();
List<ValidationError> validationErrors = workflowValidator.setSource("WORKFLOW_MODEL_JSON/YAML").validate();
```

Where `WORKFLOW_MODEL_JSON/YAML` is the actual workflow model JSON or YAML definition.

Or you can just check if it is valid (without getting specific errors):

``` java
WorkflowValidator workflowValidator = new WorkflowValidatorImpl();
boolean isValidWorkflow = workflowValidator.setSource("WORKFLOW_MODEL_JSON/YAML").isValid();
```

If you build your Workflow programmatically, you can validate it as well:

``` java
Workflow workflow = new Workflow()
                .withId("test-workflow")
                .withVersion("1.0")
                .withStart(new Start().withStateName("MyDelayState"))
                .withStates(Arrays.asList(
                        new DelayState().withName("MyDelayState").withType(DefaultState.Type.DELAY)
                                .withTimeDelay("PT1M")
                                .withEnd(
                                        new End().withTerminate(true)
                                )
                ));
);

WorkflowValidator workflowValidator = new WorkflowValidatorImpl();
List<ValidationError> validationErrors = workflowValidator.setWorkflow(workflow).validate();
```

#### Using Workflow Utils
Workflow utils provide a number of useful methods for extracting information from workflow definitions.
Once you have a `Workflow` instance, you can use it
##### Get Starting State
```Java
State startingState = WorkflowUtils.getStartingState(workflow);
```
##### Get States by State Type
```Java
    List<State> states = WorkflowUtils.getStates(workflow, DefaultState.Type.EVENT);
```
##### Get Consumed-Events, Produced-Events and their count
```Java
 List<EventDefinition> consumedEvents = WorkflowUtils.getWorkflowConsumedEvents(workflow);
 int consumedEventsCount = WorkflowUtils.getWorkflowConsumedEventsCount(workflow);

 List<EventDefinition> producedEvents = WorkflowUtils.getWorkflowProducedEvents(workflow);
 int producedEventsCount = WorkflowUtils.getWorkflowProducedEventsCount(workflow);
 ```
##### Get Defined Consumed-Events, Defined Produced-Events and their count
```Java
 List<EventDefinition> consumedEvents = WorkflowUtils.getWorkflowConsumedEventsCount(workflow);
 int consumedEventsCount = WorkflowUtils.getWorkflowConsumedEventsCount(workflow);

 List<EventDefinition> producedEvents = WorkflowUtils.getWorkflowProducedEvents(workflow);
 int producedEventsCount = WorkflowUtils.getWorkflowProducedEventsCount(workflow);
 ```
##### Get Function definitions which is used by an action
```Java
FunctionDefinition finalizeApplicationFunctionDefinition =
        WorkflowUtils.getFunctionDefinitionsForAction(workflow, "finalizeApplicationAction");
```
##### Get Actions which uses a Function definition
```Java
 List<Action> actionsForFunctionDefinition =
        WorkflowUtils.getActionsForFunctionDefinition(workflow, functionRefName);
```


