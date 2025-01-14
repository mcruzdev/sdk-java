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
package io.serverlessworkflow.generator;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.sun.codemodel.JBlock;
import com.sun.codemodel.JClass;
import com.sun.codemodel.JClassAlreadyExistsException;
import com.sun.codemodel.JClassContainer;
import com.sun.codemodel.JConditional;
import com.sun.codemodel.JDefinedClass;
import com.sun.codemodel.JExpr;
import com.sun.codemodel.JFieldVar;
import com.sun.codemodel.JInvocation;
import com.sun.codemodel.JMethod;
import com.sun.codemodel.JMod;
import com.sun.codemodel.JPackage;
import com.sun.codemodel.JType;
import com.sun.codemodel.JVar;
import jakarta.validation.ConstraintViolationException;
import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.net.URLDecoder;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.regex.Pattern;
import org.jsonschema2pojo.Jsonschema2Pojo;
import org.jsonschema2pojo.Schema;
import org.jsonschema2pojo.exception.GenerationException;
import org.jsonschema2pojo.rules.RuleFactory;
import org.jsonschema2pojo.rules.SchemaRule;

class AllAnyOneOfSchemaRule extends SchemaRule {

  private RuleFactory ruleFactory;

  AllAnyOneOfSchemaRule(RuleFactory ruleFactory) {
    super(ruleFactory);
    this.ruleFactory = ruleFactory;
  }

  private static final String REF = "$ref";
  private static final String TITLE = "title";
  private static final String PATTERN = "pattern";

  private enum Format {
    URI_TEMPLATE("^[A-Za-z][A-Za-z0-9+\\-.]*://.*");

    private final String pattern;

    Format(String pattern) {
      this.pattern = pattern;
    }

    public static Format parse(String str) {
      if (str != null) {
        switch (str) {
          case "uri-template":
            return URI_TEMPLATE;
        }
      }
      return null;
    }

    String pattern() {

      return pattern;
    }
  }

  private static class JTypeWrapper implements Comparable<JTypeWrapper> {

    private final JType type;
    private final JsonNode node;

    public JTypeWrapper(JType type, JsonNode node) {
      this.type = type;
      this.node = node;
    }

    public JType getType() {
      return type;
    }

    public JsonNode getNode() {
      return node;
    }

    @Override
    public int compareTo(JTypeWrapper other) {
      return typeToNumber() - other.typeToNumber();
    }

    private int typeToNumber() {
      if (type.name().equals("Object")) {
        return 6;
      } else if (type.name().equals("String")) {
        return node.has(PATTERN) || node.has(REF) ? 4 : 5;
      } else if (type.isPrimitive()) {
        return 3;
      } else if (type.isReference()) {
        return 2;
      } else if (type.isArray()) {
        return 1;
      } else {
        return 0;
      }
    }
  }

  @Override
  public JType apply(
      String nodeName,
      JsonNode schemaNode,
      JsonNode parent,
      JClassContainer generatableType,
      Schema schema) {

    Optional<JType> refType = refType(nodeName, schemaNode, parent, generatableType, schema);
    List<JTypeWrapper> oneOfTypes = new ArrayList<>();
    List<JTypeWrapper> allOfTypes = new ArrayList<>();

    unionType("oneOf", nodeName, schemaNode, parent, generatableType, schema, oneOfTypes);
    unionType("anyOf", nodeName, schemaNode, parent, generatableType, schema, oneOfTypes);
    unionType("allOf", nodeName, schemaNode, parent, generatableType, schema, allOfTypes);

    Collections.sort(oneOfTypes);

    JType javaType;
    if (schemaNode.has("enum")) {
      javaType =
          ruleFactory.getEnumRule().apply(nodeName, schemaNode, parent, generatableType, schema);
    } else if (!schemaNode.has("properties")
        && oneOfTypes.isEmpty()
        && allOfTypes.isEmpty()
        && refType.isPresent()) {
      javaType = refType.get();
    } else if (!schemaNode.has("properties")
        && oneOfTypes.isEmpty()
        && allOfTypes.size() == 1
        && refType.isEmpty()) {
      javaType = allOfTypes.get(0).getType();
    } else if (!schemaNode.has("properties")
        && oneOfTypes.size() == 1
        && allOfTypes.isEmpty()
        && refType.isEmpty()) {
      javaType = oneOfTypes.get(0).getType();
    } else {
      JPackage container = generatableType.getPackage();
      javaType = ruleFactory.getTypeRule().apply(nodeName, schemaNode, parent, container, schema);
      if (javaType instanceof JDefinedClass) {
        javaType =
            populateAllOf(
                schema, populateRef((JDefinedClass) javaType, refType, schema), allOfTypes);
      }
      if (!oneOfTypes.isEmpty()) {
        try {
          JDefinedClass unionClass;
          Optional<JType> commonType;
          if (javaType instanceof JDefinedClass) {
            JDefinedClass clazz = (JDefinedClass) javaType;
            if (clazz.methods().isEmpty()) {
              unionClass = clazz;
              commonType = Optional.empty();
            } else {
              unionClass = container._class(clazz.name() + "Union");
              commonType = Optional.of(clazz);
            }
          } else {
            unionClass =
                container._class(
                    ruleFactory
                        .getNameHelper()
                        .getUniqueClassName(nodeName, schemaNode, container));
            commonType = Optional.empty();
          }
          javaType = populateOneOf(schema, unionClass, commonType, oneOfTypes);
        } catch (JClassAlreadyExistsException ex) {
          throw new IllegalStateException(ex);
        }
      }
      schema.setJavaType(javaType);
    }
    return javaType;
  }

  private JDefinedClass populateAllOf(
      Schema parentSchema, JDefinedClass definedClass, Collection<JTypeWrapper> allOfTypes) {
    return wrapAll(parentSchema, definedClass, Optional.empty(), allOfTypes, Optional.empty());
  }

  private JDefinedClass populateOneOf(
      Schema parentSchema,
      JDefinedClass definedClass,
      Optional<JType> commonType,
      Collection<JTypeWrapper> oneOfTypes) {

    JFieldVar valueField =
        definedClass.field(
            JMod.PRIVATE,
            commonType.orElse(definedClass.owner().ref(Object.class)),
            ruleFactory.getNameHelper().getPropertyName("value", null),
            null);

    definedClass._implements(
        definedClass
            .owner()
            .ref(GeneratorUtils.ONE_OF_VALUE_PROVIDER_INTERFACE_NAME)
            .narrow(valueField.type()));
    GeneratorUtils.implementInterface(definedClass, valueField);
    try {
      JDefinedClass serializer = generateSerializer(definedClass);
      definedClass.annotate(JsonSerialize.class).param("using", serializer);
    } catch (JClassAlreadyExistsException ex) {
      // already serialized aware
    }

    try {
      JDefinedClass deserializer =
          generateDeserializer(definedClass, oneOfTypes, "deserializeOneOf");
      definedClass.annotate(JsonDeserialize.class).param("using", deserializer);
    } catch (JClassAlreadyExistsException ex) {
      // already deserialized aware
    }

    return wrapAll(parentSchema, definedClass, commonType, oneOfTypes, Optional.of(valueField));
  }

  private JDefinedClass wrapAll(
      Schema parentSchema,
      JDefinedClass definedClass,
      Optional<JType> commonType,
      Collection<JTypeWrapper> types,
      Optional<JFieldVar> valueField) {
    Collection<JTypeWrapper> stringTypes = new ArrayList<>();
    for (JTypeWrapper unionType : types) {
      if (isStringType(unionType.getType())) {
        stringTypes.add(unionType);
      } else {
        if (unionType.getType() instanceof JDefinedClass) {
          commonType.ifPresent(
              c -> ((JDefinedClass) unionType.getType())._extends((JDefinedClass) c));
        }

        wrapIt(parentSchema, definedClass, valueField, unionType.getType(), unionType.getNode());
      }
    }
    if (!stringTypes.isEmpty()) {
      wrapStrings(parentSchema, definedClass, valueField, stringTypes);
    }
    return definedClass;
  }

  private JDefinedClass populateRef(
      JDefinedClass definedClass, Optional<JType> refType, Schema parentSchema) {
    refType.ifPresent(
        type -> {
          if (type instanceof JClass) {
            definedClass._extends((JClass) type);
          } else {
            wrapIt(parentSchema, definedClass, Optional.empty(), type, null);
          }
        });

    if (definedClass.constructors().hasNext()
        && definedClass.getConstructor(new JType[0]) == null) {
      definedClass.constructor(JMod.PUBLIC);
    }
    return definedClass;
  }

  private static boolean isStringType(JType type) {
    return type.name().equals("String");
  }

  private JDefinedClass generateSerializer(JDefinedClass relatedClass)
      throws JClassAlreadyExistsException {
    JDefinedClass definedClass = GeneratorUtils.serializerClass(relatedClass);
    GeneratorUtils.fillSerializer(
        definedClass,
        relatedClass,
        (method, valueParam, genParam) ->
            method
                .body()
                .staticInvoke(
                    definedClass.owner().ref(GeneratorUtils.SERIALIZE_HELPER_NAME),
                    "serializeOneOf")
                .arg(genParam)
                .arg(valueParam));
    return definedClass;
  }

  private JDefinedClass generateDeserializer(
      JDefinedClass relatedClass, Collection<JTypeWrapper> oneOfTypes, String methodName)
      throws JClassAlreadyExistsException {
    JDefinedClass definedClass = GeneratorUtils.deserializerClass(relatedClass);
    GeneratorUtils.fillDeserializer(
        definedClass,
        relatedClass,
        (method, parserParam) -> {
          JBlock body = method.body();

          body._return(
              definedClass
                  .owner()
                  .ref(GeneratorUtils.DESERIALIZE_HELPER_NAME)
                  .staticInvoke(methodName)
                  .arg(parserParam)
                  .arg(relatedClass.dotclass())
                  .arg(list(definedClass, oneOfTypes)));
        });
    return definedClass;
  }

  private JInvocation list(JDefinedClass definedClass, Collection<JTypeWrapper> list) {
    JInvocation result = definedClass.owner().ref(List.class).staticInvoke("of");
    list.forEach(c -> result.arg(((JClass) c.getType()).dotclass()));
    return result;
  }

  private void wrapIt(
      Schema parentSchema,
      JDefinedClass definedClass,
      Optional<JFieldVar> valueField,
      JType unionType,
      JsonNode node) {
    JFieldVar instanceField = getInstanceField(parentSchema, definedClass, unionType, node);
    JMethod method = getSetterMethod(definedClass, instanceField, node);
    method
        .body()
        .assign(
            JExpr._this().ref(instanceField),
            setupMethod(definedClass, method, valueField, instanceField));
  }

  private JVar setupMethod(
      JDefinedClass definedClass,
      JMethod method,
      Optional<JFieldVar> valueField,
      JFieldVar instanceField) {
    JVar methodParam = method.param(instanceField.type(), instanceField.name());
    valueField.ifPresent(
        v -> {
          method.body().assign(JExpr._this().ref(v), methodParam);
          method
              .annotate(definedClass.owner().ref(GeneratorUtils.SETTER_ANNOTATION_NAME))
              .param("value", instanceField.type());
        });
    return methodParam;
  }

  private JMethod getSetterMethod(
      JDefinedClass definedClass, JFieldVar instanceField, JsonNode node) {
    String setterName = ruleFactory.getNameHelper().getSetterName(instanceField.name(), node);
    JMethod fluentMethod =
        definedClass.method(JMod.PUBLIC, definedClass, setterName.replaceFirst("set", "with"));
    JBlock body = fluentMethod.body();
    body.assign(instanceField, fluentMethod.param(instanceField.type(), "value"));
    body._return(JExpr._this());
    return definedClass.method(JMod.PUBLIC, definedClass.owner().VOID, setterName);
  }

  private void wrapStrings(
      Schema parentSchema,
      JDefinedClass definedClass,
      Optional<JFieldVar> valueField,
      Collection<JTypeWrapper> stringTypes) {
    Iterator<JTypeWrapper> iter = stringTypes.iterator();
    JTypeWrapper first = iter.next();
    String pattern = pattern(first.getNode(), parentSchema);
    if (pattern == null && iter.hasNext()) {
      pattern = ".*";
    }
    JFieldVar instanceField =
        getInstanceField(parentSchema, definedClass, first.getType(), first.getNode());
    JMethod setterMethod = getSetterMethod(definedClass, instanceField, first.getNode());
    JVar methodParam = setupMethod(definedClass, setterMethod, valueField, instanceField);
    JBlock body = setterMethod.body();
    if (pattern != null) {
      JConditional condition =
          body._if(getPatternCondition(pattern, body, instanceField, methodParam, definedClass));
      condition._then().assign(JExpr._this().ref(instanceField), methodParam);
      while (iter.hasNext()) {
        JTypeWrapper item = iter.next();
        instanceField =
            getInstanceField(parentSchema, definedClass, item.getType(), item.getNode());
        pattern = pattern(item.getNode(), parentSchema);
        if (pattern == null) {
          pattern = ".*";
        }
        condition =
            condition._elseif(
                getPatternCondition(pattern, body, instanceField, methodParam, definedClass));
        condition._then().assign(JExpr._this().ref(instanceField), methodParam);
      }
      condition
          ._else()
          ._throw(
              JExpr._new(definedClass.owner()._ref(ConstraintViolationException.class))
                  .arg(
                      definedClass
                          .owner()
                          .ref(String.class)
                          .staticInvoke("format")
                          .arg("%s does not match any pattern")
                          .arg(methodParam))
                  .arg(JExpr._null()));
    } else {
      body.assign(JExpr._this().ref(instanceField), methodParam);
    }
  }

  private JFieldVar getInstanceField(
      Schema parentSchema, JDefinedClass definedClass, JType type, JsonNode node) {
    JFieldVar instanceField =
        definedClass.field(
            JMod.PRIVATE,
            type,
            ruleFactory
                .getNameHelper()
                .getPropertyName(getTypeName(node, type, parentSchema), node));
    GeneratorUtils.getterMethod(
        definedClass, instanceField, ruleFactory.getNameHelper(), instanceField.name());
    return instanceField;
  }

  private static String getFromNode(JsonNode node, String fieldName) {
    if (node != null) {
      JsonNode item = node.get(fieldName);
      if (item != null) {
        return item.asText();
      }
    }

    return null;
  }

  private JInvocation getPatternCondition(
      String pattern,
      JBlock body,
      JFieldVar instanceField,
      JVar instanceParam,
      JDefinedClass definedClass) {
    JFieldVar patternField =
        definedClass.field(
            JMod.PRIVATE | JMod.STATIC | JMod.FINAL,
            Pattern.class,
            instanceField.name() + "_" + "Pattern",
            definedClass.owner().ref(Pattern.class).staticInvoke("compile").arg(pattern));
    return JExpr.invoke(JExpr.invoke(patternField, "matcher").arg(instanceParam), "matches");
  }

  private void unionType(
      String prefix,
      String nodeName,
      JsonNode schemaNode,
      JsonNode parent,
      JClassContainer generatableType,
      Schema parentSchema,
      Collection<JTypeWrapper> types) {
    if (schemaNode.has(prefix)) {
      ArrayNode array = (ArrayNode) schemaNode.get(prefix);
      if (schemaNode.has(TITLE)) {
        nodeName = schemaNode.get(TITLE).asText();
      }
      int i = 0;
      for (JsonNode oneOf : array) {
        if (!ignoreNode(oneOf)) {
          String ref = parentSchema.getId().toString() + '/' + prefix + '/' + i++;
          Schema schema =
              ruleFactory
                  .getSchemaStore()
                  .create(
                      URI.create(ref),
                      ruleFactory.getGenerationConfig().getRefFragmentPathDelimiters());
          types.add(
              new JTypeWrapper(
                  schema.isGenerated()
                      ? schema.getJavaType()
                      : apply(nodeName, oneOf, parent, generatableType.getPackage(), schema),
                  oneOf));
        }
      }
    }
  }

  private static boolean ignoreNode(JsonNode node) {
    return allRequired(node) || allRemoveProperties(node);
  }

  private static boolean allRemoveProperties(JsonNode node) {
    if (node.size() == 1 && node.has("properties")) {
      JsonNode propsNode = node.get("properties");
      for (JsonNode propNode : propsNode) {
        if (!propNode.isBoolean() || propNode.asBoolean()) {
          return false;
        }
      }
      return true;
    }
    return false;
  }

  private static boolean allRequired(JsonNode node) {
    return node.size() == 1 && node.has("required");
  }

  private Optional<JType> refType(
      String nodeName,
      JsonNode schemaNode,
      JsonNode parent,
      JClassContainer generatableType,
      Schema parentSchema) {
    if (schemaNode.has(REF)) {
      String ref = schemaNode.get(REF).asText();
      Schema schema =
          ruleFactory
              .getSchemaStore()
              .create(
                  parentSchema,
                  ref,
                  ruleFactory.getGenerationConfig().getRefFragmentPathDelimiters());

      return Optional.of(
          schema.isGenerated()
              ? schema.getJavaType()
              : apply(
                  nameFromRef(ref, nodeName, schemaNode),
                  schema.getContent(),
                  parent,
                  generatableType,
                  schema));
    }
    return Optional.empty();
  }

  private JsonNode schemaRef(JsonNode schemaNode, Schema parentSchema) {
    String ref = getFromNode(schemaNode, REF);
    return ref != null
        ? ruleFactory
            .getSchemaStore()
            .create(
                parentSchema, ref, ruleFactory.getGenerationConfig().getRefFragmentPathDelimiters())
            .getContent()
        : null;
  }

  private String getTypeName(JsonNode node, JType type, Schema parentSchema) {
    final String title = "title";
    String name = getFromNode(node, title);
    if (name == null) {
      name = getFromNode(schemaRef(node, parentSchema), title);
    }
    if (name == null) {
      name = type.name();
    }
    return name;
  }

  private String pattern(JsonNode node, Schema parentSchema) {
    String pattern = pattern(node);
    return pattern != null ? pattern : pattern(schemaRef(node, parentSchema));
  }

  private String pattern(JsonNode node) {
    Format format = Format.parse(getFromNode(node, "format"));
    return format != null ? format.pattern() : getFromNode(node, PATTERN);
  }

  private String nameFromRef(String ref, String nodeName, JsonNode schemaNode) {
    if (schemaNode.has(TITLE)) {
      return schemaNode.get(TITLE).asText();
    }
    if ("#".equals(ref)) {
      return nodeName;
    }
    String nameFromRef;
    if (!ref.contains("#")) {
      nameFromRef = Jsonschema2Pojo.getNodeName(ref, ruleFactory.getGenerationConfig());
    } else {
      String[] nameParts = ref.split("[/\\#]");
      nameFromRef = nameParts[nameParts.length - 1];
    }

    try {
      return URLDecoder.decode(nameFromRef, "utf-8");
    } catch (UnsupportedEncodingException e) {
      throw new GenerationException("Failed to decode ref: " + ref, e);
    }
  }
}
