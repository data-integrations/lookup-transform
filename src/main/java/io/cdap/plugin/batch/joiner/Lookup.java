/*
 * Copyright © 2020 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package io.cdap.plugin.batch.joiner;

import com.google.common.base.Strings;
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Macro;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.plugin.PluginConfig;
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.cdap.etl.api.MultiInputPipelineConfigurer;
import io.cdap.cdap.etl.api.batch.BatchAutoJoiner;
import io.cdap.cdap.etl.api.batch.BatchJoiner;
import io.cdap.cdap.etl.api.batch.BatchJoinerContext;
import io.cdap.cdap.etl.api.join.AutoJoinerContext;
import io.cdap.cdap.etl.api.join.InvalidJoinException;
import io.cdap.cdap.etl.api.join.JoinCondition;
import io.cdap.cdap.etl.api.join.JoinDefinition;
import io.cdap.cdap.etl.api.join.JoinField;
import io.cdap.cdap.etl.api.join.JoinKey;
import io.cdap.cdap.etl.api.join.JoinStage;
import io.cdap.cdap.etl.api.join.error.JoinError;
import io.cdap.cdap.etl.api.join.error.OutputSchemaError;
import io.cdap.cdap.etl.api.join.error.SelectedFieldError;
import io.cdap.cdap.etl.api.lineage.field.FieldOperation;
import io.cdap.cdap.etl.api.lineage.field.FieldTransformOperation;
import io.cdap.cdap.etl.api.validation.ValidationFailure;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Nullable;

/**
 * Performs a lookup of a given field within a lookup dataset by matching it with input dataset and includes the
 * field, and it's value in resulting dataset.  The difference from joiner plugin is that this plugin returns all the
 * fields from input dataset and only the lookup field from the lookup dataset.
 * Lookup dataset will be set as broadcast.
 */
@Plugin(type = BatchJoiner.PLUGIN_TYPE)
@Name("Lookup")
@Description("Performs a lookup of a given field within a lookup dataset by matching it with input dataset and " +
  "includes the field, and it's value in resulting dataset. The difference from joiner plugin is that this plugin " +
  "returns all the fields from input dataset and only the lookup field from the lookup dataset. " +
  "Lookup dataset will be set as broadcast.")
public class Lookup extends BatchAutoJoiner {

  public static final String JOIN_OPERATION_DESCRIPTION = "Used as a key in a join";
  public static final String IDENTITY_OPERATION_DESCRIPTION = "Unchanged as part of a join";
  public static final String RENAME_OPERATION_DESCRIPTION = "Renamed as a part of a join";

  private final Config config;

  public Lookup(Config config) {
    this.config = config;
  }

  @Override
  public void prepareRun(BatchJoinerContext context) {
    FailureCollector failureCollector = context.getFailureCollector();
    Set<JoinKey> keys = getJoinKeys(context.getInputSchemas().keySet());
    boolean hasUnknownInputSchema = context.getInputSchemas().values().stream().anyMatch(Objects::isNull);
    Schema outputSchema = config.getOutputSchema(failureCollector);
    if (!hasUnknownInputSchema || outputSchema != null) {
      List<JoinField> joinFields = hasUnknownInputSchema ?
        getJoinFieldsFromOutputSchema(context.getInputSchemas().keySet(), outputSchema) :
        getJoinFieldsFromInputSchemas(context.getInputSchemas());
      context.record(createFieldOperations(joinFields, keys));
    }
  }

  /**
   * Generates list of {@link JoinField} from input schemas
   *
   * @param inputSchemas {@link Map<String, Schema>} input schemas
   * @return list of {@link JoinField}
   */
  private List<JoinField> getJoinFieldsFromInputSchemas(Map<String, Schema> inputSchemas) {
    List<JoinField> joinFields = new ArrayList<>();
    inputSchemas.forEach((stageName, schema) -> {
      if (!config.getLookupDataset().equals(stageName)) {
        schema.getFields().forEach(field -> {
          joinFields.add(new JoinField(stageName, field.getName(), field.getName()));
        });
      }
    });
    // add lookup value field
    joinFields.add(new JoinField(config.getLookupDataset(), config.getLookupValueField(), config.getOutputField()));
    return joinFields;
  }

  /**
   * Create the field operations from the provided OutputFieldInfo instances and join keys. For join we record several
   * types of transformation; Join, Identity, and Rename. For each of these transformations, if the input field is
   * directly coming from the schema of one of the stage, the field is added as {@code stage_name.field_name}. We keep
   * track of fields outputted by operation (in {@code outputsSoFar set}, so that any operation uses that field as input
   * later, we add it without the stage name.
   * <p>
   * Join transform operation is added with join keys as input tagged with the stage name, and join keys without stage
   * name as output.
   * <p>
   * For other fields which are not renamed in join, Identity transform is added, while for fields which are renamed
   * Rename transform is added.
   *
   * @param outputFields collection of output fields along with information such as stage name, alias
   * @param joinKeys     join keys
   * @return List of field operations
   */
  private List<FieldOperation> createFieldOperations(List<JoinField> outputFields, Set<JoinKey> joinKeys) {
    LinkedList<FieldOperation> operations = new LinkedList<>();
    Map<String, List<String>> perStageJoinKeys = joinKeys.stream()
      .collect(Collectors.toMap(JoinKey::getStageName, JoinKey::getFields));

    // Add JOIN operation
    List<String> joinInputs = new ArrayList<>();
    Set<String> joinOutputs = new LinkedHashSet<>();
    for (Map.Entry<String, List<String>> joinKey : perStageJoinKeys.entrySet()) {
      for (String field : joinKey.getValue()) {
        joinInputs.add(joinKey.getKey() + "." + field);
        joinOutputs.add(field);
      }
    }
    FieldOperation joinOperation = new FieldTransformOperation("Join", JOIN_OPERATION_DESCRIPTION, joinInputs,
                                                               new ArrayList<>(joinOutputs));
    operations.add(joinOperation);

    Set<String> outputsSoFar = new HashSet<>(joinOutputs);

    for (JoinField outputField : outputFields) {
      // input field name for the operation will come in from schema if its not outputted so far
      String stagedInputField = outputsSoFar.contains(outputField.getFieldName()) ?
        outputField.getFieldName() : outputField.getStageName() + "." + outputField.getFieldName();

      String outputFieldName = outputField.getAlias() == null ? outputField.getFieldName() : outputField.getAlias();
      if (outputField.getFieldName().equals(outputFieldName)) {
        // Record identity transform
        if (perStageJoinKeys.get(outputField.getStageName()).contains(outputField.getFieldName())) {
          // if the field is part of join key no need to emit the identity transform as it is already taken care
          // by join
          continue;
        }
        String operationName = String.format("Identity %s", stagedInputField);
        FieldOperation identity = new FieldTransformOperation(operationName, IDENTITY_OPERATION_DESCRIPTION,
                                                              Collections.singletonList(stagedInputField),
                                                              outputFieldName);
        operations.add(identity);
        continue;
      }

      String operationName = String.format("Rename %s", stagedInputField);

      FieldOperation transform = new FieldTransformOperation(operationName, RENAME_OPERATION_DESCRIPTION,
                                                             Collections.singletonList(stagedInputField),
                                                             outputFieldName);
      operations.add(transform);
    }

    return operations;
  }

  @Nullable
  @Override
  public JoinDefinition define(AutoJoinerContext context) {
    FailureCollector collector = context.getFailureCollector();
    boolean hasUnknownInputSchema = context.getInputStages().values().stream().map(JoinStage::getSchema)
      .anyMatch(Objects::isNull);

    if ((hasUnknownInputSchema && config.containsMacro(Config.OUTPUT_SCHEMA)) || config.fieldsContainMacros()) {
      return null;
    }
    if (hasUnknownInputSchema && !config.containsMacro(Config.OUTPUT_SCHEMA) &&
      config.getOutputSchema(collector) == null) {
      // If input schemas are unknown, an output schema must be provided.
      collector.addFailure("Output schema must be specified", null).withConfigProperty(Config.OUTPUT_SCHEMA);
      collector.getOrThrowException();
    }

    List<JoinStage> inputs = new ArrayList<>(context.getInputStages().size());
    boolean useOutputSchema = false;
    for (JoinStage joinStage : context.getInputStages().values()) {
      inputs.add(JoinStage.builder(joinStage)
                   .setRequired(!joinStage.getStageName().equals(config.getLookupDataset()))
                   .setBroadcast(joinStage.getStageName().equals(config.getLookupDataset()))
                   .build());
      useOutputSchema = useOutputSchema || joinStage.getSchema() == null;
    }
    Schema outputSchema = config.getOutputSchema(context.getFailureCollector());
    List<JoinField> joinFields = hasUnknownInputSchema ?
      getJoinFieldsFromOutputSchema(context.getInputStages().keySet(), outputSchema) :
      getJoinFieldsFromInputStages(context.getInputStages());
    Collection<JoinKey> keys = getJoinKeys(context.getInputStages().keySet());

    try {
      JoinDefinition.Builder joinBuilder = JoinDefinition.builder()
        .select(joinFields)
        .from(inputs)
        .on(JoinCondition.onKeys()
              .setKeys(keys)
              .setNullSafe(false)
              .build());
      if (useOutputSchema) {
        joinBuilder.setOutputSchema(config.getOutputSchema(collector));
      } else {
        joinBuilder.setOutputSchemaName("join.output");
      }
      return joinBuilder.build();
    } catch (InvalidJoinException e) {
      if (e.getErrors().isEmpty()) {
        collector.addFailure(e.getMessage(), null);
      }
      for (JoinError error : e.getErrors()) {
        ValidationFailure failure = collector.addFailure(error.getMessage(), error.getCorrectiveAction());
        switch (error.getType()) {
          case JOIN_KEY:
          case JOIN_KEY_FIELD:
            failure.withConfigProperty(Config.INPUT_KEY_FIELD);
            break;
          case SELECTED_FIELD:
            JoinField badField = ((SelectedFieldError) error).getField();
            failure.withConfigElement(
              Config.LOOKUP_VALUE_FIELD,
              String.format("%s.%s as %s", badField.getStageName(), badField.getFieldName(), badField.getAlias()));
            break;
          case OUTPUT_SCHEMA:
            OutputSchemaError schemaError = (OutputSchemaError) error;
            failure.withOutputSchemaField(schemaError.getField());
            break;
        }
      }
      throw collector.getOrThrowException();
    }
  }

  /**
   * Get join keys from input stages
   *
   * @param inputStages {@link Set<String>} input stage names
   * @return {@link Set<JoinKey>} join keys
   */
  private Set<JoinKey> getJoinKeys(Set<String> inputStages) {
    Set<JoinKey> joinKeys = new HashSet<>();
    inputStages.forEach(stageName -> {
      if (!config.getLookupDataset().equals(stageName)) {
        joinKeys.add(new JoinKey(stageName, Collections.singletonList(config.getInputKeyField())));
      }
    });
    // add lookup join key
    joinKeys.add(new JoinKey(config.getLookupDataset(), Collections.singletonList(config.getLookupKeyField())));
    return joinKeys;
  }

  /**
   * Get the fields from input stages to include in join result
   *
   * @param inputStages {@link Map<String, JoinStage>} input stages
   * @return {@link List<JoinField>} list fields to include in join result
   */
  private List<JoinField> getJoinFieldsFromInputStages(Map<String, JoinStage> inputStages) {
    List<JoinField> joinFields = new ArrayList<>();
    inputStages.forEach((stageName, joinStage) -> {
      if (!config.getLookupDataset().equals(stageName)) {
        joinStage.getSchema().getFields().forEach(field -> {
          joinFields.add(new JoinField(stageName, field.getName(), field.getName()));
        });
      }
    });
    // add lookup value field
    joinFields.add(new JoinField(config.getLookupDataset(), config.getLookupValueField(), config.getOutputField()));
    return joinFields;
  }

  /**
   * Get the fields from output schema to include in join result
   *
   * @param inputStages  {@link Set<String>} input stages
   * @param outputSchema {@link Schema} output schema
   * @return {@link List<JoinField>} list of join fields
   */
  private List<JoinField> getJoinFieldsFromOutputSchema(Set<String> inputStages, Schema outputSchema) {
    List<JoinField> joinFields = new ArrayList<>();
    inputStages.forEach(stageName -> {
      if (!config.getLookupDataset().equals(stageName)) {
        outputSchema.getFields().forEach(field -> {
          if (!field.getName().equals(config.getOutputField())) {
            joinFields.add(new JoinField(stageName, field.getName(), field.getName()));
          }
        });
      }
    });
    joinFields.add(new JoinField(config.getLookupDataset(), config.getLookupValueField(), config.getOutputField()));
    return joinFields;
  }

  /**
   * Config for Lookup transform
   */
  public static class Config extends PluginConfig {
    private static final String LOOKUP_DATASET = "lookupDataset";
    private static final String INPUT_KEY_FIELD = "inputKeyField";
    private static final String LOOKUP_KEY_FIELD = "lookupKeyField";
    private static final String LOOKUP_VALUE_FIELD = "lookupValueField";
    private static final String OUTPUT_FIELD = "outputField";
    public static final String OUTPUT_SCHEMA = "schema";

    @Description("Amongst the inputs connected to the lookup transformation, this determines the input that should " +
      "be used as the lookup dataset.")
    @Name(LOOKUP_DATASET)
    @Macro
    private final String lookupDataset;

    @Description("Field in the input schema that should be used as a key in the lookup condition.")
    @Name(INPUT_KEY_FIELD)
    @Macro
    private final String inputKeyField;

    @Description("Field in the lookup source that should be used as a key in the lookup condition.")
    @Name(LOOKUP_KEY_FIELD)
    @Macro
    private final String lookupKeyField;

    @Description("Field in the lookup source that should be returned after the lookup.")
    @Name(LOOKUP_VALUE_FIELD)
    @Macro
    private final String lookupValueField;

    @Description("Name of the field in which to store the result of the lookup. This field will be added to the " +
      "output schema, and will contain the value of the Lookup Value Field.")
    @Name(OUTPUT_FIELD)
    @Macro
    @Nullable
    private final String outputField;

    @Nullable
    @Macro
    @Description(OUTPUT_SCHEMA)
    private final String schema;

    public Config(String lookupDataset, String inputKeyField, String lookupKeyField, String lookupValueField,
                  @Nullable String outputField, @Nullable String schema) {
      this.lookupDataset = lookupDataset;
      this.inputKeyField = inputKeyField;
      this.lookupKeyField = lookupKeyField;
      this.lookupValueField = lookupValueField;
      this.outputField = outputField;
      this.schema = schema;
    }

    public String getLookupDataset() {
      return lookupDataset;
    }

    public String getInputKeyField() {
      return inputKeyField;
    }

    public String getLookupKeyField() {
      return lookupKeyField;
    }

    public String getLookupValueField() {
      return lookupValueField;
    }

    public String getOutputField() {
      return Strings.isNullOrEmpty(outputField) ? getLookupValueField() : outputField;
    }

    @Nullable
    public Schema getOutputSchema(FailureCollector collector) {
      try {
        return Strings.isNullOrEmpty(schema) ? null : Schema.parseJson(schema);
      } catch (IOException e) {
        collector.addFailure("Invalid schema: " + e.getMessage(), null)
          .withConfigProperty(OUTPUT_SCHEMA);
      }
      // if there was an error that was added, it will throw an exception, otherwise,
      // this statement will not be executed
      throw collector.getOrThrowException();
    }

    public boolean fieldsContainMacros() {
      return containsMacro(LOOKUP_DATASET) || containsMacro(LOOKUP_KEY_FIELD) || containsMacro(LOOKUP_VALUE_FIELD)
        || containsMacro(INPUT_KEY_FIELD) || containsMacro(OUTPUT_FIELD);
    }

    private void validate(FailureCollector failureCollector) {
      if (!containsMacro(LOOKUP_DATASET) && Strings.isNullOrEmpty(lookupDataset)) {
        failureCollector.addFailure("Missing lookup dataset.", "Lookup dataset must provided.")
          .withConfigProperty(LOOKUP_DATASET);
      }
      if (!containsMacro(INPUT_KEY_FIELD) && Strings.isNullOrEmpty(inputKeyField)) {
        failureCollector.addFailure("Missing input key field.", "Input key field must be provided.")
          .withConfigProperty(INPUT_KEY_FIELD);
      }
      if (!containsMacro(LOOKUP_KEY_FIELD) && Strings.isNullOrEmpty(lookupKeyField)) {
        failureCollector.addFailure("Missing lookup key field.", "Lookup key field must be provided.")
          .withConfigProperty(LOOKUP_KEY_FIELD);
      }
      if (!containsMacro(LOOKUP_VALUE_FIELD) && Strings.isNullOrEmpty(lookupValueField)) {
        failureCollector.addFailure("Missing lookup value field.", "Lookup value field must be provided.")
          .withConfigProperty(LOOKUP_VALUE_FIELD);
      }
    }
  }

  /**
   * Generates schema based on input schemas and provided configuration
   *
   * @param inputSchemas map containing input stage name and schema
   * @param collector    {@link FailureCollector}
   * @return {@link Schema} generated schema
   */
  private Schema generateOutputSchema(Map<String, Schema> inputSchemas, FailureCollector collector) {
    final Set<String> schemas = inputSchemas.keySet();
    if (schemas.size() == 0) {
      collector.addFailure("Missing input schema.", "Please connect valid input datasets.");
    }
    Schema lookupSchema = inputSchemas.getOrDefault(config.getLookupDataset(), null);
    Schema inputSchema = null;
    if (lookupSchema == null) {
      collector.addFailure("Lookup dataset with name not found.",
                           "Please provide valid name for lookup dataset.");
    }
    for (String schemaName : schemas) {
      if (!schemaName.equals(config.getLookupDataset())) {
        inputSchema = inputSchemas.getOrDefault(schemaName, null);
      }
    }
    List<Schema.Field> fields = new ArrayList<>(inputSchema.getFields());
    final Schema.Field lookupField = lookupSchema.getField(config.getLookupValueField());
    fields.add(Schema.Field.of(config.getOutputField(), lookupField.getSchema().isNullable() ? lookupField.getSchema()
      : Schema.nullableOf(lookupField.getSchema())));
    return Schema.recordOf("join.output", fields);
  }

  @Override
  public void configurePipeline(MultiInputPipelineConfigurer multiInputPipelineConfigurer) {
    super.configurePipeline(multiInputPipelineConfigurer);
    final Map<String, Schema> inputSchemas = multiInputPipelineConfigurer
      .getMultiInputStageConfigurer().getInputSchemas();
    FailureCollector collector = multiInputPipelineConfigurer.getMultiInputStageConfigurer().getFailureCollector();
    config.validate(collector);
    if (config.fieldsContainMacros()) {
      return;
    }
    if (inputSchemas.values().size() != 2) {
      if (inputSchemas.values().size() > 2) {
        collector.addFailure("More than two input datasets provided.",
                             "Only two input dataset are allowed.");
      } else {
        collector.addFailure("Not enough datasets provided.",
                             "Two input dataset are required.");
      }
      collector.getOrThrowException();
    }
    // one or both of the input schemas contain macro
    if (inputSchemas.containsValue(null)) {
      return;
    }
    if (!inputSchemas.containsKey(config.lookupDataset)) {
      collector.addFailure("Missing lookup dataset.",
                           "Lookup dataset name needs to match one of the input datasets.")
        .withConfigProperty(config.lookupDataset);
    }
    if (inputSchemas.get(config.lookupDataset) != null) {
      if (inputSchemas.get(config.lookupDataset).getField(config.lookupKeyField) == null) {
        collector.addFailure("Lookup key field not found.",
                             "Lookup key field needs ot be one of the lookup dataset fields.")
          .withConfigProperty(config.lookupKeyField);
      }
      if (inputSchemas.get(config.lookupDataset).getField(config.lookupValueField) == null) {
        collector.addFailure("Lookup value field not found.",
                             "Lookup value field needs ot be one of the lookup dataset fields.")
          .withConfigProperty(config.lookupValueField);
      }
    }
    String inputSchemaName = inputSchemas.keySet().stream()
      .filter(inputName -> !inputName.equals(config.lookupDataset)).findFirst().get();
    if (inputSchemas.get(inputSchemaName).getField(config.inputKeyField) == null) {
      collector.addFailure("Input key field not found.",
                           "Input key field needs to be one of the input dataset fields.")
        .withConfigProperty(config.inputKeyField);
      collector.getOrThrowException();
    }
    if (!inputSchemas.get(config.lookupDataset).getField(config.lookupKeyField).getSchema()
      .isCompatible(inputSchemas.get(inputSchemaName).getField(config.inputKeyField).getSchema())) {
      collector.addFailure("Input key field type does not match lookup key field type.",
                           "Input key field type needs to match lookup key field type.");
    }
    if (inputSchemas.get(inputSchemaName).getField(config.outputField) != null) {
      collector.addFailure("Field with name matching output field name already exists in input dataset.",
                           "Output field name should not collide with any input dataset field name.");
    }
    if (!config.containsMacro(Config.OUTPUT_SCHEMA)) {
      Schema tempSchema = config.getOutputSchema(collector);
      if (tempSchema == null) {
        tempSchema = generateOutputSchema(inputSchemas, collector);
      }
      multiInputPipelineConfigurer.getMultiInputStageConfigurer().setOutputSchema(tempSchema);
    }
    collector.getOrThrowException();
  }
}
