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

package io.cdap.plugin.batch;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import io.cdap.cdap.api.artifact.ArtifactSummary;
import io.cdap.cdap.api.common.Bytes;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.dataset.table.Row;
import io.cdap.cdap.api.dataset.table.Scan;
import io.cdap.cdap.api.dataset.table.Scanner;
import io.cdap.cdap.api.dataset.table.Table;
import io.cdap.cdap.common.utils.Tasks;
import io.cdap.cdap.datapipeline.DataPipelineApp;
import io.cdap.cdap.datapipeline.SmartWorkflow;
import io.cdap.cdap.etl.mock.test.HydratorTestBase;
import io.cdap.cdap.etl.proto.v2.ETLBatchConfig;
import io.cdap.cdap.format.StructuredRecordStringConverter;
import io.cdap.cdap.proto.artifact.AppRequest;
import io.cdap.cdap.proto.id.ApplicationId;
import io.cdap.cdap.proto.id.ArtifactId;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.test.ApplicationManager;
import io.cdap.cdap.test.TestConfiguration;
import io.cdap.cdap.test.WorkflowManager;
import io.cdap.plugin.batch.joiner.Lookup;
import org.junit.BeforeClass;
import org.junit.ClassRule;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Unit tests for our plugins.
 */
public class ETLBatchTestBase extends HydratorTestBase {

  private static final ArtifactSummary APP_ARTIFACT = new ArtifactSummary("data-pipeline", "3.5.0");
  @ClassRule
  public static final TestConfiguration CONFIG = new TestConfiguration("explore.enabled", false);

  private static int startCount;

  @BeforeClass
  public static void setupTest() throws Exception {
    if (startCount++ > 0) {
      return;
    }
    ArtifactId parentArtifact = NamespaceId.DEFAULT.artifact(APP_ARTIFACT.getName(), APP_ARTIFACT.getVersion());

    // Add the data pipeline artifact and mock plugins.
    setupBatchArtifacts(parentArtifact, DataPipelineApp.class);

    // Add our plugins artifact with the data pipeline artifact as its parent.
    // This will make our plugins available to the data pipeline.
    addPluginArtifact(NamespaceId.DEFAULT.artifact("lookup-transform-plugin", "1.0.0"),
                      parentArtifact,
                      Lookup.class
    );
  }

  protected ApplicationManager deployETL(ETLBatchConfig etlConfig, String appName) throws Exception {
    AppRequest<ETLBatchConfig> appRequest = new AppRequest<>(APP_ARTIFACT, etlConfig);
    ApplicationId appId = NamespaceId.DEFAULT.app(appName);
    return deployApplication(appId, appRequest);
  }

  /**
   * Run the SmartWorkflow in the given ETL application for once and wait for the workflow's COMPLETED status
   * with 5 minutes timeout.
   *
   * @param appManager the ETL application to run
   */
  protected WorkflowManager runETLOnce(ApplicationManager appManager)
    throws TimeoutException, InterruptedException, ExecutionException {
    return runETLOnce(appManager, ImmutableMap.of());
  }

  /**
   * Run the SmartWorkflow in the given ETL application for once and wait for the workflow's COMPLETED status
   * with 5 minutes timeout.
   *
   * @param appManager the ETL application to run
   * @param arguments  the arguments to be passed when running SmartWorkflow
   */
  protected WorkflowManager runETLOnce(ApplicationManager appManager, Map<String, String> arguments)
    throws TimeoutException, InterruptedException, ExecutionException {
    final WorkflowManager workflowManager = appManager.getWorkflowManager(SmartWorkflow.NAME);
    int numRuns = workflowManager.getHistory().size();
    workflowManager.start(arguments);
    Tasks.waitFor(numRuns + 1, () -> workflowManager.getHistory().size(), 20, TimeUnit.SECONDS);
    workflowManager.waitForStopped(5, TimeUnit.MINUTES);
    return workflowManager;
  }

  protected List<StructuredRecord> readOutput(Table table) throws IOException {
    List<StructuredRecord> records = Lists.newArrayList();
    Scan scan = new Scan(null, null);
    final Scanner scan1 = table.scan(scan);
    Row next;
    while ((next = scan1.next()) != null) {
      final StructuredRecord structuredRecord = StructuredRecordStringConverter.fromJsonString(
        Bytes.toString(next.getColumns().get("r".getBytes())),
        Schema.parseJson(Bytes.toString(next.getColumns().get("s".getBytes())))
      );
      records.add(structuredRecord);
    }
    return records;
  }
}
