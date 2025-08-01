/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pinot.controller.api.resources;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.base.Preconditions;
import com.google.common.collect.BiMap;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiKeyAuthDefinition;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;
import io.swagger.annotations.Authorization;
import io.swagger.annotations.SecurityDefinition;
import io.swagger.annotations.SwaggerDefinition;
import it.unimi.dsi.fastutil.Arrays;
import it.unimi.dsi.fastutil.Swapper;
import it.unimi.dsi.fastutil.ints.IntComparator;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.Future;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import javax.inject.Inject;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hc.client5.http.io.HttpClientConnectionManager;
import org.apache.helix.AccessOption;
import org.apache.helix.model.IdealState;
import org.apache.helix.store.zk.ZkHelixPropertyStore;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.pinot.common.exception.InvalidConfigException;
import org.apache.pinot.common.exception.RebalanceInProgressException;
import org.apache.pinot.common.exception.SchemaNotFoundException;
import org.apache.pinot.common.exception.TableNotFoundException;
import org.apache.pinot.common.metadata.ZKMetadataProvider;
import org.apache.pinot.common.metrics.ControllerMeter;
import org.apache.pinot.common.metrics.ControllerMetrics;
import org.apache.pinot.common.response.server.TableIndexMetadataResponse;
import org.apache.pinot.common.restlet.resources.TableSegmentValidationInfo;
import org.apache.pinot.common.restlet.resources.ValidDocIdsType;
import org.apache.pinot.common.utils.DatabaseUtils;
import org.apache.pinot.common.utils.LogicalTableConfigUtils;
import org.apache.pinot.common.utils.helix.HelixHelper;
import org.apache.pinot.controller.ControllerConf;
import org.apache.pinot.controller.api.access.AccessControlFactory;
import org.apache.pinot.controller.api.access.AccessType;
import org.apache.pinot.controller.api.access.Authenticate;
import org.apache.pinot.controller.api.exception.ControllerApplicationException;
import org.apache.pinot.controller.api.exception.InvalidTableConfigException;
import org.apache.pinot.controller.api.exception.TableAlreadyExistsException;
import org.apache.pinot.controller.helix.core.PinotHelixResourceManager;
import org.apache.pinot.controller.helix.core.PinotResourceManagerResponse;
import org.apache.pinot.controller.helix.core.controllerjob.ControllerJobTypes;
import org.apache.pinot.controller.helix.core.minion.PinotHelixTaskResourceManager;
import org.apache.pinot.controller.helix.core.minion.PinotTaskManager;
import org.apache.pinot.controller.helix.core.rebalance.RebalanceConfig;
import org.apache.pinot.controller.helix.core.rebalance.RebalanceResult;
import org.apache.pinot.controller.helix.core.rebalance.TableRebalanceManager;
import org.apache.pinot.controller.helix.core.rebalance.TableRebalancer;
import org.apache.pinot.controller.recommender.RecommenderDriver;
import org.apache.pinot.controller.tuner.TableConfigTunerUtils;
import org.apache.pinot.controller.util.CompletionServiceHelper;
import org.apache.pinot.controller.util.TableIngestionStatusHelper;
import org.apache.pinot.controller.util.TableMetadataReader;
import org.apache.pinot.controller.util.TaskConfigUtils;
import org.apache.pinot.core.auth.Actions;
import org.apache.pinot.core.auth.Authorize;
import org.apache.pinot.core.auth.ManualAuthorization;
import org.apache.pinot.core.auth.TargetType;
import org.apache.pinot.segment.local.utils.TableConfigUtils;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableStatsHumanReadable;
import org.apache.pinot.spi.config.table.TableStatus;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.controller.ControllerJobType;
import org.apache.pinot.spi.data.LogicalTableConfig;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.spi.utils.Enablement;
import org.apache.pinot.spi.utils.JsonUtils;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.apache.pinot.spi.utils.retry.RetryPolicies;
import org.apache.zookeeper.data.Stat;
import org.glassfish.grizzly.http.server.Request;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.pinot.spi.utils.CommonConstants.DATABASE;
import static org.apache.pinot.spi.utils.CommonConstants.SWAGGER_AUTHORIZATION_KEY;


@Api(tags = Constants.TABLE_TAG, authorizations = {
    @Authorization(value = SWAGGER_AUTHORIZATION_KEY),
    @Authorization(value = DATABASE)
})
@SwaggerDefinition(securityDefinition = @SecurityDefinition(apiKeyAuthDefinitions = {
    @ApiKeyAuthDefinition(name = HttpHeaders.AUTHORIZATION, in = ApiKeyAuthDefinition.ApiKeyLocation.HEADER,
        key = SWAGGER_AUTHORIZATION_KEY,
        description = "The format of the key is  ```\"Basic <token>\" or \"Bearer <token>\"```"),
    @ApiKeyAuthDefinition(name = DATABASE, in = ApiKeyAuthDefinition.ApiKeyLocation.HEADER, key = DATABASE,
        description = "Database context passed through http header. If no context is provided 'default' database "
            + "context will be considered.")
}))
@Path("/")
public class PinotTableRestletResource {
  /**
   * URI Mappings:
   * - "/tables", "/tables/": List all the tables
   * - "/tables/{tableName}", "/tables/{tableName}/": List config for specified table.
   *
   * - "/tables/{tableName}?state={state}"
   *   Set the state for the specified {tableName} to the specified {state} (enable|disable|drop).
   *
   * - "/tables/{tableName}?type={type}"
   *   List all tables of specified type, type can be one of {offline|realtime}.
   *
   *   Set the state for the specified {tableName} to the specified {state} (enable|disable|drop).
   *   * - "/tables/{tableName}?state={state}&amp;type={type}"
   *
   *   Set the state for the specified {tableName} of specified type to the specified {state} (enable|disable|drop).
   *   Type here is type of the table, one of 'offline|realtime'.
   * {@inheritDoc}
   */

  public static final Logger LOGGER = LoggerFactory.getLogger(PinotTableRestletResource.class);

  @Inject
  PinotHelixResourceManager _pinotHelixResourceManager;

  @Inject
  TableRebalanceManager _tableRebalanceManager;

  @Inject
  PinotHelixTaskResourceManager _pinotHelixTaskResourceManager;

  @Inject
  PinotTaskManager _pinotTaskManager;

  @Inject
  ControllerConf _controllerConf;

  @Inject
  ControllerMetrics _controllerMetrics;

  @Inject
  AccessControlFactory _accessControlFactory;

  @Inject
  Executor _executor;

  @Inject
  HttpClientConnectionManager _connectionManager;

  /**
   * API to create a table. Before adding, validations will be done (min number of replicas, checking offline and
   * realtime table configs match, checking for tenants existing).
   */
  @POST
  @Produces(MediaType.APPLICATION_JSON)
  @Path("/tables")
  @ApiOperation(value = "Adds a table", notes = "Adds a table")
  @ManualAuthorization // performed after parsing table configs
  public ConfigSuccessResponse addTable(String tableConfigStr,
      @ApiParam(value = "comma separated list of validation type(s) to skip. supported types: (ALL|TASK|UPSERT)")
      @QueryParam("validationTypesToSkip") @Nullable String typesToSkip, @Context HttpHeaders httpHeaders,
      @Context Request request) {
    // TODO introduce a table config ctor with json string.
    Pair<TableConfig, Map<String, Object>> tableConfigAndUnrecognizedProperties;
    TableConfig tableConfig;
    String tableNameWithType;
    Schema schema;
    try {
      tableConfigAndUnrecognizedProperties =
          JsonUtils.stringToObjectAndUnrecognizedProperties(tableConfigStr, TableConfig.class);
      tableConfig = tableConfigAndUnrecognizedProperties.getLeft();
      tableNameWithType = DatabaseUtils.translateTableName(tableConfig.getTableName(), httpHeaders);
      tableConfig.setTableName(tableNameWithType);

      // validate permission
      ResourceUtils.checkPermissionAndAccess(tableNameWithType, request, httpHeaders,
          AccessType.CREATE, Actions.Table.CREATE_TABLE, _accessControlFactory, LOGGER);

      schema = _pinotHelixResourceManager.getTableSchema(tableNameWithType);
      Preconditions.checkState(schema != null, "Failed to find schema for table: %s", tableNameWithType);

      TableConfigTunerUtils.applyTunerConfigs(_pinotHelixResourceManager, tableConfig, schema, Collections.emptyMap());

      // TableConfigUtils.validate(...) is used across table create/update.
      TableConfigUtils.validate(tableConfig, schema, typesToSkip);
      TableConfigUtils.validateTableName(tableConfig);
    } catch (Exception e) {
      throw new ControllerApplicationException(LOGGER, e.getMessage(), Response.Status.BAD_REQUEST, e);
    }
    try {
      try {
        TableConfigUtils.ensureMinReplicas(tableConfig, _controllerConf.getDefaultTableMinReplicas());
        TableConfigUtils.ensureStorageQuotaConstraints(tableConfig, _controllerConf.getDimTableMaxSize());
        checkHybridTableConfig(TableNameBuilder.extractRawTableName(tableNameWithType), tableConfig);
        TaskConfigUtils.validateTaskConfigs(tableConfig, schema, _pinotTaskManager, typesToSkip);
        validateInstanceAssignment(tableConfig);
      } catch (Exception e) {
        throw new InvalidTableConfigException(e);
      }
      _pinotHelixResourceManager.addTable(tableConfig);
      // TODO: validate that table was created successfully
      // (in realtime case, metadata might not have been created but would be created successfully in the next run of
      // the validation manager)
      LOGGER.info("Successfully added table: {} with config: {}", tableNameWithType, tableConfig);
      return new ConfigSuccessResponse("Table " + tableNameWithType + " successfully added",
          tableConfigAndUnrecognizedProperties.getRight());
    } catch (Exception e) {
      _controllerMetrics.addMeteredGlobalValue(ControllerMeter.CONTROLLER_TABLE_ADD_ERROR, 1L);
      if (e instanceof InvalidTableConfigException) {
        String errStr = String.format("Invalid table config for table %s: %s", tableNameWithType, e.getMessage());
        throw new ControllerApplicationException(LOGGER, errStr, Response.Status.BAD_REQUEST, e);
      } else if (e instanceof TableAlreadyExistsException) {
        throw new ControllerApplicationException(LOGGER, e.getMessage(), Response.Status.CONFLICT, e);
      } else {
        throw new ControllerApplicationException(LOGGER, e.getMessage(), Response.Status.INTERNAL_SERVER_ERROR, e);
      }
    }
  }

  @PUT
  @Produces(MediaType.APPLICATION_JSON)
  @Path("/tables/recommender")
  @Authorize(targetType = TargetType.CLUSTER, action = Actions.Cluster.RECOMMEND_CONFIG)
  @ApiOperation(value = "Recommend config", notes = "Recommend a config with input json")
  public String recommendConfig(String inputStr) {
    try {
      return RecommenderDriver.run(inputStr);
    } catch (Exception e) {
      throw new ControllerApplicationException(LOGGER, e.getMessage(), Response.Status.BAD_REQUEST, e);
    }
  }

  @GET
  @Produces(MediaType.APPLICATION_JSON)
  @Path("/tables")
  @Authorize(targetType = TargetType.CLUSTER, action = Actions.Cluster.GET_TABLE)
  @ApiOperation(value = "Lists all tables in cluster", notes = "Lists all tables in cluster")
  public String listTables(@ApiParam(value = "realtime|offline|dimension") @QueryParam("type") String tableTypeStr,
      @ApiParam(value = "Task type") @QueryParam("taskType") String taskType,
      @ApiParam(value = "name|creationTime|lastModifiedTime") @QueryParam("sortType") String sortTypeStr,
      @ApiParam(value = "true|false") @QueryParam("sortAsc") @DefaultValue("true") boolean sortAsc,
      @Context HttpHeaders headers) {
    try {
      boolean isDimensionTable = "dimension".equalsIgnoreCase(tableTypeStr);
      TableType tableType = null;
      if (isDimensionTable) {
        // Dimension is a property (isDimTable) of an OFFLINE table.
        tableType = TableType.OFFLINE;
      } else if (tableTypeStr != null) {
        tableType = TableType.valueOf(tableTypeStr.toUpperCase());
      }
      SortType sortType = sortTypeStr != null ? SortType.valueOf(sortTypeStr.toUpperCase()) : SortType.NAME;

      String database = headers.getHeaderString(DATABASE);

      // If tableTypeStr is dimension, then tableType is set to TableType.OFFLINE.
      // So, checking the isDimensionTable to get the list of dimension tables only.
      List<String> tableNamesWithType =
          isDimensionTable ? _pinotHelixResourceManager.getAllDimensionTables(database)
              : tableType == null ? _pinotHelixResourceManager.getAllTables(database)
                  : (tableType == TableType.REALTIME ? _pinotHelixResourceManager.getAllRealtimeTables(database)
                      : _pinotHelixResourceManager.getAllOfflineTables(database));

      if (StringUtils.isNotBlank(taskType)) {
        Set<String> tableNamesForTaskType = new HashSet<>();
        for (String tableNameWithType : tableNamesWithType) {
          TableConfig tableConfig = _pinotHelixResourceManager.getTableConfig(tableNameWithType);
          if (tableConfig != null && tableConfig.getTaskConfig() != null && tableConfig.getTaskConfig()
              .isTaskTypeEnabled(taskType)) {
            tableNamesForTaskType.add(tableNameWithType);
          }
        }
        tableNamesWithType.retainAll(tableNamesForTaskType);
      }

      List<String> tableNames;
      if (sortType == SortType.NAME) {
        if (tableType == null && StringUtils.isBlank(taskType)) {
          List<String> rawTableNames = tableNamesWithType.stream().map(TableNameBuilder::extractRawTableName).distinct()
              .collect(Collectors.toList());
          rawTableNames.sort(sortAsc ? null : Comparator.reverseOrder());
          tableNames = rawTableNames;
        } else {
          tableNamesWithType.sort(sortAsc ? null : Comparator.reverseOrder());
          tableNames = tableNamesWithType;
        }
      } else {
        int sortFactor = sortAsc ? 1 : -1;
        ZkHelixPropertyStore<ZNRecord> propertyStore = _pinotHelixResourceManager.getPropertyStore();
        int numTables = tableNamesWithType.size();
        List<String> zkPaths = new ArrayList<>(numTables);
        for (String tableNameWithType : tableNamesWithType) {
          zkPaths.add(ZKMetadataProvider.constructPropertyStorePathForResourceConfig(tableNameWithType));
        }
        Stat[] stats = propertyStore.getStats(zkPaths, AccessOption.PERSISTENT);
        for (int i = 0; i < numTables; i++) {
          Preconditions.checkState(stats[i] != null, "Failed to read ZK stats for table: %s",
              tableNamesWithType.get(i));
        }
        IntComparator comparator;
        if (sortType == SortType.CREATIONTIME) {
          comparator = (i, j) -> Long.compare(stats[i].getCtime(), stats[j].getCtime()) * sortFactor;
        } else {
          assert sortType == SortType.LASTMODIFIEDTIME;
          comparator = (i, j) -> Long.compare(stats[i].getMtime(), stats[j].getMtime()) * sortFactor;
        }
        Swapper swapper = (i, j) -> {
          Stat tempStat = stats[i];
          stats[i] = stats[j];
          stats[j] = tempStat;

          String tempTableName = tableNamesWithType.get(i);
          tableNamesWithType.set(i, tableNamesWithType.get(j));
          tableNamesWithType.set(j, tempTableName);
        };
        Arrays.quickSort(0, numTables, comparator, swapper);
        tableNames = tableNamesWithType;
      }

      return JsonUtils.newObjectNode().set("tables", JsonUtils.objectToJsonNode(tableNames)).toString();
    } catch (Exception e) {
      throw new ControllerApplicationException(LOGGER, e.getMessage(), Response.Status.INTERNAL_SERVER_ERROR, e);
    }
  }

  private enum SortType {
    NAME, CREATIONTIME, LASTMODIFIEDTIME
  }

  @GET
  @Path("/tables/{tableName}")
  @Authorize(targetType = TargetType.TABLE, paramName = "tableName", action = Actions.Table.GET_TABLE_CONFIG)
  @Produces(MediaType.APPLICATION_JSON)
  @ApiOperation(value = "Lists the table configs")
  public String listTableConfigs(
      @ApiParam(value = "Name of the table", required = true) @PathParam("tableName") String tableName,
      @ApiParam(value = "realtime|offline") @QueryParam("type") String tableTypeStr, @Context HttpHeaders headers) {
    try {
      tableName = DatabaseUtils.translateTableName(tableName, headers);
      ObjectNode ret = JsonUtils.newObjectNode();

      if ((tableTypeStr == null || TableType.OFFLINE.name().equalsIgnoreCase(tableTypeStr))
          && _pinotHelixResourceManager.hasOfflineTable(tableName)) {
        TableConfig tableConfig = _pinotHelixResourceManager.getOfflineTableConfig(tableName, false, false);
        Preconditions.checkNotNull(tableConfig);
        ret.set(TableType.OFFLINE.name(), tableConfig.toJsonNode());
      }

      if ((tableTypeStr == null || TableType.REALTIME.name().equalsIgnoreCase(tableTypeStr))
          && _pinotHelixResourceManager.hasRealtimeTable(tableName)) {
        TableConfig tableConfig = _pinotHelixResourceManager.getRealtimeTableConfig(tableName, false, false);
        Preconditions.checkNotNull(tableConfig);
        ret.set(TableType.REALTIME.name(), tableConfig.toJsonNode());
      }
      return ret.toString();
    } catch (Exception e) {
      throw new ControllerApplicationException(LOGGER, e.getMessage(), Response.Status.INTERNAL_SERVER_ERROR, e);
    }
  }

  @DELETE
  @Path("/tables/{tableName}")
  @Authorize(targetType = TargetType.TABLE, paramName = "tableName", action = Actions.Table.DELETE_TABLE)
  @Authenticate(AccessType.DELETE)
  @Produces(MediaType.APPLICATION_JSON)
  @ApiOperation(value = "Deletes a table", notes = "Deletes a table")
  public SuccessResponse deleteTable(
      @ApiParam(value = "Name of the table to delete", required = true) @PathParam("tableName") String tableName,
      @ApiParam(value = "realtime|offline") @QueryParam("type") String tableTypeStr,
      @ApiParam(value = "Retention period for the table segments (e.g. 12h, 3d); If not set, the retention period "
          + "will default to the first config that's not null: the cluster setting, then '7d'. Using 0d or -1d will "
          + "instantly delete segments without retention") @QueryParam("retention") String retentionPeriod,
      @Context HttpHeaders headers) {
    TableType tableType = Constants.validateTableType(tableTypeStr);

    List<String> tablesDeleted = new LinkedList<>();
    try {
      tableName = DatabaseUtils.translateTableName(tableName, headers);
      validateLogicalTableReference(tableName, tableType);
      boolean tableExist = false;
      if (verifyTableType(tableName, tableType, TableType.OFFLINE)) {
        tableExist = _pinotHelixResourceManager.hasOfflineTable(tableName);
        // Even the table name does not exist, still go on to delete remaining table metadata in case a previous delete
        // did not complete.
        _pinotHelixResourceManager.deleteOfflineTable(tableName, retentionPeriod);
        if (tableExist) {
          tablesDeleted.add(TableNameBuilder.OFFLINE.tableNameWithType(tableName));
        }
      }
      if (verifyTableType(tableName, tableType, TableType.REALTIME)) {
        tableExist = _pinotHelixResourceManager.hasRealtimeTable(tableName);
        // Even the table name does not exist, still go on to delete remaining table metadata in case a previous delete
        // did not complete.
        _pinotHelixResourceManager.deleteRealtimeTable(tableName, retentionPeriod);
        if (tableExist) {
          tablesDeleted.add(TableNameBuilder.REALTIME.tableNameWithType(tableName));
        }
      }
      if (!tablesDeleted.isEmpty()) {
        tablesDeleted.forEach(deletedTableName -> LOGGER.info("Successfully deleted table: {}", deletedTableName));
        return new SuccessResponse("Tables: " + tablesDeleted + " deleted");
      }
    } catch (Exception e) {
      throw new ControllerApplicationException(LOGGER, e.getMessage(), Response.Status.INTERNAL_SERVER_ERROR, e);
    }
    throw new ControllerApplicationException(LOGGER,
        "Table '" + tableName + "' with type " + tableType + " does not exist", Response.Status.NOT_FOUND);
  }

  //   Return true iff the table is of the expectedType based on the given tableName and tableType. The truth table:
  //        tableType   TableNameBuilder.getTableTypeFromTableName(tableName)   Return value
  //     1. null      null (i.e., table has no type suffix)           true
  //     2. null      not_null                              typeFromTableName == expectedType
  //     3. not_null      null                                    tableType == expectedType
  //     4. not_null      not_null                          tableType==typeFromTableName==expectedType
  private boolean verifyTableType(String tableName, TableType tableType, TableType expectedType) {
    if (tableType != null && tableType != expectedType) {
      return false;
    }
    TableType typeFromTableName = TableNameBuilder.getTableTypeFromTableName(tableName);
    return typeFromTableName == null || typeFromTableName == expectedType;
  }

  private void validateLogicalTableReference(String tableName, TableType tableType) {
    String tableNameWithType =
        tableType == null ? tableName : TableNameBuilder.forType(tableType).tableNameWithType(tableName);
    List<LogicalTableConfig> allLogicalTableConfigs =
        ZKMetadataProvider.getAllLogicalTableConfigs(_pinotHelixResourceManager.getPropertyStore());
    for (LogicalTableConfig logicalTableConfig : allLogicalTableConfigs) {
      if (LogicalTableConfigUtils.checkPhysicalTableRefExists(logicalTableConfig, tableNameWithType)) {
        throw new ControllerApplicationException(LOGGER,
            String.format("Cannot delete table config: %s because it is referenced in logical table: %s",
                tableName, logicalTableConfig.getTableName()), Response.Status.CONFLICT);
      }
    }
  }

  @PUT
  @Path("/tables/{tableName}")
  @Authorize(targetType = TargetType.TABLE, paramName = "tableName", action = Actions.Table.UPDATE_TABLE_CONFIG)
  @Authenticate(AccessType.UPDATE)
  @Produces(MediaType.APPLICATION_JSON)
  @ApiOperation(value = "Updates table config for a table", notes = "Updates table config for a table")
  public ConfigSuccessResponse updateTableConfig(
      @ApiParam(value = "Name of the table to update", required = true) @PathParam("tableName") String tableName,
      @ApiParam(value = "comma separated list of validation type(s) to skip. supported types: (ALL|TASK|UPSERT)")
      @QueryParam("validationTypesToSkip") @Nullable String typesToSkip, @Context HttpHeaders headers,
      String tableConfigString)
      throws Exception {
    Pair<TableConfig, Map<String, Object>> tableConfigAndUnrecognizedProperties;
    TableConfig tableConfig;
    String tableNameWithType;
    Schema schema;
    try {
      tableConfigAndUnrecognizedProperties =
          JsonUtils.stringToObjectAndUnrecognizedProperties(tableConfigString, TableConfig.class);
      tableConfig = tableConfigAndUnrecognizedProperties.getLeft();
      tableNameWithType = DatabaseUtils.translateTableName(tableConfig.getTableName(), headers);
      tableConfig.setTableName(tableNameWithType);
      String tableNameFromPath = DatabaseUtils.translateTableName(
          TableNameBuilder.forType(tableConfig.getTableType()).tableNameWithType(tableName), headers);
      if (!tableNameFromPath.equals(tableNameWithType)) {
        throw new ControllerApplicationException(LOGGER,
            "Request table " + tableNameFromPath + " does not match table name in the body " + tableNameWithType,
            Response.Status.BAD_REQUEST);
      }

      schema = _pinotHelixResourceManager.getTableSchema(tableNameWithType);
      Preconditions.checkState(schema != null, "Failed to find schema for table: %s", tableNameWithType);
      TableConfigUtils.validate(tableConfig, schema, typesToSkip);
    } catch (Exception e) {
      String msg = String.format("Invalid table config: %s with error: %s", tableName, e.getMessage());
      throw new ControllerApplicationException(LOGGER, msg, Response.Status.BAD_REQUEST, e);
    }

    try {
      if (!_pinotHelixResourceManager.hasTable(tableNameWithType)) {
        throw new ControllerApplicationException(LOGGER, "Table " + tableNameWithType + " does not exist",
            Response.Status.NOT_FOUND);
      }

      try {
        TableConfigUtils.ensureMinReplicas(tableConfig, _controllerConf.getDefaultTableMinReplicas());
        TableConfigUtils.ensureStorageQuotaConstraints(tableConfig, _controllerConf.getDimTableMaxSize());
        checkHybridTableConfig(TableNameBuilder.extractRawTableName(tableNameWithType), tableConfig);
        TaskConfigUtils.validateTaskConfigs(tableConfig, schema, _pinotTaskManager, typesToSkip);
        validateInstanceAssignment(tableConfig);
      } catch (Exception e) {
        throw new InvalidTableConfigException(e);
      }
      _pinotHelixResourceManager.updateTableConfig(tableConfig);
    } catch (InvalidTableConfigException e) {
      String errStr = String.format("Failed to update configuration for %s due to: %s", tableName, e.getMessage());
      _controllerMetrics.addMeteredGlobalValue(ControllerMeter.CONTROLLER_TABLE_UPDATE_ERROR, 1L);
      throw new ControllerApplicationException(LOGGER, errStr, Response.Status.BAD_REQUEST, e);
    } catch (Exception e) {
      _controllerMetrics.addMeteredGlobalValue(ControllerMeter.CONTROLLER_TABLE_UPDATE_ERROR, 1L);
      throw e;
    }
    LOGGER.info("Successfully updated table: {} with new config: {}", tableNameWithType, tableConfig);
    return new ConfigSuccessResponse("Table config updated for " + tableName,
        tableConfigAndUnrecognizedProperties.getRight());
  }

  @POST
  @Path("/tables/validate")
  @Produces(MediaType.APPLICATION_JSON)
  @ApiOperation(value = "Validate table config for a table",
      notes = "This API returns the table config that matches the one you get from 'GET /tables/{tableName}'."
          + " This allows us to validate table config before apply.")
  @ManualAuthorization // performed after parsing TableConfig
  public ObjectNode checkTableConfig(String tableConfigStr,
      @ApiParam(value = "comma separated list of validation type(s) to skip. supported types: (ALL|TASK|UPSERT)")
      @QueryParam("validationTypesToSkip") @Nullable String typesToSkip, @Context HttpHeaders httpHeaders,
      @Context Request request) {
    Pair<TableConfig, Map<String, Object>> tableConfigAndUnrecognizedProperties;
    try {
      tableConfigAndUnrecognizedProperties =
          JsonUtils.stringToObjectAndUnrecognizedProperties(tableConfigStr, TableConfig.class);
    } catch (IOException e) {
      String msg = String.format("Invalid table config json string: %s. Reason: %s", tableConfigStr, e.getMessage());
      throw new ControllerApplicationException(LOGGER, msg, Response.Status.BAD_REQUEST, e);
    }
    TableConfig tableConfig = tableConfigAndUnrecognizedProperties.getLeft();
    String tableNameWithType = DatabaseUtils.translateTableName(tableConfig.getTableName(), httpHeaders);
    tableConfig.setTableName(tableNameWithType);

    // validate permission
    ResourceUtils.checkPermissionAndAccess(tableNameWithType, request, httpHeaders,
        AccessType.READ, Actions.Table.VALIDATE_TABLE_CONFIGS, _accessControlFactory, LOGGER);

    ObjectNode validationResponse = validateConfig(tableConfig, typesToSkip);
    validationResponse.set("unrecognizedProperties",
        JsonUtils.objectToJsonNode(tableConfigAndUnrecognizedProperties.getRight()));
    return validationResponse;
  }

  private ObjectNode validateConfig(TableConfig tableConfig, @Nullable String typesToSkip) {
    String tableNameWithType = tableConfig.getTableName();
    try {
      Schema schema = _pinotHelixResourceManager.getTableSchema(tableNameWithType);
      if (schema == null) {
        throw new SchemaNotFoundException("Failed to find schema for table: " + tableNameWithType);
      }
      TableConfigUtils.validate(tableConfig, schema, typesToSkip);
      TaskConfigUtils.validateTaskConfigs(tableConfig, schema, _pinotTaskManager, typesToSkip);
      ObjectNode tableConfigValidateStr = JsonUtils.newObjectNode();
      if (tableConfig.getTableType() == TableType.OFFLINE) {
        tableConfigValidateStr.set(TableType.OFFLINE.name(), tableConfig.toJsonNode());
      } else {
        tableConfigValidateStr.set(TableType.REALTIME.name(), tableConfig.toJsonNode());
      }
      return tableConfigValidateStr;
    } catch (Exception e) {
      String msg = String.format("Invalid table config: %s. %s", tableNameWithType, e.getMessage());
      throw new ControllerApplicationException(LOGGER, msg, Response.Status.BAD_REQUEST, e);
    }
  }

  @POST
  @Produces(MediaType.APPLICATION_JSON)
  @Authenticate(AccessType.UPDATE)
  @Path("/tables/{tableName}/rebalance")
  @Authorize(targetType = TargetType.TABLE, paramName = "tableName", action = Actions.Table.REBALANCE_TABLE)
  @ApiOperation(value = "Rebalances a table (reassign instances and segments for a table)",
      notes = "Rebalances a table (reassign instances and segments for a table)")
  public RebalanceResult rebalance(
      //@formatter:off
      @ApiParam(value = "Name of the table to rebalance", required = true) @PathParam("tableName") String tableName,
      @ApiParam(value = "OFFLINE|REALTIME", required = true) @QueryParam("type") String tableTypeStr,
      @ApiParam(value = "Whether to rebalance table in dry-run mode") @DefaultValue("false") @QueryParam("dryRun")
      boolean dryRun,
      @ApiParam(value = "Whether to enable pre-checks for table, must be in dry-run mode to enable")
      @DefaultValue("false") @QueryParam("preChecks") boolean preChecks,
      @ApiParam(value = "Whether to reassign instances before reassigning segments") @DefaultValue("true")
      @QueryParam("reassignInstances") boolean reassignInstances,
      @ApiParam(value = "Whether to reassign CONSUMING segments for real-time table") @DefaultValue("true")
      @QueryParam("includeConsuming") boolean includeConsuming,
      @ApiParam(value = "Whether to enable minimize data movement on rebalance, DEFAULT will use "
          + "the minimizeDataMovement in table config") @DefaultValue("ENABLE")
      @QueryParam("minimizeDataMovement") Enablement minimizeDataMovement,
      @ApiParam(value = "Whether to rebalance table in bootstrap mode (regardless of minimum segment movement, "
          + "reassign all segments in a round-robin fashion as if adding new segments to an empty table)")
      @DefaultValue("false") @QueryParam("bootstrap") boolean bootstrap,
      @ApiParam(value = "Whether to allow downtime for the rebalance") @DefaultValue("false") @QueryParam("downtime")
      boolean downtime,
      @ApiParam(value = "For no-downtime rebalance, minimum number of replicas to keep alive during rebalance, or "
          + "maximum number of replicas allowed to be unavailable if value is negative") @DefaultValue("-1")
      @QueryParam("minAvailableReplicas") int minAvailableReplicas,
      @ApiParam(value = "For no-downtime rebalance, whether to enable low disk mode during rebalance. When enabled, "
          + "segments will first be offloaded from servers, then added to servers after offload is done while "
          + "maintaining the min available replicas. It may increase the total time of the rebalance, but can be "
          + "useful when servers are low on disk space, and we want to scale up the cluster and rebalance the table to "
          + "more servers.") @DefaultValue("false") @QueryParam("lowDiskMode") boolean lowDiskMode,
      @ApiParam(value = "Whether to use best-efforts to rebalance (not fail the rebalance when the no-downtime "
          + "contract cannot be achieved)") @DefaultValue("false") @QueryParam("bestEfforts") boolean bestEfforts,
      @ApiParam(value = "How many maximum segment adds per server to update in the IdealState in each step. For "
          + "non-strict replica group based assignment, this number will be capped at the batchSizePerServer value "
          + "per rebalance step (some servers may get fewer segments). For strict replica group based assignment, "
          + "this is a per-server best effort value since each partition of a replica group must be moved as a whole "
          + "and at least one partition in a replica group should be moved. A value of -1 is used to disable batching "
          + "(select as many segments as possible per incremental step in rebalance such that minAvailableReplicas is "
          + "honored).")
      @DefaultValue("-1") @QueryParam("batchSizePerServer") int batchSizePerServer,
      @ApiParam(value = "How often to check if external view converges with ideal states") @DefaultValue("1000")
      @QueryParam("externalViewCheckIntervalInMs") long externalViewCheckIntervalInMs,
      @ApiParam(value = "Maximum time (in milliseconds) to wait for external view to converge with ideal states. "
          + "Extends if progress has been made during the wait, otherwise times out") @DefaultValue("3600000")
      @QueryParam("externalViewStabilizationTimeoutInMs") long externalViewStabilizationTimeoutInMs,
      @ApiParam(value = "How often to make a status update (i.e. heartbeat)") @DefaultValue("300000")
      @QueryParam("heartbeatIntervalInMs") long heartbeatIntervalInMs,
      @ApiParam(value = "How long to wait for next status update (i.e. heartbeat) before the job is considered failed")
      @DefaultValue("3600000") @QueryParam("heartbeatTimeoutInMs") long heartbeatTimeoutInMs,
      @ApiParam(value = "Max number of attempts to rebalance") @DefaultValue("3") @QueryParam("maxAttempts")
      int maxAttempts,
      @ApiParam(value = "Initial delay to exponentially backoff retry") @DefaultValue("300000")
      @QueryParam("retryInitialDelayInMs") long retryInitialDelayInMs,
      @ApiParam(value = "Whether to update segment target tier as part of the rebalance") @DefaultValue("false")
      @QueryParam("updateTargetTier") boolean updateTargetTier,
      @Context HttpHeaders headers
      //@formatter:on
  ) {
    tableName = DatabaseUtils.translateTableName(tableName, headers);
    String tableNameWithType = constructTableNameWithType(tableName, tableTypeStr);
    RebalanceConfig rebalanceConfig = new RebalanceConfig();
    rebalanceConfig.setDryRun(dryRun);
    rebalanceConfig.setPreChecks(preChecks);
    rebalanceConfig.setReassignInstances(reassignInstances);
    rebalanceConfig.setIncludeConsuming(includeConsuming);
    rebalanceConfig.setMinimizeDataMovement(minimizeDataMovement);
    rebalanceConfig.setBootstrap(bootstrap);
    rebalanceConfig.setDowntime(downtime);
    rebalanceConfig.setMinAvailableReplicas(minAvailableReplicas);
    rebalanceConfig.setLowDiskMode(lowDiskMode);
    rebalanceConfig.setBestEfforts(bestEfforts);
    rebalanceConfig.setBatchSizePerServer(batchSizePerServer);
    rebalanceConfig.setExternalViewCheckIntervalInMs(externalViewCheckIntervalInMs);
    rebalanceConfig.setExternalViewStabilizationTimeoutInMs(externalViewStabilizationTimeoutInMs);
    heartbeatIntervalInMs = Math.max(externalViewCheckIntervalInMs, heartbeatIntervalInMs);
    rebalanceConfig.setHeartbeatIntervalInMs(heartbeatIntervalInMs);
    heartbeatTimeoutInMs = Math.max(heartbeatTimeoutInMs, 3 * heartbeatIntervalInMs);
    rebalanceConfig.setHeartbeatTimeoutInMs(heartbeatTimeoutInMs);
    rebalanceConfig.setMaxAttempts(maxAttempts);
    rebalanceConfig.setRetryInitialDelayInMs(retryInitialDelayInMs);
    rebalanceConfig.setUpdateTargetTier(updateTargetTier);
    String rebalanceJobId = TableRebalancer.createUniqueRebalanceJobIdentifier();

    try {
      if (dryRun || preChecks || downtime) {
        // For dry-run, preChecks or rebalance with downtime, it's fine to run the rebalance synchronously since it
        // should be a really short operation.
        return _tableRebalanceManager.rebalanceTable(tableNameWithType, rebalanceConfig, rebalanceJobId, false, false);
      } else {
        // Make a dry-run first to get the target assignment
        rebalanceConfig.setDryRun(true);
        RebalanceResult dryRunResult =
            _tableRebalanceManager.rebalanceTable(tableNameWithType, rebalanceConfig, rebalanceJobId, false, false);

        if (dryRunResult.getStatus() == RebalanceResult.Status.DONE) {
          // If dry-run succeeded, run rebalance asynchronously
          rebalanceConfig.setDryRun(false);
          CompletableFuture<RebalanceResult> rebalanceResultFuture =
              _tableRebalanceManager.rebalanceTableAsync(tableNameWithType, rebalanceConfig, rebalanceJobId, true,
                  true);
          rebalanceResultFuture.whenComplete((rebalanceResult, throwable) -> {
            if (throwable != null) {
              String errorMsg = String.format("Caught exception/error while rebalancing table: %s", tableNameWithType);
              LOGGER.error(errorMsg, throwable);
            }
          });
          boolean isJobIdPersisted =
              waitForRebalanceToPersist(dryRunResult.getJobId(), tableNameWithType, rebalanceResultFuture);

          if (rebalanceResultFuture.isDone()) {
            try {
              return rebalanceResultFuture.get();
            } catch (Throwable t) {
              if (!isJobIdPersisted) {
                // If the jobId is not persisted, we return the exception to indicate the rebalance failed.
                // Otherwise, polling the job id return NOT_FOUND indefinitely.
                throw new ControllerApplicationException(LOGGER, t.getMessage(), Response.Status.INTERNAL_SERVER_ERROR);
              }
            }
          }

          return new RebalanceResult(dryRunResult.getJobId(), RebalanceResult.Status.IN_PROGRESS,
              "In progress, check controller logs for updates", dryRunResult.getInstanceAssignment(),
              dryRunResult.getTierInstanceAssignment(), dryRunResult.getSegmentAssignment(),
              dryRunResult.getPreChecksResult(), dryRunResult.getRebalanceSummaryResult());
        } else {
          // If dry-run failed or is no-op, return the dry-run result
          return dryRunResult;
        }
      }
    } catch (TableNotFoundException e) {
      throw new ControllerApplicationException(LOGGER, e.getMessage(), Response.Status.NOT_FOUND);
    } catch (RebalanceInProgressException e) {
      throw new ControllerApplicationException(LOGGER, e.getMessage(), Response.Status.CONFLICT);
    }
  }

  /**
   * Waits for jobId to be persisted or the rebalance to complete using a retry policy.
   * Tables with 100k+ segments take up to a few seconds for the jobId to persist. This ensures the jobId is present
   * before returning the jobId to the caller, so they can correctly poll the jobId.
   */
  public boolean waitForRebalanceToPersist(
      String jobId, String tableNameWithType, Future<RebalanceResult> rebalanceResultFuture) {
    try {
      // This retry policy waits at most for 7.5s to 15s in total. This is chosen to cover typical delays for tables
      // with many segments and avoid excessive HTTP request timeouts.
      RetryPolicies.exponentialBackoffRetryPolicy(5, 500L, 2.0).attempt(() ->
          getControllerJobMetadata(jobId) != null || rebalanceResultFuture.isDone());
      return true;
    } catch (Exception e) {
      LOGGER.warn("waiting for jobId not successful while rebalancing table: {}", tableNameWithType);
      return false;
    }
  }

  public Map<String, String> getControllerJobMetadata(String jobId) {
    return _pinotHelixResourceManager.getControllerJobZKMetadata(jobId, ControllerJobTypes.TABLE_REBALANCE);
  }

  @DELETE
  @Produces(MediaType.APPLICATION_JSON)
  @Authenticate(AccessType.UPDATE)
  @Path("/tables/{tableName}/rebalance")
  @Authorize(targetType = TargetType.TABLE, paramName = "tableName", action = Actions.Table.CANCEL_REBALANCE)
  @ApiOperation(value = "Cancel all rebalance jobs for the given table, and noop if no rebalance is running", notes =
      "Cancel all rebalance jobs for the given table, and noop if no rebalance is running")
  public List<String> cancelRebalance(
      @ApiParam(value = "Name of the table to rebalance", required = true) @PathParam("tableName") String tableName,
      @ApiParam(value = "OFFLINE|REALTIME", required = true) @QueryParam("type") String tableTypeStr,
      @Context HttpHeaders headers) {
    tableName = DatabaseUtils.translateTableName(tableName, headers);
    String tableNameWithType = constructTableNameWithType(tableName, tableTypeStr);
    return _tableRebalanceManager.cancelRebalance(tableNameWithType);
  }

  @GET
  @Produces(MediaType.APPLICATION_JSON)
  @Authenticate(AccessType.UPDATE)
  @Path("/rebalanceStatus/{jobId}")
  @Authorize(targetType = TargetType.CLUSTER, action = Actions.Cluster.GET_REBALANCE_STATUS)
  @ApiOperation(value = "Gets detailed stats of a rebalance operation",
      notes = "Gets detailed stats of a rebalance operation")
  public ServerRebalanceJobStatusResponse rebalanceStatus(
      @ApiParam(value = "Rebalance Job Id", required = true) @PathParam("jobId") String jobId)
      throws JsonProcessingException {
    return _tableRebalanceManager.getRebalanceStatus(jobId);
  }

  @GET
  @Produces(MediaType.APPLICATION_JSON)
  @Path("/tables/{tableName}/state")
  @Authorize(targetType = TargetType.TABLE, paramName = "tableName", action = Actions.Table.GET_STATE)
  @ApiOperation(value = "Get current table state", notes = "Get current table state")
  public String getTableState(
      @ApiParam(value = "Name of the table to get its state", required = true) @PathParam("tableName") String tableName,
      @ApiParam(value = "realtime|offline", required = true) @QueryParam("type") String tableTypeStr,
      @Context HttpHeaders headers) {
    tableName = DatabaseUtils.translateTableName(tableName, headers);
    String tableNameWithType = constructTableNameWithType(tableName, tableTypeStr);
    try {
      ObjectNode data = JsonUtils.newObjectNode();
      data.put("state", _pinotHelixResourceManager.isTableEnabled(tableNameWithType) ? "enabled" : "disabled");
      return data.toString();
    } catch (TableNotFoundException e) {
      throw new ControllerApplicationException(LOGGER, "Failed to find table: " + tableNameWithType,
          Response.Status.NOT_FOUND);
    }
  }

  @PUT
  @Path("/tables/{tableName}/state")
  @Authenticate(AccessType.UPDATE)
  @Produces(MediaType.APPLICATION_JSON)
  @Consumes(MediaType.TEXT_PLAIN)
  @ApiOperation(value = "Enable/disable a table", notes = "Enable/disable a table")
  @ApiResponses(value = {
      @ApiResponse(code = 200, message = "Success"),
      @ApiResponse(code = 400, message = "Bad Request"),
      @ApiResponse(code = 404, message = "Table not found"),
      @ApiResponse(code = 500, message = "Internal error")
  })
  public SuccessResponse toggleTableState(
      @ApiParam(value = "Table name", required = true) @PathParam("tableName") String tableName,
      @ApiParam(value = "realtime|offline", required = true) @QueryParam("type") String tableTypeStr,
      @ApiParam(value = "enable|disable", required = true) @QueryParam("state") String state,
      @Context HttpHeaders headers) {
    tableName = DatabaseUtils.translateTableName(tableName, headers);
    String tableNameWithType = constructTableNameWithType(tableName, tableTypeStr);
    StateType stateType;
    if (StateType.ENABLE.name().equalsIgnoreCase(state)) {
      stateType = StateType.ENABLE;
    } else if (StateType.DISABLE.name().equalsIgnoreCase(state)) {
      stateType = StateType.DISABLE;
    } else {
      throw new ControllerApplicationException(LOGGER, "Unknown state '" + state + "'", Response.Status.BAD_REQUEST);
    }
    if (!_pinotHelixResourceManager.hasTable(tableNameWithType)) {
      throw new ControllerApplicationException(LOGGER, "Table '" + tableName + "' does not exist",
          Response.Status.NOT_FOUND);
    }
    PinotResourceManagerResponse response = _pinotHelixResourceManager.toggleTableState(tableNameWithType, stateType);
    if (response.isSuccessful()) {
      return new SuccessResponse("Request to " + state + " table '" + tableNameWithType + "' is successful");
    } else {
      throw new ControllerApplicationException(LOGGER,
          "Failed to " + state + " table '" + tableNameWithType + "': " + response.getMessage(),
          Response.Status.INTERNAL_SERVER_ERROR);
    }
  }

  @GET
  @Path("/tables/{tableName}/stats")
  @Authorize(targetType = TargetType.TABLE, paramName = "tableName", action = Actions.Table.GET_METADATA)
  @Produces(MediaType.APPLICATION_JSON)
  @ApiOperation(value = "table stats", notes = "Provides metadata info/stats about the table.")
  public String getTableStats(
      @ApiParam(value = "Name of the table", required = true) @PathParam("tableName") String tableName,
      @ApiParam(value = "realtime|offline") @QueryParam("type") String tableTypeStr, @Context HttpHeaders headers) {
    tableName = DatabaseUtils.translateTableName(tableName, headers);
    ObjectNode ret = JsonUtils.newObjectNode();
    if ((tableTypeStr == null || TableType.OFFLINE.name().equalsIgnoreCase(tableTypeStr))
        && _pinotHelixResourceManager.hasOfflineTable(tableName)) {
      String tableNameWithType = TableNameBuilder.forType(TableType.OFFLINE).tableNameWithType(tableName);
      TableStatsHumanReadable tableStats = _pinotHelixResourceManager.getTableStatsHumanReadable(tableNameWithType);
      ret.set(TableType.OFFLINE.name(), JsonUtils.objectToJsonNode(tableStats));
    }
    if ((tableTypeStr == null || TableType.REALTIME.name().equalsIgnoreCase(tableTypeStr))
        && _pinotHelixResourceManager.hasRealtimeTable(tableName)) {
      String tableNameWithType = TableNameBuilder.forType(TableType.REALTIME).tableNameWithType(tableName);
      TableStatsHumanReadable tableStats = _pinotHelixResourceManager.getTableStatsHumanReadable(tableNameWithType);
      ret.set(TableType.REALTIME.name(), JsonUtils.objectToJsonNode(tableStats));
    }
    return ret.toString();
  }

  private String constructTableNameWithType(String tableName, String tableTypeStr) {
    TableType tableType;
    try {
      tableType = TableType.valueOf(tableTypeStr.toUpperCase());
    } catch (Exception e) {
      throw new ControllerApplicationException(LOGGER, "Illegal table type: " + tableTypeStr,
          Response.Status.BAD_REQUEST);
    }
    return TableNameBuilder.forType(tableType).tableNameWithType(tableName);
  }

  private void checkHybridTableConfig(String rawTableName, TableConfig tableConfig) {
    if (tableConfig.getTableType() == TableType.REALTIME) {
      if (_pinotHelixResourceManager.hasOfflineTable(rawTableName)) {
        TableConfigUtils.verifyHybridTableConfigs(rawTableName,
            _pinotHelixResourceManager.getOfflineTableConfig(rawTableName), tableConfig);
      }
    } else {
      if (_pinotHelixResourceManager.hasRealtimeTable(rawTableName)) {
        TableConfigUtils.verifyHybridTableConfigs(rawTableName, tableConfig,
            _pinotHelixResourceManager.getRealtimeTableConfig(rawTableName));
      }
    }
  }

  @GET
  @Path("/tables/{tableName}/status")
  @Authorize(targetType = TargetType.TABLE, paramName = "tableName", action = Actions.Table.GET_METADATA)
  @Produces(MediaType.APPLICATION_JSON)
  @ApiOperation(value = "table status", notes = "Provides status of the table including ingestion status")
  public String getTableStatus(
      @ApiParam(value = "Name of the table", required = true) @PathParam("tableName") String tableName,
      @ApiParam(value = "realtime|offline") @QueryParam("type") String tableTypeStr, @Context HttpHeaders headers) {
    tableName = DatabaseUtils.translateTableName(tableName, headers);
    try {
      TableType tableType = Constants.validateTableType(tableTypeStr);
      if (tableType == null) {
        throw new ControllerApplicationException(LOGGER, "Table type should either be realtime|offline",
            Response.Status.BAD_REQUEST);
      }
      String tableNameWithType = TableNameBuilder.forType(tableType).tableNameWithType(tableName);
      if (!_pinotHelixResourceManager.hasTable(tableNameWithType)) {
        throw new ControllerApplicationException(LOGGER,
            "Specified table name: " + tableName + " of type: " + tableTypeStr + " does not exist.",
            Response.Status.BAD_REQUEST);
      }
      TableStatus.IngestionStatus ingestionStatus = null;
      if (TableType.OFFLINE == tableType) {
        ingestionStatus =
            TableIngestionStatusHelper.getOfflineTableIngestionStatus(tableNameWithType, _pinotHelixResourceManager,
                _pinotHelixTaskResourceManager);
      } else {
        ingestionStatus = TableIngestionStatusHelper.getRealtimeTableIngestionStatus(tableNameWithType,
            _controllerConf.getServerAdminRequestTimeoutSeconds() * 1000, _executor, _connectionManager,
            _pinotHelixResourceManager);
      }
      TableStatus tableStatus = new TableStatus(ingestionStatus);
      return JsonUtils.objectToPrettyString(tableStatus);
    } catch (Exception e) {
      throw new ControllerApplicationException(LOGGER,
          String.format("Failed to get status (ingestion status) for table %s. Reason: %s", tableName, e.getMessage()),
          Response.Status.INTERNAL_SERVER_ERROR, e);
    }
  }

  @GET
  @Path("tables/{tableName}/metadata")
  @Authorize(targetType = TargetType.TABLE, paramName = "tableName", action = Actions.Table.GET_METADATA)
  @Produces(MediaType.APPLICATION_JSON)
  @ApiOperation(value = "Get the aggregate metadata of all segments for a table",
      notes = "Get the aggregate metadata of all segments for a table")
  public String getTableAggregateMetadata(
      @ApiParam(value = "Name of the table", required = true) @PathParam("tableName") String tableName,
      @ApiParam(value = "OFFLINE|REALTIME") @QueryParam("type") String tableTypeStr,
      @ApiParam(value = "Columns name", allowMultiple = true) @QueryParam("columns") @DefaultValue("")
      List<String> columns, @Context HttpHeaders headers) {
    tableName = DatabaseUtils.translateTableName(tableName, headers);
    LOGGER.info("Received a request to fetch aggregate metadata for a table {}", tableName);
    TableType tableType = Constants.validateTableType(tableTypeStr);
    if (tableType == TableType.REALTIME) {
      throw new ControllerApplicationException(LOGGER, "Table type : " + tableTypeStr + " not yet supported.",
          Response.Status.NOT_IMPLEMENTED);
    }
    String tableNameWithType =
        ResourceUtils.getExistingTableNamesWithType(_pinotHelixResourceManager, tableName, tableType, LOGGER).get(0);
    TableConfig tableConfig = _pinotHelixResourceManager.getTableConfig(tableNameWithType);
    int numReplica = tableConfig == null ? 1 : tableConfig.getReplication();

    String segmentsMetadata;
    try {
      JsonNode segmentsMetadataJson = getAggregateMetadataFromServer(tableNameWithType, columns, numReplica);
      segmentsMetadata = JsonUtils.objectToPrettyString(segmentsMetadataJson);
    } catch (InvalidConfigException e) {
      throw new ControllerApplicationException(LOGGER, e.getMessage(), Response.Status.BAD_REQUEST);
    } catch (IOException ioe) {
      throw new ControllerApplicationException(LOGGER, "Error parsing Pinot server response: " + ioe.getMessage(),
          Response.Status.INTERNAL_SERVER_ERROR, ioe);
    }
    return segmentsMetadata;
  }

  @GET
  @Path("tables/{tableName}/validDocIdsMetadata")
  @Authorize(targetType = TargetType.TABLE, paramName = "tableName", action = Actions.Table.GET_METADATA)
  @Produces(MediaType.APPLICATION_JSON)
  @ApiOperation(value = "Get the aggregate validDocIds metadata of all segments for a table", notes = "Get the "
      + "aggregate validDocIds metadata of all segments for a table")
  public String getTableAggregateValidDocIdsMetadata(
      @ApiParam(value = "Name of the table", required = true) @PathParam("tableName") String tableName,
      @ApiParam(value = "OFFLINE|REALTIME") @QueryParam("type") String tableTypeStr,
      @ApiParam(value = "A list of segments", allowMultiple = true) @QueryParam("segmentNames")
      List<String> segmentNames,
      @ApiParam(value = "Valid doc ids type") @QueryParam("validDocIdsType")
      @DefaultValue("SNAPSHOT") ValidDocIdsType validDocIdsType,
      @ApiParam(value = "Number of segments in a batch per server request")
      @QueryParam("serverRequestBatchSize") @DefaultValue("500") int serverRequestBatchSize,
      @Context HttpHeaders headers) {
    tableName = DatabaseUtils.translateTableName(tableName, headers);
    LOGGER.info("Received a request to fetch aggregate validDocIds metadata for a table {}", tableName);
    TableType tableType = Constants.validateTableType(tableTypeStr);
    if (tableType == TableType.OFFLINE) {
      throw new ControllerApplicationException(LOGGER, "Table type : " + tableTypeStr + " not yet supported.",
          Response.Status.NOT_IMPLEMENTED);
    }
    String tableNameWithType =
        ResourceUtils.getExistingTableNamesWithType(_pinotHelixResourceManager, tableName, tableType, LOGGER).get(0);

    String validDocIdsMetadata;
    try {
      TableMetadataReader tableMetadataReader =
          new TableMetadataReader(_executor, _connectionManager, _pinotHelixResourceManager);
      validDocIdsType = (validDocIdsType == null) ? ValidDocIdsType.SNAPSHOT : validDocIdsType;
      JsonNode segmentsMetadataJson =
          tableMetadataReader.getAggregateValidDocIdsMetadata(tableNameWithType, segmentNames,
              validDocIdsType.toString(), _controllerConf.getServerAdminRequestTimeoutSeconds() * 1000,
              serverRequestBatchSize);
      validDocIdsMetadata = JsonUtils.objectToPrettyString(segmentsMetadataJson);
    } catch (InvalidConfigException e) {
      throw new ControllerApplicationException(LOGGER, e.getMessage(), Response.Status.BAD_REQUEST);
    } catch (IOException ioe) {
      throw new ControllerApplicationException(LOGGER, "Error parsing Pinot server response: " + ioe.getMessage(),
          Response.Status.INTERNAL_SERVER_ERROR, ioe);
    }
    return validDocIdsMetadata;
  }

  @GET
  @Path("tables/{tableName}/indexes")
  @Authorize(targetType = TargetType.TABLE, paramName = "tableName", action = Actions.Table.GET_METADATA)
  @Produces(MediaType.APPLICATION_JSON)
  @ApiOperation(value = "Get the aggregate index details of all segments for a table", notes = "Get the aggregate "
      + "index details of all segments for a table")
  public String getTableIndexes(
      @ApiParam(value = "Name of the table", required = true) @PathParam("tableName") String tableName,
      @ApiParam(value = "OFFLINE|REALTIME") @QueryParam("type") String tableTypeStr, @Context HttpHeaders headers) {
    tableName = DatabaseUtils.translateTableName(tableName, headers);
    LOGGER.info("Received a request to fetch aggregate metadata for a table {}", tableName);
    TableType tableType = Constants.validateTableType(tableTypeStr);
    String tableNameWithType =
        ResourceUtils.getExistingTableNamesWithType(_pinotHelixResourceManager, tableName, tableType, LOGGER).get(0);

    String tableIndexMetadata;
    try {
      JsonNode segmentsMetadataJson = getAggregateIndexMetadataFromServer(tableNameWithType);
      tableIndexMetadata = JsonUtils.objectToPrettyString(segmentsMetadataJson);
    } catch (InvalidConfigException e) {
      throw new ControllerApplicationException(LOGGER, e.getMessage(), Response.Status.BAD_REQUEST);
    } catch (IOException ioe) {
      throw new ControllerApplicationException(LOGGER, "Error parsing Pinot server response: " + ioe.getMessage(),
          Response.Status.INTERNAL_SERVER_ERROR, ioe);
    }
    return tableIndexMetadata;
  }

  private JsonNode getAggregateIndexMetadataFromServer(String tableNameWithType)
      throws InvalidConfigException, JsonProcessingException {
    final Map<String, List<String>> serverToSegments =
        _pinotHelixResourceManager.getServerToSegmentsMap(tableNameWithType);

    BiMap<String, String> serverEndPoints =
        _pinotHelixResourceManager.getDataInstanceAdminEndpoints(serverToSegments.keySet());
    CompletionServiceHelper completionServiceHelper =
        new CompletionServiceHelper(_executor, _connectionManager, serverEndPoints);

    List<String> serverUrls = new ArrayList<>();
    BiMap<String, String> endpointsToServers = serverEndPoints.inverse();
    for (String endpoint : endpointsToServers.keySet()) {
      String segmentIndexesEndpoint = endpoint + String.format("/tables/%s/indexes", tableNameWithType);
      serverUrls.add(segmentIndexesEndpoint);
    }

    CompletionServiceHelper.CompletionServiceResponse serviceResponse =
        completionServiceHelper.doMultiGetRequest(serverUrls, null, true, 10000);

    int totalSegments = 0;
    Map<String, Map<String, Integer>> columnToIndexCountMap = new HashMap<>();
    for (Map.Entry<String, String> streamResponse : serviceResponse._httpResponses.entrySet()) {
      String responseString = streamResponse.getValue();
      TableIndexMetadataResponse response = JsonUtils.stringToObject(responseString, TableIndexMetadataResponse.class);
      totalSegments += response.getTotalOnlineSegments();
      response.getColumnToIndexesCount().forEach((col, indexToCount) -> {
        Map<String, Integer> indexCountMap = columnToIndexCountMap.computeIfAbsent(col, c -> new HashMap<>());
        indexToCount.forEach((indexName, count) -> {
          indexCountMap.merge(indexName, count, Integer::sum);
        });
      });
    }

    TableIndexMetadataResponse tableIndexMetadataResponse =
        new TableIndexMetadataResponse(totalSegments, columnToIndexCountMap);

    return JsonUtils.objectToJsonNode(tableIndexMetadataResponse);
  }

  /**
   * This is a helper method to get the metadata for all segments for a given table name.
   * @param tableNameWithType name of the table along with its type
   * @param columns name of the columns
   * @param numReplica num or replica for the table
   * @return aggregated metadata of the table segments
   */
  private JsonNode getAggregateMetadataFromServer(String tableNameWithType, List<String> columns, int numReplica)
      throws InvalidConfigException, IOException {
    TableMetadataReader tableMetadataReader =
        new TableMetadataReader(_executor, _connectionManager, _pinotHelixResourceManager);
    return tableMetadataReader.getAggregateTableMetadata(tableNameWithType, columns, numReplica,
        _controllerConf.getServerAdminRequestTimeoutSeconds() * 1000);
  }

  @GET
  @Path("table/{tableName}/jobs")
  @Authorize(targetType = TargetType.TABLE, paramName = "tableName", action = Actions.Table.GET_CONTROLLER_JOBS)
  @Produces(MediaType.APPLICATION_JSON)
  @ApiOperation(value = "Get list of controller jobs for this table",
      notes = "Get list of controller jobs for this table")
  public Map<String, Map<String, String>> getControllerJobs(
      @ApiParam(value = "Name of the table", required = true) @PathParam("tableName") String tableName,
      @ApiParam(value = "OFFLINE|REALTIME") @QueryParam("type") String tableTypeStr,
      @ApiParam(value = "Comma separated list of job types") @QueryParam("jobTypes") @Nullable String jobTypesString,
      @Context HttpHeaders headers) {
    tableName = DatabaseUtils.translateTableName(tableName, headers);
    TableType tableTypeFromRequest = Constants.validateTableType(tableTypeStr);
    List<String> tableNamesWithType =
        ResourceUtils.getExistingTableNamesWithType(_pinotHelixResourceManager, tableName, tableTypeFromRequest,
            LOGGER);
    Set<ControllerJobType> jobTypesToFilter = null;
    if (StringUtils.isNotEmpty(jobTypesString)) {
      jobTypesToFilter = new HashSet<>();
      for (String jobTypeStr : StringUtils.split(jobTypesString, ',')) {
        ControllerJobTypes jobType;
        try {
          jobType = ControllerJobTypes.valueOf(jobTypeStr.toUpperCase());
        } catch (IllegalArgumentException e) {
          throw new ControllerApplicationException(LOGGER, "Unknown job type: " + jobTypeStr,
              Response.Status.BAD_REQUEST);
        }
        jobTypesToFilter.add(jobType);
      }
    }
    Map<String, Map<String, String>> result = new HashMap<>();
    for (String tableNameWithType : tableNamesWithType) {
      result.putAll(_pinotHelixResourceManager.getAllJobs(jobTypesToFilter == null
              ? new HashSet<>(EnumSet.allOf(ControllerJobTypes.class)) : jobTypesToFilter,
          jobMetadata -> jobMetadata.get(CommonConstants.ControllerJob.TABLE_NAME_WITH_TYPE)
              .equals(tableNameWithType)));
    }
    return result;
  }

  @POST
  @Path("tables/{tableName}/timeBoundary")
  @Authorize(targetType = TargetType.TABLE, paramName = "tableName", action = Actions.Table.UPDATE_TABLE_CONFIG)
  @ApiOperation(value = "Set hybrid table query time boundary based on offline segments' metadata", notes = "Set "
      + "hybrid table query time boundary based on offline segments' metadata")
  @Produces(MediaType.APPLICATION_JSON)
  public SuccessResponse setTimeBoundary(
      @ApiParam(value = "Name of the hybrid table (without type suffix)", required = true) @PathParam("tableName")
      String tableName, @Context HttpHeaders headers)
      throws Exception {
    tableName = DatabaseUtils.translateTableName(tableName, headers);
    // Validate its a hybrid table
    if (!_pinotHelixResourceManager.hasRealtimeTable(tableName) || !_pinotHelixResourceManager.hasOfflineTable(
        tableName)) {
      throw new ControllerApplicationException(LOGGER, "Table isn't a hybrid table", Response.Status.NOT_FOUND);
    }

    // Call all servers to validate all segments loaded and return the time boundary (max end time of all segments)
    String offlineTableName = TableNameBuilder.OFFLINE.tableNameWithType(tableName);
    long timeBoundaryMs = validateSegmentStateForTable(offlineTableName);
    if (timeBoundaryMs < 0) {
      throw new ControllerApplicationException(LOGGER,
          "No segments found for offline table : " + offlineTableName + ". Could not update time boundary.",
          Response.Status.SERVICE_UNAVAILABLE);
    }

    // Set the timeBoundary in tableIdealState
    IdealState idealState =
        HelixHelper.updateIdealState(_pinotHelixResourceManager.getHelixZkManager(), offlineTableName, is -> {
          is.getRecord()
              .setSimpleField(CommonConstants.IdealState.HYBRID_TABLE_TIME_BOUNDARY, Long.toString(timeBoundaryMs));
          return is;
        }, RetryPolicies.exponentialBackoffRetryPolicy(5, 1000L, 1.2f));

    if (idealState == null) {
      throw new ControllerApplicationException(LOGGER, "Could not update time boundary",
          Response.Status.INTERNAL_SERVER_ERROR);
    }

    return new SuccessResponse("Time boundary successfully updated to: " + timeBoundaryMs);
  }

  @DELETE
  @Path("tables/{tableName}/timeBoundary")
  @Authorize(targetType = TargetType.TABLE, paramName = "tableName", action = Actions.Table.DELETE_TIME_BOUNDARY)
  @ApiOperation(value = "Delete hybrid table query time boundary", notes = "Delete hybrid table query time boundary")
  @Produces(MediaType.APPLICATION_JSON)
  public SuccessResponse deleteTimeBoundary(
      @ApiParam(value = "Name of the hybrid table (without type suffix)", required = true) @PathParam("tableName")
      String tableName, @Context HttpHeaders headers) {
    tableName = DatabaseUtils.translateTableName(tableName, headers);
    String offlineTableName = TableNameBuilder.OFFLINE.tableNameWithType(tableName);
    if (!_pinotHelixResourceManager.hasTable(offlineTableName)) {
      throw new ControllerApplicationException(LOGGER, "Failed to find table: " + offlineTableName,
          Response.Status.NOT_FOUND);
    }

    // Delete the timeBoundary in tableIdealState
    IdealState idealState =
        HelixHelper.updateIdealState(_pinotHelixResourceManager.getHelixZkManager(), offlineTableName, is -> {
          is.getRecord().getSimpleFields().remove(CommonConstants.IdealState.HYBRID_TABLE_TIME_BOUNDARY);
          return is;
        }, RetryPolicies.exponentialBackoffRetryPolicy(5, 1000L, 1.2f));

    if (idealState == null) {
      throw new ControllerApplicationException(LOGGER, "Could not remove time boundary",
          Response.Status.INTERNAL_SERVER_ERROR);
    }

    return new SuccessResponse("Time boundary successfully removed");
  }

  private long validateSegmentStateForTable(String offlineTableName)
      throws InvalidConfigException, JsonProcessingException {
    // Call all servers to validate offline table state
    Map<String, List<String>> serverToSegments = _pinotHelixResourceManager.getServerToSegmentsMap(offlineTableName);
    BiMap<String, String> serverEndPoints =
        _pinotHelixResourceManager.getDataInstanceAdminEndpoints(serverToSegments.keySet());
    CompletionServiceHelper completionServiceHelper =
        new CompletionServiceHelper(_executor, _connectionManager, serverEndPoints);
    List<String> serverUrls = new ArrayList<>();
    BiMap<String, String> endpointsToServers = serverEndPoints.inverse();
    for (String endpoint : endpointsToServers.keySet()) {
      String reloadTaskStatusEndpoint = endpoint + "/tables/" + offlineTableName + "/allSegmentsLoaded";
      serverUrls.add(reloadTaskStatusEndpoint);
    }

    CompletionServiceHelper.CompletionServiceResponse serviceResponse =
        completionServiceHelper.doMultiGetRequest(serverUrls, null, true, 10000);

    if (serviceResponse._failedResponseCount > 0) {
      throw new ControllerApplicationException(LOGGER, "Could not validate table segment status",
          Response.Status.SERVICE_UNAVAILABLE);
    }

    long timeBoundaryMs = -1;
    // Validate all responses
    for (String response : serviceResponse._httpResponses.values()) {
      TableSegmentValidationInfo tableSegmentValidationInfo =
          JsonUtils.stringToObject(response, TableSegmentValidationInfo.class);
      if (!tableSegmentValidationInfo.isValid()) {
        String error = "Table segment validation failed. error=" + tableSegmentValidationInfo.getInvalidReason();
        throw new ControllerApplicationException(LOGGER, error, Response.Status.PRECONDITION_FAILED);
      }
      timeBoundaryMs = Math.max(timeBoundaryMs, tableSegmentValidationInfo.getMaxEndTimeMs());
    }

    return timeBoundaryMs;
  }

  /**
   * Try to calculate the instance partitions for the given table config. Throws exception if it fails.
   */
  private void validateInstanceAssignment(TableConfig tableConfig) {
    TableRebalancer tableRebalancer = new TableRebalancer(_pinotHelixResourceManager.getHelixZkManager());
    try {
      tableRebalancer.getInstancePartitionsMap(tableConfig, true, true, true);
    } catch (Exception e) {
      throw new RuntimeException(
          "Failed to calculate instance partitions for table: " + tableConfig.getTableName() + ", reason: "
              + e.getMessage());
    }
  }
}
