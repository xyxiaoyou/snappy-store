package com.pivotal.gemfirexd.internal.engine.ddl.catalog;

import java.sql.Clob;
import java.sql.SQLException;

import com.gemstone.gemfire.internal.cache.DistributedRegion;
import com.gemstone.gemfire.internal.cache.ExternalTableMetaData;
import com.gemstone.gemfire.internal.cache.LocalRegion;
import com.gemstone.gemfire.internal.cache.PartitionedRegion;
import com.gemstone.gemfire.internal.snappy.CallbackFactoryProvider;
import com.pivotal.gemfirexd.internal.engine.GfxdConstants;
import com.pivotal.gemfirexd.internal.engine.Misc;
import com.pivotal.gemfirexd.internal.engine.ddl.callbacks.CallbackProcedures;
import com.pivotal.gemfirexd.internal.engine.distributed.utils.GemFireXDUtils;
import com.pivotal.gemfirexd.internal.engine.store.GemFireContainer;
import com.pivotal.gemfirexd.internal.iapi.error.PublicAPI;
import com.pivotal.gemfirexd.internal.iapi.error.StandardException;
import com.pivotal.gemfirexd.internal.iapi.sql.conn.ConnectionUtil;
import com.pivotal.gemfirexd.internal.iapi.types.HarmonySerialClob;
import com.pivotal.gemfirexd.internal.impl.jdbc.Util;
import com.pivotal.gemfirexd.internal.shared.common.reference.SQLState;
import com.pivotal.gemfirexd.internal.shared.common.sanity.SanityManager;

public class V2ConnectorProcedures extends GfxdSystemProcedures {
  /**
   *
   * @param tableName input param - table for which metadata is needed
   * @param schemaAsJson output param - Schema of the table
   * @param storageType table's type viz. row/column
   * @param bucketCount output param - 0 for replicated tables otherwise the actual count
   * @param partColumns output param - partitioning columns
   * @param bucketToServerMapping output param - bucket to server mapping for partitioned tables OR
   *                              replica to server mapping for replicated table
   * @throws SQLException
   */
  public static void V2_GET_TABLE_METADATA(
      String tableName,
      Clob[] schemaAsJson,
      String[] storageType,
      int[] bucketCount,
      String[] partColumns,
      Clob[] bucketToServerMapping
  ) throws SQLException {

    String schema;
    String table;
    int dotIndex;
    // NULL table name is illegal
    if (tableName == null) {
      throw Util.generateCsSQLException(SQLState.ENTITY_NAME_MISSING);
    }

    if (GemFireXDUtils.TraceSysProcedures) {
      SanityManager.DEBUG_PRINT(GfxdConstants.TRACE_SYS_PROCEDURES,
          "executing V2_GET_TABLE_METADATA for table " + tableName);
    }

    if ((dotIndex = tableName.indexOf('.')) >= 0) {
      schema = tableName.substring(0, dotIndex);
      table = tableName.substring(dotIndex + 1);
    } else {
      schema = Misc.getDefaultSchemaName(ConnectionUtil.getCurrentLCC());
      table = tableName;
    }

//    GET_COLUMN_TABLE_SCHEMA(schema, table, schemaAsJson);

    // get other attributes bucket count, partitioning cols,
    // bucket to server/replica to server mapping
    try {
      final GemFireContainer container = CallbackProcedures
          .getContainerForTable(schema, table);

      ExternalTableMetaData externalTableMetaData = container.fetchHiveMetaData(false);
      String schemaString = CallbackFactoryProvider.getStoreCallbacks().jsonSchema(externalTableMetaData.schema);
      schemaAsJson[0] = new HarmonySerialClob(schemaString);
      storageType[0] = externalTableMetaData.tableType;

      final LocalRegion region = container.getRegion();
      if (region.getAttributes().getPartitionAttributes() != null) {
        getPRMetaData((PartitionedRegion)region, tableName,
            partColumns, bucketCount, bucketToServerMapping);
      } else {
        getRRMetaData((DistributedRegion)region, bucketToServerMapping);
        bucketCount[0] = 0;
      }
    } catch (StandardException se) {
      // getContainerForTable can throw error for external tables
      // (parquet / csv etc.)
      if (se.getSQLState().equals(SQLState.LANG_TABLE_NOT_FOUND)) {
        bucketCount[0] = 0;
        partColumns[0] = null;
        bucketToServerMapping[0] = new HarmonySerialClob(""); // to avoid NPE
      } else {
        throw PublicAPI.wrapStandardException(se);
      }
    }
  }
}
