package com.pivotal.gemfirexd.internal.engine.distributed.execution;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import com.gemstone.gemfire.DataSerializer;
import com.gemstone.gemfire.internal.shared.Version;
import com.pivotal.gemfirexd.internal.engine.distributed.DVDIOUtil;
import com.pivotal.gemfirexd.internal.iapi.error.StandardException;
import com.pivotal.gemfirexd.internal.iapi.types.DataValueDescriptor;
import com.pivotal.gemfirexd.internal.snappy.CallbackFactoryProvider;
import com.pivotal.gemfirexd.internal.snappy.LeadNodeExecutionContext;
import com.pivotal.gemfirexd.internal.snappy.SparkSQLExecute;

public class SampleInsertExecutionObject extends LeadNodeExecutionObject {

  private String baseTableName;
  private List<DataValueDescriptor[]> rows;
  public SampleInsertExecutionObject(String baseTableName, List<DataValueDescriptor[]>
    rows) {
    this.baseTableName = baseTableName;
    this.rows = rows;
  }

  /**
   * Default constructor for deserialization. Not to be invoked directly.
   */
  public SampleInsertExecutionObject() {}

  public SparkSQLExecute getSparkSQlExecute(Version v, LeadNodeExecutionContext ctx) throws Exception {

    return CallbackFactoryProvider.getClusterCallbacks().getSampleInsertExecute(
      this.baseTableName, ctx, v, this.rows);
  }

  @Override
  public boolean isUpdateOrDeleteOrPut() {
    return true;
  }

  @Override
  public void reset() {

  }

  @Override
  public byte getGfxdID() {
    return SAMPLE_INSERT_EXEC_OBJECT;
  }

  @Override
  public void toData(DataOutput out) throws IOException {
    int numCols = this.rows.get(0).length;
    int numEightColGroups = numCols / 8 + (numCols % 8 == 0 ? 0 : 1);
    byte numPartCols = (byte)(numCols % 8);
    if (numPartCols == 0) {
      numPartCols = 8;
    }
    DataSerializer.writeString(this.baseTableName, out);
    DataSerializer.writePrimitiveInt(numEightColGroups, out);
    DataSerializer.writePrimitiveByte(numPartCols, out);
    DataSerializer.writePrimitiveInt(rows.size(), out);
    try {
      for (DataValueDescriptor[] dvds : rows) {
        DVDIOUtil.writeDVDArray(dvds, numEightColGroups, numPartCols, out);
      }
    } catch(StandardException se) {
      throw new IOException(se);
    }
  }

  @Override
  public void fromData(DataInput in) throws IOException, ClassNotFoundException {
    this.baseTableName = DataSerializer.readString(in);
    int numEightColGroups = DataSerializer.readPrimitiveInt(in);
    byte numPartCols = DataSerializer.readPrimitiveByte(in);
    int numRows = DataSerializer.readPrimitiveInt(in);
    this.rows = new ArrayList<DataValueDescriptor[]>();
    for(int i = 0; i < numRows; ++i) {
      DataValueDescriptor[] dvds = new DataValueDescriptor[numEightColGroups * 8 + numPartCols];
      DVDIOUtil.readDVDArray(dvds, in, numEightColGroups, numPartCols);
      this.rows.add(dvds);
    }
  }
}
