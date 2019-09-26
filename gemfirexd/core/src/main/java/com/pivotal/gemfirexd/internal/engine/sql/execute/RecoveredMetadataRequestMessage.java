package com.pivotal.gemfirexd.internal.engine.sql.execute;

import com.gemstone.gemfire.cache.execute.ResultCollector;
import com.gemstone.gemfire.distributed.DistributedMember;
import com.pivotal.gemfirexd.internal.engine.Misc;
import com.pivotal.gemfirexd.internal.engine.distributed.GfxdDistributionAdvisor;
import com.pivotal.gemfirexd.internal.engine.distributed.RecoveryModeResultHolder;
import com.pivotal.gemfirexd.internal.engine.distributed.message.GfxdFunctionMessage;
import com.pivotal.gemfirexd.internal.engine.distributed.message.MemberExecutorMessage;
import com.pivotal.gemfirexd.internal.engine.distributed.message.PersistentStateInRecoveryMode;
import com.pivotal.gemfirexd.internal.engine.distributed.utils.GemFireXDUtils;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;

public class RecoveredMetadataRequestMessage extends MemberExecutorMessage {
  public RecoveredMetadataRequestMessage(
      ResultCollector collector) {
    super(collector, null, false, true);
  }

  private final Double CATALOG_OBJECTS_SUBLIST_SIZE = 25.0;
  private final Double OTHER_DDLS_SUBLIST_SIZE = 100.0;
  private final Double REGION_VIEWS_SUBLIST_SIZE = 30.0;
  /**
   * Default constructor for deserialization. Not to be invoked directly.
   */
  public RecoveredMetadataRequestMessage() {
    super(true);
  }

  @Override
  protected void execute() throws Exception {
    GemFireXDUtils.waitForNodeInitialization();
    sendPersistentStateMsg(Misc.getMemStore().getPersistentStateMsg());
    this.lastResultSent = true;
  }

  @Override
  public Set<DistributedMember> getMembers() {
    final Set<DistributedMember> set = new HashSet<>();
    Set<DistributedMember> members = GemFireXDUtils.getGfxdAdvisor().adviseAllNodes(null);
    members.stream().filter(x -> {
      final GfxdDistributionAdvisor.GfxdProfile other = GemFireXDUtils
          .getGfxdProfile(x);
      return (other.getVMKind().isLocator() || other.getVMKind().isStore());
    }).forEach(x -> set.add(x));
    return set;
  }

  private void sendPersistentStateMsg(PersistentStateInRecoveryMode persistentStateMsg) {
    RecoveryModeResultHolder.PersistentStateInRMMetadata resultHolderMetadata = new RecoveryModeResultHolder.PersistentStateInRMMetadata(persistentStateMsg.getMember(), persistentStateMsg.getPrToNumBuckets(), persistentStateMsg.getReplicatedRegions(), persistentStateMsg.isServer());
    this.sendResult(resultHolderMetadata);
    sendList(persistentStateMsg.getCatalogObjects(), CATALOG_OBJECTS_SUBLIST_SIZE, 1);
    sendList(persistentStateMsg.getOtherDDLs(), OTHER_DDLS_SUBLIST_SIZE, 2);
    sendList(persistentStateMsg.getAllRegionViews(), REGION_VIEWS_SUBLIST_SIZE, 3);
  }

  public <T> void sendList(ArrayList<T> arrayList, Double partSize, int type) {
    int n = arrayList.size();
    for (int i = 0; i < n; i += partSize) {
      ArrayList<T> arr = new ArrayList<>();
      arr.addAll(arrayList.subList(i, Math.min(n, i + partSize.intValue())));

      if (type == 1) {
        sendResult(new RecoveryModeResultHolder.PersistentStateInRMCatalogObjectsList((java.util.ArrayList<java.lang.Object>)arr));
      } else if (type == 2) {
        sendResult(new RecoveryModeResultHolder.PersistentStateInRMOtherDDLsList((ArrayList<String>)arr));
      } else {
        // RegionViews...
        this.lastResult(new RecoveryModeResultHolder.PersistentStateInRMAllRegionViews((ArrayList<PersistentStateInRecoveryMode
            .RecoveryModePersistentView>)arr));
      }
    }
  }

  @Override
  public void postExecutionCallback() {

  }

  @Override
  public byte getGfxdID() {
    return LEAD_DISK_STATE_MSG;
  }

  @Override
  public boolean isHA() {
    return false;
  }

  @Override
  public boolean optimizeForWrite() {
    return false;
  }

  @Override
  protected GfxdFunctionMessage clone() {
    return null;
  }
}
