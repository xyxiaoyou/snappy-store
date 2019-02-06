package com.pivotal.gemfirexd.internal.engine.sql.execute;

import com.gemstone.gemfire.cache.execute.ResultCollector;
import com.gemstone.gemfire.distributed.DistributedMember;
import com.pivotal.gemfirexd.internal.engine.Misc;
import com.pivotal.gemfirexd.internal.engine.distributed.GfxdDistributionAdvisor;
import com.pivotal.gemfirexd.internal.engine.distributed.message.GfxdFunctionMessage;
import com.pivotal.gemfirexd.internal.engine.distributed.message.MemberExecutorMessage;
import com.pivotal.gemfirexd.internal.engine.distributed.utils.GemFireXDUtils;

import java.util.HashSet;
import java.util.Set;

public class RecoveredMetadataRequestMessage extends MemberExecutorMessage {
  public RecoveredMetadataRequestMessage(
      ResultCollector collector) {
    super(collector, null, false, true);
  }

  /**
   * Default constructor for deserialization. Not to be invoked directly.
   */
  public RecoveredMetadataRequestMessage() {
    super(true);
  }

  @Override
  protected void execute() throws Exception {
    GemFireXDUtils.waitForNodeInitialization();
    this.lastResult(Misc.getMemStore().getPersistentStateMsg());
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
