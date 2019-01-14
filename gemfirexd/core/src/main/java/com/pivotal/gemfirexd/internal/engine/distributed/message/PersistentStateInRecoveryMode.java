/*
 * Changes for SnappyData data platform.
 *
 * Portions Copyright (c) 2018 SnappyData, Inc. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You
 * may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License. See accompanying
 * LICENSE file.
 */
package com.pivotal.gemfirexd.internal.engine.distributed.message;

import com.gemstone.gemfire.DataSerializable;
import com.gemstone.gemfire.DataSerializer;
import com.gemstone.gemfire.LogWriter;
import com.gemstone.gemfire.distributed.internal.membership.InternalDistributedMember;
import com.gemstone.gemfire.internal.cache.AbstractDiskRegion;
import com.gemstone.gemfire.internal.cache.DiskStoreImpl;
import com.gemstone.gemfire.internal.cache.GemFireCacheImpl;
import com.gemstone.gemfire.internal.cache.RegionEntry;
import com.gemstone.gemfire.internal.cache.persistence.PRPersistentConfig;
import com.gemstone.gemfire.internal.cache.versions.RegionVersionHolder;
import com.gemstone.gemfire.internal.cache.versions.RegionVersionVector;
import com.pivotal.gemfirexd.internal.engine.GfxdConstants;
import com.pivotal.gemfirexd.internal.engine.Misc;
import com.pivotal.gemfirexd.internal.engine.ddl.DDLConflatable;
import com.pivotal.gemfirexd.internal.engine.distributed.utils.GemFireXDUtils;
import io.snappydata.thrift.CatalogTableObject;
import org.apache.thrift.TException;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.*;

public class PersistentStateInRecoveryMode implements DataSerializable {

  private InternalDistributedMember member = null;
  private final ArrayList<RecoveryModePersistentView>
      allRegionView = new ArrayList<>();
  private final ArrayList<CatalogTableObject> catalogObjects = new ArrayList<>();
  private ArrayList<String> otherExtractedDDLText = new ArrayList<>();
  private final HashMap<String, Integer> prToNumBuckets = new HashMap<>();

  private boolean isServer;

  public PersistentStateInRecoveryMode(
      List<CatalogTableObject> allEntries,
      List<DDLConflatable> extractedDDLs) {
    member = Misc.getMyId();
    if (allEntries != null && !allEntries.isEmpty()) {
      catalogObjects.addAll(allEntries);
    }
    if (extractedDDLs != null && !extractedDDLs.isEmpty()) {
      extractedDDLs.forEach( x -> {
        otherExtractedDDLText.add(x.getValueToConflate());
      });
    }
    this.isServer = Misc.getMemStore().getMyVMKind().isStore();
  }

  public PersistentStateInRecoveryMode() {

  }

  public void addView(RecoveryModePersistentView v) {
    this.allRegionView.add(v);
  }

  public void addPRConfigs() {
    GemFireCacheImpl cache = Misc.getGemFireCache();
    Collection<DiskStoreImpl> diskStores = cache.listDiskStores();
    for(DiskStoreImpl ds : diskStores) {
      String dsName = ds.getName();
      if (!(dsName.equals(GfxdConstants.GFXD_DD_DISKSTORE_NAME) ||
         dsName.equals(GfxdConstants.SNAPPY_DEFAULT_DELTA_DISKSTORE) ||
         dsName.endsWith(GfxdConstants.SNAPPY_DELTA_DISKSTORE_SUFFIX))) {
        Map<String, PRPersistentConfig> prConfigs = ds.getDiskInitFile().getAllPRs();
        for (Map.Entry<String, PRPersistentConfig> e : prConfigs.entrySet()) {
          this.prToNumBuckets.put(e.getKey(), e.getValue().getTotalNumBuckets());
        }
      }
    }
  }

  public ArrayList<RecoveryModePersistentView> getAllRegionViews() {
    return this.allRegionView;
  }

  public InternalDistributedMember getMember() {
    return this.member;
  }

  public ArrayList<String> getOtherDDLs() {
    return this.otherExtractedDDLText;
  }

  public ArrayList<CatalogTableObject> getCatalogObjects() {
    return this.catalogObjects;
  }

  public HashMap<String, Integer> getPrToNumBuckets() {
    return this.prToNumBuckets;
  }

  @Override
  public void fromData(DataInput in) throws IOException, ClassNotFoundException {
    this.member = DataSerializer.readObject(in);
    this.isServer = DataSerializer.readBoolean(in);
    this.allRegionView.addAll(DataSerializer.readArrayList(in));
    this.otherExtractedDDLText.addAll(DataSerializer.readArrayList(in));
    this.prToNumBuckets.putAll(DataSerializer.readHashMap(in));
    int numObjects = in.readInt();
    try {
      for (int i = 0; i < numObjects; i++) {
        byte[] b = DataSerializer.readByteArray(in);
        CatalogTableObject obj = new CatalogTableObject();
        GemFireXDUtils.readThriftObject(obj, b);
        this.catalogObjects.add(obj);
      }
    } catch(TException e) {
      throw new IOException(e);
    }
    this.allRegionView.forEach(x -> x.member = this.member);
  }

  @Override
  public void toData(final DataOutput out) throws IOException {
    DataSerializer.writeObject(this.member, out);
    DataSerializer.writeBoolean(this.isServer, out);
    DataSerializer.writeArrayList(this.allRegionView, out);
    DataSerializer.writeArrayList(this.otherExtractedDDLText, out);
    DataSerializer.writeHashMap(this.prToNumBuckets, out);
    int numCatalogObjects = this.catalogObjects.size();
    out.writeInt(numCatalogObjects);
    try {
      for (int i = 0; i < numCatalogObjects; i++) {
        byte[] b = GemFireXDUtils.writeThriftObject(this.catalogObjects.get(i));
        DataSerializer.writeByteArray(b, out);
      }
    } catch (TException t) {
      throw new IOException(t);
    }
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder();
    sb.append("member: ");
    sb.append(this.member);
    sb.append("\n");
    sb.append("RecoveryModePersistentView objects");
    for (RecoveryModePersistentView e : this.allRegionView) {
      sb.append(e);
      sb.append("\n");
    }
    sb.append("Catalog Objects\n");
    for (CatalogTableObject obj : this.catalogObjects) {
      sb.append(obj);
      sb.append("\n");
    }
    sb.append("Other Extracted ddls\n");
    for (String ddl : this.otherExtractedDDLText) {
      sb.append(ddl);
      sb.append("\n");
    }
    return sb.toString();
  }

  public static long getLatestModifiedTime(AbstractDiskRegion adr, LogWriter logger) {
    if (logger.infoEnabled()) {
      logger.info("getLatestModifiedTime: map = " + adr.getRecoveredEntryMap());
    }
    if (logger.infoEnabled()) {
      logger.info("getLatestModifiedTime: map entries = " + adr.getRecoveredEntryMap().regionEntries());
    }
    Optional<RegionEntry> rmax = adr.getRecoveredEntryMap()
        .regionEntries().stream().max((t1, t2) -> {
          if (t1.getLastModified() <= t2.getLastModified()) return -1;
          return 1;
        });
    return rmax.get().getLastModified();
  }

  public static class RecoveryModePersistentView
      implements Comparable<RecoveryModePersistentView>, DataSerializable {

    private String regionPath;
    private String diskStoreName;
    private transient RegionVersionVector rvv;
    private long mostRecentEntryModifiedTime;
    private long latestOplogTime;
    private transient InternalDistributedMember member;

    public RecoveryModePersistentView(
        final String diskStoreName, final String regionFullPath,
        final RegionVersionVector regionVersionVector,
        long recentModifiedTime, long latestOplogTime) {
      this.regionPath = regionFullPath;
      this.diskStoreName = diskStoreName;
      this.rvv = regionVersionVector.getCloneForTransmission();
      this.mostRecentEntryModifiedTime = recentModifiedTime;
      this.latestOplogTime = latestOplogTime;
    }

    public RecoveryModePersistentView() {

    }

    public String getRegionPath() {
      return this.regionPath;
    }

    public InternalDistributedMember getMember() {
      return this.member;
    }

    public String getExecutorHost() {
      return this.member.canonicalString();
    }

    public String getDiskStoreName() {
      return this.diskStoreName;
    }

    @Override
    public void toData(DataOutput out) throws IOException {
      out.writeUTF(regionPath);
      out.writeUTF(diskStoreName);
      DataSerializer.writeObject(this.rvv, out);
      DataSerializer.writeLong(mostRecentEntryModifiedTime, out);
      DataSerializer.writeLong(latestOplogTime, out);

    }

    @Override
    public void fromData(DataInput in) throws IOException, ClassNotFoundException {
      this.regionPath = in.readUTF();
      this.diskStoreName = in.readUTF();
      this.rvv = DataSerializer.readObject(in);
      this.mostRecentEntryModifiedTime = DataSerializer.readLong(in);
      this.latestOplogTime = DataSerializer.readLong(in);
    }

    @Override
    public String toString() {
      StringBuilder sb = new StringBuilder();
      sb.append("RecoveryModePersistentView member: ");
      sb.append(this.member);
      sb.append(": region: ");
      sb.append(this.regionPath);
      return sb.toString();
    }

    // TODO: KN put logs and comments
    @Override
    public int compareTo(RecoveryModePersistentView other) {
      // They should be called for the same region
      assert this.regionPath.equals(other.regionPath);
      if (this.rvv.sameAs(other.rvv)) {
        if (this.latestOplogTime <= other.latestOplogTime) {
          return -1;
        }
        if (this.mostRecentEntryModifiedTime
            <= other.mostRecentEntryModifiedTime) {
          return -1;
        }
        return 1;
      } else {
        Map<?, RegionVersionHolder<?>> versionHolderOne
            = this.rvv.getMemberToVersion();
        Map<?, RegionVersionHolder<?>> versionHolderTwo
            = this.rvv.getMemberToVersion();
        if (versionHolderOne.keySet().equals(versionHolderTwo.keySet())) {
          for(Map.Entry<?, RegionVersionHolder<?>> e : versionHolderOne.entrySet()){
            RegionVersionHolder rvh1 = e.getValue();
            RegionVersionHolder rvh2 = versionHolderTwo.get(e.getKey());
            if (rvh2.dominates(rvh1)) {
              return -1;
            } else if (rvh1.dominates(rvh2)) {
              return 1;
            }
            // If no one dominates then let them be equal.
          }
        } else {
          // log a warning and pick the one with more version holder objects
          if (versionHolderOne.size() < versionHolderTwo.size()) {
            return -1;
          } else if (versionHolderTwo.size() < versionHolderOne.size()) {
            return 1;
          } else {
            // equal. Means you can't do much. So let any one get picked up.
          }
        }
      }
      return 0;
    }
  }
}
