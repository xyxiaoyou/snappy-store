package com.pivotal.gemfirexd.internal.engine.distributed;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;

import com.gemstone.gemfire.DataSerializable;
import com.gemstone.gemfire.DataSerializer;
import com.gemstone.gemfire.distributed.internal.membership.InternalDistributedMember;
import com.pivotal.gemfirexd.internal.engine.distributed.message.PersistentStateInRecoveryMode;

public interface RecoveryModeResultHolder {

  public class PersistentStateInRMMetadata implements DataSerializable {

    private final HashMap<String, Integer> prToNumBuckets = new HashMap<>();
    private InternalDistributedMember member = null;
    private HashSet<String> replicatedRegions = new HashSet<String>();
    private boolean isServer;

    public PersistentStateInRMMetadata(InternalDistributedMember member,
        HashMap<String, Integer> prToNumBuckets,
        HashSet replicatedRegions, boolean isServer) {
      this.member = member;
      this.prToNumBuckets.putAll(prToNumBuckets);
      this.replicatedRegions = replicatedRegions;
      this.isServer = isServer;
    }

    public PersistentStateInRMMetadata() {
    }


    @Override
    public void toData(DataOutput out) throws IOException {
      DataSerializer.writeObject(this.member, out);
      DataSerializer.writeHashMap(this.prToNumBuckets, out);
      DataSerializer.writeHashSet(this.replicatedRegions, out);
      DataSerializer.writeBoolean(this.isServer, out);
    }

    public HashMap<String, Integer> getPrToNumBuckets() {
      return this.prToNumBuckets;
    }

    public InternalDistributedMember getMember() {
      return this.member;
    }

    public HashSet<String> getReplicatedRegions() {
      return this.replicatedRegions;
    }

    public boolean isServer() {
      return this.isServer;
    }

    @Override
    public void fromData(DataInput in) throws IOException, ClassNotFoundException {
      this.member = DataSerializer.readObject(in);
      this.prToNumBuckets.putAll(DataSerializer.readHashMap(in));
      this.replicatedRegions = DataSerializer.readHashSet(in);
      this.isServer = DataSerializer.readBoolean(in);
    }

  }

  class PersistentStateInRMAllRegionViews implements DataSerializable {
    private final ArrayList<PersistentStateInRecoveryMode.RecoveryModePersistentView>
        allRegionView = new ArrayList<>();

    public PersistentStateInRMAllRegionViews(ArrayList<PersistentStateInRecoveryMode.RecoveryModePersistentView>
        allRegionView) {
      this.allRegionView.addAll(allRegionView);
    }

    public PersistentStateInRMAllRegionViews() {
    }

    public ArrayList<PersistentStateInRecoveryMode.RecoveryModePersistentView> getAllRegionView() {
      return this.allRegionView;
    }

    @Override
    public void toData(DataOutput out) throws IOException {
      DataSerializer.writeArrayList(this.allRegionView, out);
    }

    @Override
    public void fromData(DataInput in) throws IOException, ClassNotFoundException {
      this.allRegionView.addAll(DataSerializer.readArrayList(in));
    }
  }


  class PersistentStateInRMCatalogObjectsList implements DataSerializable {
    private final ArrayList<Object> catalogObjects = new ArrayList<>();

    public ArrayList<Object> getCatalogObjects() {
      return this.catalogObjects;
    }

    public PersistentStateInRMCatalogObjectsList(ArrayList<Object> catalogObjects) {
      this.catalogObjects.addAll(catalogObjects);
    }

    public PersistentStateInRMCatalogObjectsList() {
    }

    @Override
    public void toData(DataOutput out) throws IOException {
      DataSerializer.writeArrayList(this.catalogObjects, out);
    }

    @Override
    public void fromData(DataInput in) throws IOException, ClassNotFoundException {
      this.catalogObjects.addAll(DataSerializer.readArrayList(in));
    }
  }

  class PersistentStateInRMOtherDDLsList implements DataSerializable {
    private ArrayList<String> otherExtractedDDLText = new ArrayList<>();

    public PersistentStateInRMOtherDDLsList(ArrayList<String> otherExtractedDDLText) {
      this.otherExtractedDDLText.addAll(otherExtractedDDLText);
    }

    public ArrayList<String> getOtherExtractedDDLText() {
      return this.otherExtractedDDLText;
    }

    public PersistentStateInRMOtherDDLsList() {
    }

    @Override
    public void toData(DataOutput out) throws IOException {
      DataSerializer.writeArrayList(this.otherExtractedDDLText, out);
    }

    @Override
    public void fromData(DataInput in) throws IOException, ClassNotFoundException {
      this.otherExtractedDDLText.addAll(DataSerializer.readArrayList(in));
    }
  }

}
