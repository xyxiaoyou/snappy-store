/*
 * Copyright (c) 2010-2015 Pivotal Software, Inc. All rights reserved.
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

package com.gemstone.gemfire.internal.cache;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Set;

import com.gemstone.gemfire.CancelException;
import com.gemstone.gemfire.DataSerializer;
import com.gemstone.gemfire.cache.CacheException;
import com.gemstone.gemfire.cache.CacheWriterException;
import com.gemstone.gemfire.cache.EntryExistsException;
import com.gemstone.gemfire.cache.Operation;
import com.gemstone.gemfire.cache.TransactionException;
import com.gemstone.gemfire.distributed.internal.DM;
import com.gemstone.gemfire.distributed.internal.DirectReplyProcessor;
import com.gemstone.gemfire.distributed.internal.DistributionManager;
import com.gemstone.gemfire.distributed.internal.InternalDistributedSystem;
import com.gemstone.gemfire.distributed.internal.ReplyException;
import com.gemstone.gemfire.distributed.internal.ReplyMessage;
import com.gemstone.gemfire.distributed.internal.ReplyProcessor21;
import com.gemstone.gemfire.distributed.internal.ReplySender;
import com.gemstone.gemfire.distributed.internal.membership.InternalDistributedMember;
import com.gemstone.gemfire.i18n.LogWriterI18n;
import com.gemstone.gemfire.internal.Assert;
import com.gemstone.gemfire.internal.InternalDataSerializer;
import com.gemstone.gemfire.internal.NanoTimer;
import com.gemstone.gemfire.internal.cache.EntryEventImpl.NewValueImporter;
import com.gemstone.gemfire.internal.cache.EntryEventImpl.OldValueImporter;
import com.gemstone.gemfire.internal.cache.tier.sockets.ClientProxyMembershipID;
import com.gemstone.gemfire.internal.cache.versions.DiskVersionTag;
import com.gemstone.gemfire.internal.cache.versions.RegionVersionVector;
import com.gemstone.gemfire.internal.cache.versions.VersionTag;
import com.gemstone.gemfire.internal.i18n.LocalizedStrings;
import com.gemstone.gemfire.internal.offheap.annotations.Unretained;
import com.gemstone.gemfire.internal.shared.Version;
import com.gemstone.gemfire.internal.util.Breadcrumbs;

import static com.gemstone.gemfire.internal.offheap.annotations.OffHeapIdentifier.ENTRY_EVENT_OLD_VALUE;
import static com.gemstone.gemfire.internal.offheap.annotations.OffHeapIdentifier.ENTRY_EVENT_NEW_VALUE;

import static com.gemstone.gemfire.internal.cache.DistributedCacheOperation.VALUE_IS_BYTES;
import static com.gemstone.gemfire.internal.cache.DistributedCacheOperation.VALUE_IS_SERIALIZED_OBJECT;
import static com.gemstone.gemfire.internal.cache.DistributedCacheOperation.VALUE_IS_OBJECT;

/**
 * A Replicate Region update message.  Meant to be sent only to
 * the peer who hosts transactional data.
 *
 * @since 6.5
 */
public final class RemotePutMessage extends
    RemoteOperationMessageWithDirectReply implements NewValueImporter, OldValueImporter {

  /** The key associated with the value that must be sent */
  private Object key;

  /** The value associated with the key that must be sent */
  private byte[] valBytes;

  private byte[] oldValBytes;

  /**
   * Used on sender side only to defer serialization until toData is called.
   */
  @Unretained(ENTRY_EVENT_NEW_VALUE) 
  private transient Object valObj;

  @Unretained(ENTRY_EVENT_OLD_VALUE) 
  private transient Object oldValObj;

  /** The callback arg of the operation */
  private Object cbArg;

  /** The time stamp when the value was created */
  protected long lastModified;

  /** The operation performed on the sender */
  private Operation op;

  /**
   * An additional object providing context for the operation, e.g., for
   * BridgeServer notification
   */
  ClientProxyMembershipID bridgeContext;

  /** event identifier */
  EventID eventId;

  /**
   * for relayed messages, this is the sender of the original message. It should
   * be used in constructing events for listener notification.
   */
  InternalDistributedMember originalSender;

  /**
   * Indicates if and when the new value should be deserialized on the
   * the receiver. Distinguishes between Deltas which need to be eagerly
   * deserialized (DESERIALIZATION_POLICY_EAGER), a non-byte[] value that was
   * serialized (DESERIALIZATION_POLICY_LAZY) and a
   * byte[] array value that didn't need to be serialized
   * (DESERIALIZATION_POLICY_NONE). While this seems like an extra data, it
   * isn't, because serializing a byte[] causes the type (a byte)
   * to be written in the stream, AND what's better is
   * that handling this distinction at this level reduces processing for values
   * that are byte[].
   */
  protected byte deserializationPolicy =
    DistributedCacheOperation.DESERIALIZATION_POLICY_NONE;

  protected byte oldValueIsSerialized;

  /**
   * whether it's okay to create a new key
   */
  private boolean ifNew;

  /**
   * whether it's okay to update an existing key
   */
  private boolean ifOld;

  /**
   * Whether an old value is required in the response
   */
  private boolean requireOldValue;

  /**
   * True if CacheWriter has to be invoked else false. Currently only used by
   * transactional ops.
   */
  private boolean cacheWrite;

  /**
   * For put to happen, the old value must be equal to this expectedOldValue.
   * 
   * @see PartitionedRegion#replace(Object, Object, Object)
   */
  private Object expectedOldValue; // TODO OFFHEAP make it a cd

  private VersionTag<?> versionTag;

  private transient InternalDistributedSystem internalDs;

  /**
   * state from operateOnRegion that must be preserved for transmission from the
   * waiting pool
   */
  transient boolean result = false;

  private boolean hasOldValue = false;

  /** whether new value is formed by applying delta **/
  private transient boolean applyDeltaBytes = false;

  /** delta bytes read in fromData that will be used in operate() */
  private transient byte[] deltaBytes;

  /** whether to send delta or full value **/
  private transient boolean sendDelta = false;

  private EntryEventImpl event = null;

  private boolean useOriginRemote;

  private boolean possibleDuplicate;

  /** true if this is a GFXD "PUT INTO ..." DML */
  private boolean isPutDML;

  // additional bitmask flags used for serialization/deserialization

  protected static final short IF_NEW = UNRESERVED_FLAGS_START;
  protected static final short IF_OLD = (IF_NEW << 1);
  protected static final short REQUIRED_OLD_VAL = (IF_OLD << 1);
  protected static final short HAS_OLD_VAL = (REQUIRED_OLD_VAL << 1);
  protected static final short HAS_DELTA_BYTES = (HAS_OLD_VAL << 1);
  protected static final short USE_ORIGIN_REMOTE = (HAS_DELTA_BYTES << 1);
  protected static final short CACHE_WRITE = (USE_ORIGIN_REMOTE << 1);
  protected static final short HAS_EXPECTED_OLD_VAL = (CACHE_WRITE << 1);
  protected static final short IS_PUT_DML = (HAS_EXPECTED_OLD_VAL << 1);

  // below flags go into deserializationPolicy
  protected static final int HAS_BRIDGE_CONTEXT =
      getNextByteMask(DistributedCacheOperation.DESERIALIZATION_POLICY_END);
  protected static final int HAS_ORIGINAL_SENDER =
      getNextByteMask(HAS_BRIDGE_CONTEXT);
  protected static final int HAS_VERSION_TAG =
      getNextByteMask(HAS_ORIGINAL_SENDER);
  protected static final int HAS_CALLBACKARG = getNextByteMask(HAS_VERSION_TAG);

  /**
   * Empty constructor to satisfy {@link DataSerializer}requirements
   */
  public RemotePutMessage() {
  }

  private RemotePutMessage(InternalDistributedMember recipient,
      LocalRegion r,
      DirectReplyProcessor processor,
      EntryEventImpl event,
      final long lastModified,
      boolean ifNew,
      boolean ifOld,
      Object expectedOldValue,
      boolean requireOldValue,
      boolean cacheWrite,
      boolean useOriginRemote,
      boolean possibleDuplicate) {
    super(recipient, r, processor, event.getTXState(r));
    initialize(event, lastModified, ifNew, ifOld, expectedOldValue,
        requireOldValue, cacheWrite, useOriginRemote, possibleDuplicate);
  }

  private RemotePutMessage(Set<?> recipients,
                     LocalRegion r,
                     DirectReplyProcessor processor,
                     EntryEventImpl event,
                     final long lastModified,
                     boolean ifNew,
                     boolean ifOld,
                     Object expectedOldValue,
                     boolean requireOldValue,
                     boolean cacheWrite,
                     boolean useOriginRemote,
                     boolean possibleDuplicate) {
    super(recipients, r, processor, event.getTXState(r));
    initialize(event, lastModified, ifNew, ifOld, expectedOldValue,
        requireOldValue, cacheWrite, useOriginRemote, possibleDuplicate);
  }

  private void initialize(EntryEventImpl event, final long lastModified,
      boolean ifNew, boolean ifOld, Object expectedOldValue,
      boolean requireOldValue, boolean cacheWrite, boolean useOriginRemote,
      boolean possibleDuplicate) {
    this.requireOldValue = requireOldValue;
    this.expectedOldValue = expectedOldValue;
    this.useOriginRemote = useOriginRemote;
    this.key = event.getKey();
    this.possibleDuplicate = possibleDuplicate;

    // TX will use coordinator to determine originRemote internally
    event.setOriginRemote(useOriginRemote);

    if (event.hasNewValue()) {
      if (CachedDeserializableFactory.preferObject() || event.hasDelta()) {
        this.deserializationPolicy = DistributedCacheOperation.DESERIALIZATION_POLICY_EAGER;
      } else {
        this.deserializationPolicy = DistributedCacheOperation.DESERIALIZATION_POLICY_LAZY;
      }
      event.exportNewValue(this);
    }
    else {
      // assert that if !event.hasNewValue, then deserialization policy is NONE
      assert this.deserializationPolicy ==
        DistributedCacheOperation.DESERIALIZATION_POLICY_NONE :
        this.deserializationPolicy;
    }

    // added for cqs on Bridge Servers. rdubey

    if (event.hasOldValue()) {
      this.hasOldValue = true;
      event.exportOldValue(this);
    }

    this.event = event;
  
    this.cbArg = event.getRawCallbackArgument();
    this.lastModified = lastModified;
    this.op = event.getOperation();
    this.bridgeContext = event.getContext();
    this.eventId = event.getEventId();
    Assert.assertTrue(this.eventId != null);
    this.ifNew = ifNew;
    this.ifOld = ifOld;
    this.cacheWrite = cacheWrite;
    this.versionTag = event.getVersionTag();
    this.isPutDML = event.isPutDML();
  }

  /**
   * this is similar to send() but it selects an initialized replicate
   * that is used to proxy the message
   * 
   * @param event represents the current operation
   * @param lastModified lastModified time
   * @param ifNew whether a new entry can be created
   * @param ifOld whether an old entry can be used (updates are okay)
   * @param expectedOldValue the value being overwritten is required to match this value
   * @param requireOldValue whether the old value should be returned
   * @param onlyPersistent send message to persistent members only
   * @return whether the message was successfully distributed to another member
   */
  @SuppressWarnings({ "rawtypes", "unchecked" })
  public static boolean distribute(EntryEventImpl event, long lastModified,
      boolean ifNew, boolean ifOld, Object expectedOldValue,
      boolean requireOldValue, boolean onlyPersistent) {
    boolean successful = false;
    final DistributedRegion r = (DistributedRegion)event.getRegion();
    Collection<InternalDistributedMember> replicates = onlyPersistent ? r
        .getCacheDistributionAdvisor().adviseInitializedPersistentMembers()
        .keySet() : r.getCacheDistributionAdvisor()
        .adviseInitializedReplicates();
    if (replicates.isEmpty()) {
      return false;
    }
    LogWriterI18n log = r.getLogWriterI18n();
    if (replicates.size() > 1) {
      ArrayList<InternalDistributedMember> l =
          new ArrayList<InternalDistributedMember>(replicates);
      Collections.shuffle(l);
      replicates = l;
    }
    int attempts = 0;
    if (log.fineEnabled()) {
      log.fine("performing remote put messaging for " + event);
    }
    for (InternalDistributedMember replicate : replicates) {
      try {
        attempts++;
        final boolean posDup = (attempts > 1);
        RemotePutResponse response = send(replicate, r, event, lastModified,
            ifNew, ifOld, expectedOldValue, requireOldValue, false, false,
            posDup);
        PutResult result = response.waitForResult();
        event.setOldValue(result.oldValue, true/*force*/);
        event.setOperation(result.op);
        if (result.versionTag != null) {
          event.setVersionTag(result.versionTag);
          final RegionVersionVector rvv = r.getVersionVector();
          if (rvv != null) {
            rvv.recordVersion(result.versionTag.getMemberID(),
                result.versionTag, event);
          }
        }
        event.setInhibitDistribution(true);
        return true;

      } catch (TransactionException te) {
        throw te;
      } catch (CancelException e) {
        r.getCancelCriterion().checkCancelInProgress(e);

      } catch (CacheException e) {
        if (log.fineEnabled()) {
          log.fine(
              "RemotePutMessage caught CacheException during distribution", e);
        }
        successful = true; // not a cancel-exception, so don't complain any more
                           // about it

      } catch(RemoteOperationException e) {
        if (DistributionManager.VERBOSE || log.fineEnabled()) {
          log.info(LocalizedStrings.DEBUG, "RemotePutMessage caught an "
              + "unexpected exception during distribution", e);
        }
      }
    }
    return successful;
  }

  @Override
  public boolean isSevereAlertCompatible() {
    // allow forced-disconnect processing for all cache op messages
    return true;
  }

  /**
   * Sends a ReplicateRegion
   * {@link com.gemstone.gemfire.cache.Region#put(Object, Object)} message to
   * the recipient
   * @param recipient the member to which the put message is sent
   * @param r  the PartitionedRegion for which the put was performed
   * @param event the event prompting this message
   * @param ifNew whether a new entry must be created
   * @param ifOld whether an old entry must be updated (no creates)
   * @return the processor used to await acknowledgement that the update was
   *         sent, or null to indicate that no acknowledgement will be sent
   * @throws RemoteOperationException if the peer is no longer available
   */
  public static RemotePutResponse send(InternalDistributedMember recipient,
                                       LocalRegion r,
                                       EntryEventImpl event,
                                       final long lastModified,
                                       boolean ifNew,
                                       boolean ifOld,
                                       Object expectedOldValue,
                                       boolean requireOldValue,
                                       boolean cacheWrite,
                                       boolean useOriginRemote,
                                       boolean possibleDuplicate)
  throws RemoteOperationException {

    final RemotePutMessage m = prepareSend(r.getSystem(), recipient, r, event,
        lastModified, ifNew, ifOld, expectedOldValue, requireOldValue,
        cacheWrite, useOriginRemote, possibleDuplicate);
    final Set<?> failures = r.getDistributionManager().putOutgoing(m);
    if (failures != null && failures.size() > 0) {
      throw new RemoteOperationException(
          LocalizedStrings.RemotePutMessage_FAILED_SENDING_0
              .toLocalizedString(m));
    }
    return (RemotePutResponse)m.processor;
  }

  /**
   * Prepares a {@link RemotePutMessage} for sending to the given recipient but
   * does not actually send using {@link DistributionManager}.
   * 
   * @param sys
   *          the distributed system
   * @param recipient
   *          the member to which the put message is sent
   * @param r
   *          the LocalRegion for which the put was performed
   * @param event
   *          the event prompting this message
   * @param ifNew
   *          whether a new entry must be created
   * @param ifOld
   *          whether an old entry must be updated (no creates)
   * 
   * @return the RemotePutMessage that was prepared for sending to the recipient
   */
  public static RemotePutMessage prepareSend(
      final InternalDistributedSystem sys,
      final InternalDistributedMember recipient, LocalRegion r,
      EntryEventImpl event, final long lastModified, boolean ifNew,
      boolean ifOld, Object expectedOldValue, boolean requireOldValue,
      boolean cacheWrite, boolean useOriginRemote, boolean possibleDuplicate) {
    // recipient can be null for event notifications
    //Assert.assertTrue(recipient != null, "RemotePutMessage NULL recipient");

    final RemotePutResponse processor = new RemotePutResponse(sys,
        recipient, event.getKey(), false);
    final RemotePutMessage m = new RemotePutMessage(recipient,
                                  r,
                                  processor,
                                  event,
                                  lastModified,
                                  ifNew,
                                  ifOld,
                                  expectedOldValue,
                                  requireOldValue,
                                  cacheWrite,
                                  useOriginRemote,
                                  possibleDuplicate);
    m.setInternalDs(sys);
    m.setSendDelta(true);

    processor.setRemotePutMessage(m);
    return m;
  }

  /**
   * Sends a ReplicateRegion
   * {@link com.gemstone.gemfire.cache.Region#put(Object, Object)} message to
   * the given recipients
   * @param recipients the members to which the put message is sent
   * @param r  the PartitionedRegion for which the put was performed
   * @param event the event prompting this message
   * @param ifNew whether a new entry must be created
   * @param ifOld whether an old entry must be updated (no creates)
   * @return the processor used to await acknowledgement that the update was
   *         sent, or null to indicate that no acknowledgement will be sent
   * @throws RemoteOperationException if the peer is no longer available
   */
  public static RemotePutResponse send(Set<?> recipients,
                                       LocalRegion r,
                                       EntryEventImpl event,
                                       final long lastModified,
                                       boolean ifNew,
                                       boolean ifOld,
                                       Object expectedOldValue,
                                       boolean requireOldValue,
                                       boolean cacheWrite,
                                       boolean useOriginRemote,
                                       boolean possibleDuplicate)
  throws RemoteOperationException {
    final RemotePutResponse processor = new RemotePutResponse(r.getSystem(),
        recipients, event.getKey(), false);
    final RemotePutMessage m = new RemotePutMessage(recipients,
                                  r,
                                  processor,
                                  event,
                                  lastModified,
                                  ifNew,
                                  ifOld,
                                  expectedOldValue,
                                  requireOldValue,
                                  cacheWrite,
                                  useOriginRemote,
                                  possibleDuplicate);
    m.setInternalDs(r.getSystem());
    m.setSendDelta(true);

    processor.setRemotePutMessage(m);

    final Set<?> failures = r.getDistributionManager().putOutgoing(m);
    if (failures != null && failures.size() > 0) {
      throw new RemoteOperationException(LocalizedStrings
          .RemotePutMessage_FAILED_SENDING_0.toLocalizedString(m));
    }
    return processor;
  }

  //  public final boolean needsDirectAck()
  //  {
  //    return this.directAck;
  //  }

//  final public int getProcessorType() {
//    return DistributionManager.PARTITIONED_REGION_EXECUTOR;
//  }



  public final Object getKey()
  {
    return this.key;
  }

  public final void setKey(Object key)
  {
    this.key = key;
  }

  public final byte[] getValBytes()
  {
    return this.valBytes;
  }

  public final byte[] getOldValueBytes() {
    return this.oldValBytes;
  }

  private void setValBytes(byte[] valBytes) {
    this.valBytes = valBytes;
  }

  private void setOldValBytes(byte[] valBytes) {
    this.oldValBytes = valBytes;
  }

  private Object getOldValObj(){
    return this.oldValObj;
  }

  private void setValObj(@Unretained(ENTRY_EVENT_NEW_VALUE) Object o) {
    this.valObj = o;
  }

  private void setOldValObj(@Unretained(ENTRY_EVENT_OLD_VALUE) Object o){
    this.oldValObj = o;
  }

  public final Object getCallbackArg() {
    return this.cbArg;
  }

  protected final Operation getOperation()
  {
    return this.op;
  }

  @Override
  public final void setOperation(Operation operation) {
    this.op = operation;
  }

  /**
   * sets the instance variable hasOldValue to the giving boolean value.
   */
  @Override
  public void setHasOldValue(final boolean value){
    this.hasOldValue = value;
  }

  public int getDSFID() {
    return R_PUT_MESSAGE;
  }

  @Override
  public void fromData(DataInput in)
      throws IOException, ClassNotFoundException {
    super.fromData(in);
    final Version version = InternalDataSerializer.getVersionForDataStream(in);
    final boolean isVer11 = (Version.SQLF_11.compareTo(version) <= 0);

    setKey(DataSerializer.readObject(in));
    final int extraFlags = in.readUnsignedByte();
    this.deserializationPolicy = (byte)(extraFlags
        & DistributedCacheOperation.DESERIALIZATION_POLICY_MASK);
    if (!isVer11) {
      this.cbArg = DataSerializer.readObject(in);
    }
    this.lastModified = in.readLong();
    this.op = Operation.fromOrdinal(in.readByte());
    if (isVer11 && (extraFlags & HAS_CALLBACKARG) != 0) {
      this.cbArg = DataSerializer.readObject(in);
    }
    if ((extraFlags & HAS_BRIDGE_CONTEXT) != 0) {
      this.bridgeContext = DataSerializer.readObject(in);
    }
    if ((extraFlags & HAS_ORIGINAL_SENDER) != 0) {
      this.originalSender = DataSerializer.readObject(in);
    }
    this.eventId = new EventID();
    InternalDataSerializer.invokeFromData(this.eventId, in);

    final short flags = this.flags;
    this.ifNew = (flags & IF_NEW) != 0;
    this.ifOld = (flags & IF_OLD) != 0;
    this.requireOldValue = (flags & REQUIRED_OLD_VAL) != 0;
    this.hasOldValue = (flags & HAS_OLD_VAL) != 0;
    this.cacheWrite = (flags & CACHE_WRITE) != 0;
    this.useOriginRemote = (flags & USE_ORIGIN_REMOTE) != 0;
    this.isPutDML = (flags & IS_PUT_DML) != 0;

    if ((flags & HAS_EXPECTED_OLD_VAL) != 0) {
      this.expectedOldValue = DataSerializer.readObject(in);
    }

    if (this.hasOldValue) {
      this.oldValueIsSerialized = in.readByte();
      if (this.oldValueIsSerialized == VALUE_IS_OBJECT) {
        setOldValObj(DataSerializer.readObject(in));
      }
      else {
        setOldValBytes(DataSerializer.readByteArray(in));
      }
    }
    if (this.deserializationPolicy ==
        DistributedCacheOperation.DESERIALIZATION_POLICY_EAGER) {
      setValObj(DataSerializer.readObject(in));
    }
    else {
      setValBytes(DataSerializer.readByteArray(in));
    }
    if ((flags & HAS_DELTA_BYTES) != 0) {
      this.applyDeltaBytes = true;
      this.deltaBytes = DataSerializer.readByteArray(in);
    }
    if ((extraFlags & HAS_VERSION_TAG) != 0) {
      this.versionTag = DataSerializer.readObject(in);
    }
  }

  @Override
  public EventID getEventID() {
    return this.eventId;
  }

  @Override
  public final void toData(DataOutput out) throws IOException
  {
    super.toData(out);
    final Version version = InternalDataSerializer.getVersionForDataStream(out);
    final boolean isVer11 = (Version.SQLF_11.compareTo(version) <= 0);

    Object key = getKey();
    if (key instanceof KeyWithRegionContext) {
      if (this.valBytes != null || (this.valObj != null && !(this.valObj
              instanceof com.gemstone.gemfire.internal.cache.delta.Delta))) {
        key = ((KeyWithRegionContext)key).beforeSerializationWithValue(false);
      }
    }
    DataSerializer.writeObject(key, out);

    int extraFlags = this.deserializationPolicy;
    if (this.bridgeContext != null) extraFlags |= HAS_BRIDGE_CONTEXT;
    if (this.originalSender != null) extraFlags |= HAS_ORIGINAL_SENDER;
    if (this.versionTag != null) extraFlags |= HAS_VERSION_TAG;

    // with >= GFXD1.1, we use a flag for callback arg to write only if it is
    // not null
    final Object cbArg = getCallbackArg();
    final boolean writeCbArg = (isVer11 && cbArg != null);
    if (writeCbArg) extraFlags |= HAS_CALLBACKARG;
    out.writeByte(extraFlags);

    if (!isVer11) {
      DataSerializer.writeObject(cbArg, out);
    }
    out.writeLong(this.lastModified);
    out.writeByte(this.op.ordinal);
    if (writeCbArg) {
      DataSerializer.writeObject(cbArg, out);
    }
    if (this.bridgeContext != null) {
      DataSerializer.writeObject(this.bridgeContext, out);
    }
    if (this.originalSender != null) {
      DataSerializer.writeObject(this.originalSender, out);
    }
    InternalDataSerializer.invokeToData(this.eventId, out);

    if (this.expectedOldValue != null) {
      DataSerializer.writeObject(this.expectedOldValue, out);
    }
    // this will be on wire for cqs old value generations.
    if (this.hasOldValue) {
      out.writeByte(this.oldValueIsSerialized);
      byte policy = DistributedCacheOperation.valueIsToDeserializationPolicy(oldValueIsSerialized);
      DistributedCacheOperation.writeValue(policy, getOldValObj(), getOldValueBytes(), out);
    }
    DistributedCacheOperation.writeValue(this.deserializationPolicy, this.valObj, getValBytes(), out);
    if (this.event.getDeltaBytes() != null) {
      DataSerializer.writeByteArray(this.event.getDeltaBytes(), out);
    }
    if (this.versionTag != null) {
      DataSerializer.writeObject(this.versionTag, out);
    }
  }

  @Override
  protected short computeCompressedShort(short s) {
    if (this.ifNew) s |= IF_NEW;
    if (this.ifOld) s |= IF_OLD;
    if (this.requireOldValue) s |= REQUIRED_OLD_VAL;
    if (this.hasOldValue) s |= HAS_OLD_VAL;
    if (this.cacheWrite) s |= CACHE_WRITE;
    if (this.event.getDeltaBytes() != null) s |= HAS_DELTA_BYTES;
    if (this.expectedOldValue != null) s |= HAS_EXPECTED_OLD_VAL;
    if (this.useOriginRemote) s |= USE_ORIGIN_REMOTE;
    if (this.possibleDuplicate) s |= POS_DUP;
    if (this.isPutDML) s |= IS_PUT_DML;
    return s;
  }

  /**
   * This method is called upon receipt and make the desired changes to the
   * Replicate Region. Note: It is very important that this message does NOT
   * cause any deadlocks as the sender will wait indefinitely for the
   * acknowledgement
   */
  @Override
  protected final boolean operateOnRegion(DistributionManager dm,
                                                     LocalRegion r,
                                                     long startTime)
  throws EntryExistsException, RemoteOperationException {
//    final LogWriterI18n l = r.getCache().getLoggerI18n();
//    if (DistributionManager.VERBOSE) {
//      l.fine("RemotePutMessage operateOnRegion: "
//          + r.getFullPath());
//    }
    this.setInternalDs(r.getSystem());// set the internal DS. Required to
                                      // checked DS level delta-enabled property
                                      // while sending delta
    final boolean sendReply = true;

    if (r.isCacheContentProxy()) {
      return sendReply;
    }

    InternalDistributedMember eventSender = originalSender;
    if (eventSender == null) {
       eventSender = getSender();
    }
    if (r.keyRequiresRegionContext()) {
      final KeyWithRegionContext key = (KeyWithRegionContext)this.key;
      if (this.valBytes != null) {
        key.afterDeserializationWithValue(this.valBytes);
      }
      else if (this.valObj != null && !(this.valObj
          instanceof com.gemstone.gemfire.internal.cache.delta.Delta)) {
        key.afterDeserializationWithValue(this.valObj);
      }
      key.setRegionContext(r);
    }
    this.event = EntryEventImpl.create(
        r,
        getOperation(),
        getKey(),
        null, /*newValue*/
        getCallbackArg(),
        useOriginRemote, /*originRemote - false to force distribution in buckets*/
        eventSender,
        true/*generateCallbacks*/,
        false/*initializeId*/);
    final EntryEventImpl event = this.event;
    try {
    if (this.versionTag != null) {
      this.versionTag.replaceNullIDs(getSender());
      event.setVersionTag(this.versionTag);
    }
    event.setCausedByMessage(this);

    event.setPossibleDuplicate(this.possibleDuplicate);
    if (this.bridgeContext != null) {
      event.setContext(this.bridgeContext);
    }

    Assert.assertTrue(eventId != null);
    event.setEventId(eventId);
    event.setLockingPolicy(getLockingPolicy());
    event.setPutDML(this.isPutDML);

    //added for cq procesing
    if (this.hasOldValue) {
      if (this.oldValueIsSerialized == VALUE_IS_SERIALIZED_OBJECT) {
        event.setSerializedOldValue(getOldValueBytes());
      }
      else if (this.oldValueIsSerialized == VALUE_IS_BYTES) {
        event.setOldValue(getOldValueBytes());
      }
      else {
        event.setOldValue(getOldValObj());
      }
    }

    if (this.applyDeltaBytes) {
      event.setNewValue(this.valObj);
      event.setDeltaBytes(this.deltaBytes);
    }
    else {
      switch (this.deserializationPolicy) {
        case DistributedCacheOperation.DESERIALIZATION_POLICY_LAZY:
          event.setSerializedNewValue(getValBytes());
          break;
        case DistributedCacheOperation.DESERIALIZATION_POLICY_NONE:
          event.setNewValue(getValBytes());
          break;
        case DistributedCacheOperation.DESERIALIZATION_POLICY_EAGER:
          // new value is a Delta
          event.setNewValue(this.valObj); // sets the delta field
          break;
        default:
          throw new AssertionError("unknown deserialization policy: "
              + deserializationPolicy);
      }
    }

    final TXStateInterface tx = getTXState(r);
    event.setTXState(tx);
    try {
      this.result = AbstractUpdateOperation.doPutOrCreate(r, event,
          this.lastModified, tx == null, this.cacheWrite);
      /*
        // the event must show it's true origin for cachewriter invocation
//        event.setOriginRemote(true);
//        this.op = r.doCacheWriteBeforePut(event, ifNew);  // fix this for bug 37072
        result = r.getSharedDataView().putEntry(event, this.ifNew, this.ifOld,
            this.expectedOldValue, this.requireOldValue, this.cacheWrite,
            this.lastModified, true);

        if (!this.result) { // make sure the region hasn't gone away
          r.checkReadiness();
          if (!this.ifNew && !this.ifOld) {
            // no reason to be throwing an exception, so let's retry
            RemoteOperationException fre = new RemoteOperationException(LocalizedStrings
                .RemotePutMessage_UNABLE_TO_PERFORM_PUT_BUT_OPERATION_SHOULD_NOT_FAIL_0
                    .toLocalizedString());
            fre.setHash(key.hashCode());
            sendReply(getSender(), getProcessorId(), dm,
                new ReplyException(fre), r, startTime);
          }
        }
      */
    } catch (CacheWriterException cwe) {
      sendReply(getSender(), getProcessorId(), dm, new ReplyException(cwe), r,
          startTime);
      return false;
    } catch (PrimaryBucketException pbe) {
      sendReply(getSender(), getProcessorId(), dm, new ReplyException(pbe), r,
          startTime);
      return false;
    }

    setOperation(event.getOperation()); // set operation for reply message

    if (sendReply) {
      sendReply(getSender(),
                getProcessorId(),
                dm,
                null,
                r,
                startTime,
                event);
    }
    return false;
    } finally {
      event.release(); // OFFHEAP this may be too soon to make this call
    }
  }

  protected void sendReply(InternalDistributedMember member,
                           int procId,
                           DM dm,
                           ReplyException ex,
                           LocalRegion pr,
                           long startTime,
                           EntryEventImpl event) {
    PutReplyMessage.send(member, procId, getReplySender(dm), result,
        getOperation(), ex, this, event);
  }

  // override reply message type from PartitionMessage
  @Override
  protected void sendReply(InternalDistributedMember member, int procId, DM dm,
      ReplyException ex, LocalRegion pr, long startTime) {
    PutReplyMessage.send(member, procId, getReplySender(dm), result,
        getOperation(), ex, this, null);
  }

  @Override
  protected final void appendFields(StringBuilder buff) {
    super.appendFields(buff);
    buff.append("; key=").append(getKey())
        .append("; value=");
//    buff.append(getValBytes());
    buff.append(getValBytes() == null ? this.valObj : "("
        + getValBytes().length + " bytes)");
    buff.append("; callback=").append(this.cbArg).append("; op=")
        .append(this.op);
    if (this.originalSender != null) {
      buff.append("; originalSender=").append(originalSender);
    }
    if (this.bridgeContext != null) {
      buff.append("; bridgeContext=").append(this.bridgeContext);
    }
    if (this.eventId != null) {
      buff.append("; eventId=").append(this.eventId);
    }
    buff.append("; ifOld=")
        .append(this.ifOld)
        .append("; ifNew=")
        .append(this.ifNew)
        .append("; op=")
        .append(this.getOperation());
    buff.append("; hadOldValue=").append(this.hasOldValue);
    if(this.hasOldValue){
      byte[] ov = getOldValueBytes();
      if (ov != null) {
        buff.append("; oldValueLength=").append(ov.length);
      }
    }
    buff.append("; deserializationPolicy=");
    buff.append(DistributedCacheOperation
        .deserializationPolicyToString(this.deserializationPolicy));
    buff.append("; sendDelta=");
    buff.append(this.sendDelta);
    buff.append("; isDeltaApplied=");
    buff.append(this.applyDeltaBytes);
    buff.append("; cacheWrite=");
    buff.append(this.cacheWrite);
  }

  public final InternalDistributedSystem getInternalDs()
  {
    return internalDs;
  }

  public final void setInternalDs(InternalDistributedSystem internalDs)
  {
    this.internalDs = internalDs;
  }

  public static final class PutReplyMessage extends ReplyMessage implements OldValueImporter {

    static final byte FLAG_RESULT = 0x01;
    static final byte FLAG_HASVERSION = 0x02;
    static final byte FLAG_PERSISTENT = 0x04;

    /** Result of the Put operation */
    boolean result;

    /** The Operation actually performed */
    Operation op;

    /**
     * Set to true by the import methods if the oldValue
     * is already serialized. In that case toData
     * should just copy the bytes to the stream.
     * In either case fromData just calls readObject.
     */
    private transient boolean oldValueIsSerialized;
    
    /**
     * Old value in serialized form: either a byte[] or CachedDeserializable,
     * or null if not set.
     */
    @Unretained(ENTRY_EVENT_OLD_VALUE)
    Object oldValue;

    /**
     * version tag for concurrency control
     */
    VersionTag<?> versionTag;

    @Override
    protected boolean getMessageInlineProcess() {
      return true;
    }

    /**
     * Empty constructor to conform to DataSerializable interface
     */
    public PutReplyMessage() {
    }

    private PutReplyMessage(int processorId,
                            boolean result,
                            Operation op,
                            ReplyException ex,
                            Object oldValue,
                            RemotePutMessage sourceMessage,
                            VersionTag<?> versionTag) {
      super(sourceMessage, true, true);
      this.op = op;
      this.result = result;
      setProcessorId(processorId);
      setException(ex);
      this.oldValue = oldValue;
      this.versionTag = versionTag;
    }

    /** Send an ack */
    public static void send(InternalDistributedMember recipient,
                            int processorId,
                            ReplySender dm,
                            boolean result,
                            Operation op,
                            ReplyException ex,
                            RemotePutMessage sourceMessage,
                            EntryEventImpl event)
    {
      Assert.assertTrue(recipient != null, "PutReplyMessage NULL recipient");
      PutReplyMessage m = new PutReplyMessage(processorId, result, op, ex,
          null, sourceMessage, event != null ? event.getVersionTag() : null);
      
      if (sourceMessage.requireOldValue && event != null) {
        event.exportOldValue(m);
      }

      m.setRecipient(recipient);
      dm.putOutgoing(m);
    }

    /**
     * Processes this message.  This method is invoked by the receiver
     * of the message.
     * @param dm the distribution manager that is processing the message.
     */
    @Override
    public void process(final DM dm, final ReplyProcessor21 rp) {
      final long startTime = getTimestamp();
      LogWriterI18n l = dm.getLoggerI18n();
      if (l.fineEnabled()) {
        l.fine("Processing " + this);
      }
      //dm.getLogger().warning("RemotePutResponse processor is "
      //+ ReplyProcessor21.getProcessor(this.processorId));
      if (rp == null) {
        if (l.fineEnabled()) {
          l.fine("PutReplyMessage processor not found");
        }
        return;
      }
      if (this.versionTag != null) {
        this.versionTag.replaceNullIDs(getSender());
      }
      if (rp instanceof RemotePutResponse) {
        RemotePutResponse processor = (RemotePutResponse)rp;
        processor.setResponse(this);
      }
      rp.process(this);

      if (DistributionManager.VERBOSE) {
        LogWriterI18n logger = dm.getLoggerI18n();
        logger.info(LocalizedStrings.RemotePutMessage_0__PROCESSED__1,
            new Object[] { rp, this });
      }
      dm.getStats().incReplyMessageTime(NanoTimer.getTime() - startTime);
    }

    /** Return oldValue in deserialized form */
    public Object getOldValue() {
      // oldValue field is in serialized form, either a CachedDeserializable,
      // a byte[], or null if not set
      if (this.oldValue instanceof CachedDeserializable) {
        return ((CachedDeserializable)this.oldValue).getDeserializedValue(null,
            null);
      }
      return this.oldValue;
    }

    @Override
    public int getDSFID() {
      return R_PUT_REPLY_MESSAGE;
    }

    @Override
    public void fromData(DataInput in)
      throws IOException, ClassNotFoundException {
      super.fromData(in);
      byte flags = (byte)(in.readByte() & 0xff);
      this.result = (flags & FLAG_RESULT) != 0;
      this.op = Operation.fromOrdinal(in.readByte());
      this.oldValue = DataSerializer.readObject(in);
      if ((flags & FLAG_HASVERSION) != 0) {
        boolean persistentTag = (flags & FLAG_PERSISTENT) != 0;
        this.versionTag = VersionTag.create(persistentTag, in);
      }
    }

    @Override
    public void toData(DataOutput out) throws IOException {
      super.toData(out);
      byte flags = 0;
      if (this.result) flags |= FLAG_RESULT;
      if (this.versionTag != null) flags |= FLAG_HASVERSION;
      if (this.versionTag instanceof DiskVersionTag) flags |= FLAG_PERSISTENT;
      out.writeByte(flags);
      out.writeByte(this.op.ordinal);
      if (this.oldValueIsSerialized) {
        byte[] oldValueBytes = (byte[]) this.oldValue;
        out.write(oldValueBytes);
      } else {
        DataSerializer.writeObject(this.oldValue, out);
      }
      if (this.versionTag != null) {
        InternalDataSerializer.invokeToData(this.versionTag, out);
      }
    }

    @Override
    public String toString() {
      StringBuilder sb = new StringBuilder();
      sb.append("PutReplyMessage ")
      .append("processorid=").append(this.processorId)
      .append(" returning ").append(this.result)
      .append(" op=").append(op)
      .append(" exception=").append(getException());
      if (this.versionTag != null) {
        sb.append(" version=").append(this.versionTag);
      }
      return sb.toString();
    }

    @Override
    public boolean prefersOldSerialized() {
      return true;
    }

    @Override
    public boolean isUnretainedOldReferenceOk() {
      return true;
    }
    
    @Override
    public boolean isCachedDeserializableValueOk() {
      return true;
    }

    @Override
    public void importOldObject(@Unretained(ENTRY_EVENT_OLD_VALUE) Object ov, boolean isSerialized) {
      // isSerialized does not matter.
      // toData will just call writeObject
      // and fromData will just call readObject
      this.oldValue = ov;
    }

    @Override
    public void importOldBytes(byte[] ov, boolean isSerialized) {
      if (isSerialized) {
        this.oldValueIsSerialized = true;
      }
      this.oldValue = ov;
    }
  }

  /**
   * A processor to capture the value returned by {@link RemotePutMessage}
   * @author bruce
   * @since 5.1
   */
  public static final class RemotePutResponse extends RemoteOperationResponse {
    private volatile boolean returnValue;
    private volatile Operation op;
    private volatile Object oldValue;
    private volatile VersionTag<?> versionTag;
    private final Object key;
    private RemotePutMessage putMessage;

    public RemotePutResponse(InternalDistributedSystem ds,
        InternalDistributedMember recipient, Object key, boolean register) {
      super(ds, recipient, register);
      this.key = key;
    }

    public RemotePutResponse(InternalDistributedSystem ds,
        Set<?> recipients, Object key, boolean register) {
      super(ds, recipients, register);
      this.key = key;
    }

    public void setRemotePutMessage(RemotePutMessage putMessage) {
      this.putMessage = putMessage;
    }
    
    public RemotePutMessage getRemotePutMessage() {
      return this.putMessage;
    }

    public final synchronized void setResponse(PutReplyMessage msg) {
      // for multiple results in case of transaction, fail if it failed anywhere
      if (this.op == null || !msg.result) {
        this.returnValue = msg.result;
        this.op = msg.op;
        this.oldValue = msg.getOldValue();
        this.versionTag = msg.versionTag;
      }
    }

    /**
     * @return the result of the remote put operation
     * @throws RemoteOperationException if the peer is no longer available
     * @throws CacheException if the peer generates an error
     */
    public PutResult waitForResult() throws CacheException,
        RemoteOperationException {
      try {
        waitForCacheException();
      }
      catch (RemoteOperationException e) {
        e.checkKey(key);
        throw e;
      }
      if (this.op == null) {
        throw new RemoteOperationException(
            LocalizedStrings.RemotePutMessage_DID_NOT_RECEIVE_A_VALID_REPLY
                .toLocalizedString());
      }
      return new PutResult(this.returnValue, this.op, this.oldValue,
          this.versionTag);
    }
  }

  public static class PutResult  {
    /** the result of the put operation */
    public boolean returnValue;
    /** the actual operation performed (CREATE/UPDATE) */
    public Operation op;

    /** the old value, or null if not set */
    public Object oldValue;

    /** the concurrency control version tag */
    public VersionTag<?> versionTag;

    public PutResult(boolean flag, Operation actualOperation, Object oldValue,
        VersionTag<?> versionTag) {
      this.returnValue = flag;
      this.op = actualOperation;
      this.oldValue = oldValue;
      this.versionTag = versionTag;
    }
  }

  public void setSendDelta(boolean sendDelta) {
    this.sendDelta = sendDelta;
  }

  @Override
  public boolean prefersNewSerialized() {
    return true;
  }

  @Override
  public boolean isUnretainedNewReferenceOk() {
    return true;
  }

  private void setDeserializationPolicy(boolean isSerialized) {
    if (!isSerialized) {
      this.deserializationPolicy = DistributedCacheOperation.DESERIALIZATION_POLICY_NONE;
    }
  }

  @Override
  public void importNewObject(@Unretained(ENTRY_EVENT_NEW_VALUE) Object nv, boolean isSerialized) {
    setDeserializationPolicy(isSerialized);
    setValObj(nv);
  }

  @Override
  public void importNewBytes(byte[] nv, boolean isSerialized) {
    setDeserializationPolicy(isSerialized);
    setValBytes(nv);
  }

  @Override
  public boolean prefersOldSerialized() {
    return true;
  }

  @Override
  public boolean isUnretainedOldReferenceOk() {
    return true;
  }

  @Override
  public boolean isCachedDeserializableValueOk() {
    return false;
  }
  
  private void setOldValueIsSerialized(boolean isSerialized) {
    if (isSerialized) {
      if (CachedDeserializableFactory.preferObject()) {
        this.oldValueIsSerialized = VALUE_IS_OBJECT;
      } else {
        // Defer serialization until toData is called.
        this.oldValueIsSerialized = VALUE_IS_SERIALIZED_OBJECT;
      }
    } else {
      this.oldValueIsSerialized = VALUE_IS_BYTES;
    }
  }
  
  public void importOldObject(@Unretained(ENTRY_EVENT_OLD_VALUE) Object ov, boolean isSerialized) {
    setOldValueIsSerialized(isSerialized);
    // Defer serialization until toData is called.
    setOldValObj(ov);
  }

  @Override
  public void importOldBytes(byte[] ov, boolean isSerialized) {
    setOldValueIsSerialized(isSerialized);
    setOldValBytes(ov);
  }
}
