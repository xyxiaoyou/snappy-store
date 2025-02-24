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
/**
 * 
 */
package com.gemstone.gemfire.internal.cache.tier.sockets.command;

import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.client.internal.GetOp;
import com.gemstone.gemfire.cache.operations.GetOperationContext;
import com.gemstone.gemfire.distributed.internal.DistributionStats;
import com.gemstone.gemfire.i18n.LogWriterI18n;
import com.gemstone.gemfire.internal.Assert;
import com.gemstone.gemfire.internal.cache.CachedDeserializable;
import com.gemstone.gemfire.internal.cache.EntryEventImpl;
import com.gemstone.gemfire.internal.cache.LocalRegion;
import com.gemstone.gemfire.internal.cache.PartitionedRegion;
import com.gemstone.gemfire.internal.cache.TXStateInterface;
import com.gemstone.gemfire.internal.cache.Token;
import com.gemstone.gemfire.internal.cache.tier.CachedRegionHelper;
import com.gemstone.gemfire.internal.cache.tier.Command;
import com.gemstone.gemfire.internal.cache.tier.MessageType;
import com.gemstone.gemfire.internal.cache.tier.sockets.BaseCommand;
import com.gemstone.gemfire.internal.cache.tier.sockets.CacheServerStats;
import com.gemstone.gemfire.internal.cache.tier.sockets.ClientProxyMembershipID;
import com.gemstone.gemfire.internal.cache.tier.sockets.Message;
import com.gemstone.gemfire.internal.cache.tier.sockets.Part;
import com.gemstone.gemfire.internal.cache.tier.sockets.ServerConnection;
import com.gemstone.gemfire.internal.cache.versions.VersionTag;
import com.gemstone.gemfire.internal.i18n.LocalizedStrings;
import com.gemstone.gemfire.internal.offheap.StoredObject;
import com.gemstone.gemfire.internal.security.AuthorizeRequest;
import com.gemstone.gemfire.internal.security.AuthorizeRequestPP;
import com.gemstone.gemfire.security.NotAuthorizedException;
import com.gemstone.org.jgroups.util.StringId;

import java.io.IOException;

public class Get70 extends BaseCommand {

  private final static Get70 singleton = new Get70();

  public static Command getCommand() {
    return singleton;
  }

  protected Get70() {
  }
  
  @Override
  public void cmdExecute(Message msg, ServerConnection servConn, long startparam)
      throws IOException {
    long start = startparam;
    Part regionNamePart = null, keyPart = null, valuePart = null;
    String regionName = null;
    Object callbackArg = null, key = null;
    CachedRegionHelper crHelper = servConn.getCachedRegionHelper();
    CacheServerStats stats = servConn.getCacheServerStats();
    StringId errMessage = null;
    if (crHelper.emulateSlowServer() > 0) {
      // this.logger.fine("SlowServer", new Exception());
      boolean interrupted = Thread.interrupted();
      try {
        Thread.sleep(crHelper.emulateSlowServer());
      }
      catch (InterruptedException ugh) {
        interrupted = true;
        servConn.getCachedRegionHelper().getCache().getCancelCriterion()
            .checkCancelInProgress(ugh);
      }
      finally {
        if (interrupted) {
          Thread.currentThread().interrupt();
        }
      }
      ;
    }
    servConn.setAsTrue(REQUIRES_RESPONSE);
    // requiresResponse = true;
    {
      long oldStart = start;
      start = DistributionStats.getStatTime();
      stats.incReadGetRequestTime(start - oldStart);
    }
    // Retrieve the data from the message parts
    int parts = msg.getNumberOfParts();
    regionNamePart = msg.getPart(0);
    keyPart = msg.getPart(1);
//    valuePart = null;  (redundant assignment)
    if (parts > 2) {
      valuePart = msg.getPart(2);
      try {
        callbackArg = valuePart.getObject();
      }
      catch (Exception e) {
        writeException(msg, e, false, servConn);
        // responded = true;
        servConn.setAsTrue(RESPONDED);
        return;
      }
    }
    regionName = regionNamePart.getString();
    try {
      key = keyPart.getStringOrObject();
    }
    catch (Exception e) {
      writeException(msg, e, false, servConn);
      // responded = true;
      servConn.setAsTrue(RESPONDED);
      return;
    }
    if (logger.fineEnabled()) {
      logger.fine(servConn.getName() + ": Received 7.0 get request ("
          + msg.getPayloadLength() + " bytes) from "
          + servConn.getSocketString() + " for region " + regionName + " key "
          + key + " txId " + msg.getTransactionId());
    }

    // Process the get request
    if (key == null || regionName == null) {
      if ((key == null) && (regionName == null)) {
        errMessage = LocalizedStrings.Request_THE_INPUT_REGION_NAME_AND_KEY_FOR_THE_GET_REQUEST_ARE_NULL;
      } else if (key == null) {
        errMessage = LocalizedStrings.Request_THE_INPUT_KEY_FOR_THE_GET_REQUEST_IS_NULL;   
      } else if (regionName == null) {
        errMessage = LocalizedStrings.Request_THE_INPUT_REGION_NAME_FOR_THE_GET_REQUEST_IS_NULL;
      }
      if(logger.warningEnabled()) {
        logger.warning(LocalizedStrings.TWO_ARG_COLON,
          new Object[] {servConn.getName(), errMessage.toLocalizedString()});
      }
      writeErrorResponse(msg, MessageType.REQUESTDATAERROR, errMessage
          .toLocalizedString(), servConn);
      // responded = true;
      servConn.setAsTrue(RESPONDED);
    }
    else {
      Region region = crHelper.getRegion(regionName);
      if (region == null) {
        String reason = LocalizedStrings.Request__0_WAS_NOT_FOUND_DURING_GET_REQUEST.toLocalizedString(regionName);
        writeRegionDestroyedEx(msg, regionName, reason, servConn);
        servConn.setAsTrue(RESPONDED);
      }
      else {
        
        GetOperationContext getContext = null;
        
          try {
            AuthorizeRequest authzRequest = servConn.getAuthzRequest();
              if (authzRequest != null) {
              getContext = authzRequest
                  .getAuthorize(regionName, key, callbackArg);
              callbackArg = getContext.getCallbackArg();
            }
          }
          catch (NotAuthorizedException ex) {
            writeException(msg, ex, false, servConn);
            servConn.setAsTrue(RESPONDED);
            return;
          }

        // Get the value and update the statistics. Do not deserialize
        // the value if it is a byte[].
        Entry entry;
        try {
          entry = getValueAndIsObject(region, key, callbackArg, logger, servConn);
        }
        catch (Exception e) {
          writeException(msg, e, false, servConn);
          servConn.setAsTrue(RESPONDED);
          return;
        }

        Object data = entry.value;
        boolean isObject = entry.isObject;
        VersionTag versionTag = entry.versionTag;
        boolean keyNotPresent = entry.keyNotPresent;
        
        
        try {
          AuthorizeRequestPP postAuthzRequest = servConn.getPostAuthzRequest();
          if (postAuthzRequest != null) {
            getContext = postAuthzRequest.getAuthorize(regionName, key, data,
                isObject, getContext);
            byte[] serializedValue = getContext.getSerializedValue();
            if (serializedValue == null) {
              data = getContext.getObject();
            }
            else {
              data = serializedValue;
            }
            isObject = getContext.isObject();
          }
        }
        catch (NotAuthorizedException ex) {
          writeException(msg, ex, false, servConn);
          servConn.setAsTrue(RESPONDED);
          return;
        }
        {
          long oldStart = start;
          start = DistributionStats.getStatTime();
          stats.incProcessGetTime(start - oldStart);
        }
        
        if (region instanceof PartitionedRegion) {
          PartitionedRegion pr = (PartitionedRegion)region;
          if (pr.isNetworkHop() != (byte)0) {
            writeResponseWithRefreshMetadata(data, callbackArg, msg, isObject,
                servConn, pr, pr.isNetworkHop(), versionTag, keyNotPresent);
            pr.setIsNetworkHop((byte)0);
            pr.setMetadataVersion(Byte.valueOf((byte)0));
          }
          else {
            writeResponse(data, callbackArg, msg, isObject, versionTag, keyNotPresent, servConn);
          }
        }
        else {
          writeResponse(data, callbackArg, msg, isObject, versionTag, keyNotPresent, servConn);
        }
        
        servConn.setAsTrue(RESPONDED);
        if (logger.fineEnabled()) {
          logger.fine(servConn.getName() + ": Wrote get response back to "
              + servConn.getSocketString() + " for region " + regionName
              + entry);
        }
        stats.incWriteGetResponseTime(DistributionStats.getStatTime() - start);
      }
    }

  }

  // take the result 3 element "result" as argument instead of
  // returning as the result to avoid creating the array repeatedly
  // for large number of entries like in getAll.  Third element added in
  // 7.0 for retrieving version information
  public Entry getValueAndIsObject(Region region, Object key,
      Object callbackArg, LogWriterI18n log,
      ServerConnection servConn) {

//    Region.Entry entry;
    final LocalRegion lr = (LocalRegion)region;
    String regionName = lr.getFullPath();
    if (servConn != null) {
      servConn.setModificationInfo(true, regionName, key);
    }
    VersionTag versionTag = null;
//    LocalRegion lregion = (LocalRegion)region;

//    entry = lregion.getEntry(key, true);

    boolean isObject = true;
    Object data = null;

    final TXStateInterface tx = lr.discoverJTA();

//    if (entry != null && region.getAttributes().getConcurrencyChecksEnabled()) {
//      RegionEntry re;
//      if (entry instanceof NonTXEntry) {
//        re = ((NonTXEntry)entry).getRegionEntry();
//      } else if (entry instanceof EntrySnapshot) {
//        re = ((EntrySnapshot)entry).getRegionEntry();
//      } else if (entry instanceof TXEntry) {
//        re = null; // versioning not supported in tx yet
//        data = entry.getValue(); // can I get a serialized form??
//      } else {
//        re = (RegionEntry)entry;
//      }
//      if (re != null) {
//        data = re.getValueInVM();
//        VersionStamp stamp = re.getVersionStamp();
//        if (stamp != null) {
//          versionHolder.setVersionTag(stamp.asVersionTag());
//        }
//      }
//    } else {
    ClientProxyMembershipID id = servConn == null ? null : servConn
        .getProxyID();
    EntryEventImpl versionHolder = EntryEventImpl.createVersionTagHolder();
    try {
      // TODO OFFHEAP: optimize
      lr.operationStart();
      data = lr.get(key, callbackArg, true, true, true, id, tx, null,
          versionHolder, true, true/*allowReadFromHDFS*/);
    } finally {
      versionHolder.release();
      lr.operationCompleted();
    }
    versionTag = versionHolder.getVersionTag();
    
    // If the value in the VM is a CachedDeserializable,
    // get its value. If it is Token.REMOVED, Token.DESTROYED,
    // Token.INVALID, or Token.LOCAL_INVALID
    // set it to null. If it is NOT_AVAILABLE, get the value from
    // disk. If it is already a byte[], set isObject to false.
    boolean wasInvalid = false;
    if (data instanceof CachedDeserializable) {
      if (data instanceof StoredObject && !((StoredObject) data).isSerialized()) {
        // it is a byte[]
        isObject = false;
        data = ((StoredObject) data).getDeserializedForReading();
      } else {
        data = ((CachedDeserializable)data).getValue();
      }
    }
    else if (data == Token.REMOVED_PHASE1 || data == Token.REMOVED_PHASE2 || data == Token.DESTROYED) {
      data = null;
    }
    else if (data == Token.INVALID || data == Token.LOCAL_INVALID) {
      data = null; // fix for bug 35884
      wasInvalid = true;
    }
    else if (data instanceof byte[]) {
      isObject = false;
    }
    Entry result = new Entry();
    result.value = data;
    result.isObject = isObject;
    result.keyNotPresent = !wasInvalid && (data == null || data == Token.TOMBSTONE);
    result.versionTag = versionTag;
    return result;
  }

  /** this is used to return results from getValueAndIsObject */
  public static class Entry {
    public Object value;
    public boolean isObject;
    public boolean keyNotPresent;
    public VersionTag versionTag;
    
    @Override
    public String toString() {
      return "value=" + value + " isObject=" + isObject + " notPresent=" + keyNotPresent + " version=" + versionTag;
    }
  }
  
  @Override
  protected void writeReply(Message origMsg, ServerConnection servConn)
      throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  protected void writeReplyWithRefreshMetadata(Message origMsg,
      ServerConnection servConn, PartitionedRegion pr, byte nwHop) throws IOException {
    throw new UnsupportedOperationException();
  }

  private void writeResponse(Object data, Object callbackArg,
      Message origMsg, boolean isObject, VersionTag versionTag, boolean keyNotPresent, ServerConnection servConn)
      throws IOException {
    Message responseMsg = servConn.getResponseMessage();
    responseMsg.setMessageType(MessageType.RESPONSE);
    responseMsg.setTransactionId(origMsg.getTransactionId());

    int numParts = 2;
    int flags = 0;
    
    if (callbackArg != null) {
      numParts++;
      flags |= GetOp.HAS_CALLBACK_ARG;
    }
    if (versionTag != null) {
      numParts++;
      flags |= GetOp.HAS_VERSION_TAG;
    }
    if (keyNotPresent) {
      flags |= GetOp.KEY_NOT_PRESENT;
    } else if (data == null  &&  isObject) {
      flags |= GetOp.VALUE_IS_INVALID;
    }
//    servConn.getLogger().fine("returning flags " + Integer.toBinaryString(flags));
    
    responseMsg.setNumberOfParts(numParts);
    
    if (data instanceof byte[]) {
      responseMsg.addRawPart((byte[])data, isObject);
    }
    else {
      Assert.assertTrue(isObject,
          "isObject should be true when value is not a byte[]");
      responseMsg.addObjPart(data, zipValues);
    }

    responseMsg.addIntPart(flags);


    if (callbackArg != null) {
      responseMsg.addObjPart(callbackArg);
    }
    if (versionTag != null) {
      responseMsg.addObjPart(versionTag);
    }
    servConn.getCache().getCancelCriterion().checkCancelInProgress(null);
    responseMsg.send(servConn);
    origMsg.flush();
  }
  
  protected static void writeResponse(Object data, Object callbackArg,
      Message origMsg, boolean isObject, ServerConnection servConn)
      throws IOException {
    throw new UnsupportedOperationException();
  }

  private void writeResponseWithRefreshMetadata(Object data,
      Object callbackArg, Message origMsg, boolean isObject,
      ServerConnection servConn, PartitionedRegion pr, byte nwHop,
      VersionTag versionTag, boolean keyNotPresent) throws IOException {
    Message responseMsg = servConn.getResponseMessage();
    responseMsg.setMessageType(MessageType.RESPONSE);
    responseMsg.setTransactionId(origMsg.getTransactionId());

    int numParts = 3;

    int flags = 0;
    
    if (callbackArg != null) {
      numParts++;
      flags |= GetOp.HAS_CALLBACK_ARG;
    }
    if (versionTag != null) {
      numParts++;
      flags |= GetOp.HAS_VERSION_TAG;
    }
    if (keyNotPresent) {
      flags |= GetOp.KEY_NOT_PRESENT;
    } else if (data == null  &&  isObject) {
      flags |= GetOp.VALUE_IS_INVALID;
    }
//    servConn.getLogger().fine("returning flags " + Integer.toBinaryString(flags));
    
    responseMsg.setNumberOfParts(numParts);

    if (data instanceof byte[]) {
      responseMsg.addRawPart((byte[])data, isObject);
    }
    else {
      Assert.assertTrue(isObject,
          "isObject should be true when value is not a byte[]");
      responseMsg.addObjPart(data, zipValues);
    }
    
    responseMsg.addIntPart(flags);
    
    if (callbackArg != null) {
      responseMsg.addObjPart(callbackArg);
    }
    if (versionTag != null) {
      responseMsg.addObjPart(versionTag);
    }

    responseMsg.addBytesPart(new byte[]{pr.getMetadataVersion().byteValue(),nwHop});
    servConn.getCache().getCancelCriterion().checkCancelInProgress(null);
    responseMsg.send(servConn);
    origMsg.flush();
  }

}
