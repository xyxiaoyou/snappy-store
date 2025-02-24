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
package com.gemstone.gemfire.internal.cache.persistence;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import com.gemstone.gemfire.cache.persistence.ConflictingPersistentDataException;
import com.gemstone.gemfire.cache.persistence.RevokedPersistentDataException;
import com.gemstone.gemfire.distributed.internal.DistributionManager;
import com.gemstone.gemfire.distributed.internal.MembershipListener;
import com.gemstone.gemfire.distributed.internal.ProfileListener;
import com.gemstone.gemfire.distributed.internal.membership.InternalDistributedMember;
import com.gemstone.gemfire.i18n.LogWriterI18n;
import com.gemstone.gemfire.internal.cache.GemFireCacheImpl;
import com.gemstone.gemfire.internal.i18n.LocalizedStrings;
import com.gemstone.gemfire.internal.util.TransformUtils;
import jdk.nashorn.internal.runtime.linker.LinkerCallSite;

/**
 * @author dsmith
 *
 */
public class PersistentMemberManager {
  
  private final Set<MemberRevocationListener> revocationListeners = new HashSet<MemberRevocationListener>();
  private final Map<PersistentMemberPattern, Object> revokedMembers = new ConcurrentHashMap<PersistentMemberPattern, Object>();
  private final LogWriterI18n logger;
  private Map<PersistentMemberPattern, PendingRevokeListener> pendingRevokes 
      = new HashMap<PersistentMemberPattern, PendingRevokeListener>();

  private final Set<PersistentMemberPattern> doNotWait = new HashSet();
  private volatile boolean unblockNonHostingBuckets = false;

  private static final Object TOKEN = new Object();
  
  public PersistentMemberManager(LogWriterI18n logger) {
    this.logger = logger;
  }
  
  public void revokeMember(PersistentMemberPattern pattern) {
    cancelRevoke(pattern);
    synchronized(this) {
      if(revokedMembers.put(pattern, TOKEN) == null) {
        this.logger.info(LocalizedStrings.PersistenceAdvisorImpl_MEMBER_REVOKED,pattern);
        for(MemberRevocationListener listener : revocationListeners) {
          listener.revoked(pattern);
        }
      }
    }
  }

  public boolean doNotWaitOnMember(PersistentMemberID id) {
    synchronized (this) {
      for (PersistentMemberPattern p : this.doNotWait) {
        if (p.matches(id))
          return true;
      }
    }
    return false;
  }

  public void unblockMemberForPattern(PersistentMemberPattern pattern) {
    synchronized (this) {
      for (MemberRevocationListener listener : revocationListeners) {
        if (listener.matches(pattern)) {
          listener.unblock();
        }
      }
      if (pattern != null)
        this.doNotWait.add(pattern);

      this.unblockNonHostingBuckets = true;
    }
  }

  public boolean unblockNonHostingBuckets() {
    return this.unblockNonHostingBuckets;
  }

  /**
   * Add a new revokation listener
   * 
   * @param listener
   *          The revokocation listener
   * @param recoveredRevokedMembers
   *          a set of members which the listener knows have been revoked
   */
  public HashSet<PersistentMemberPattern> addRevocationListener(
      MemberRevocationListener listener,
      Set<PersistentMemberPattern> recoveredRevokedMembers) {
    synchronized(this) {
      //Fix for 42607, don't allow us to start up a member if we're in the
      //process of revoking that member.
      for(PersistentMemberPattern pattern: pendingRevokes.keySet()) {
        if(listener.matches(pattern)) {
          throw new RevokedPersistentDataException(LocalizedStrings.PersistentMemberManager_Member_0_is_already_revoked.toLocalizedString(pattern));
        }
      }
      for(PersistentMemberPattern pattern: revokedMembers.keySet()) {
        if(listener.matches(pattern)) {
          throw new RevokedPersistentDataException(LocalizedStrings.PersistentMemberManager_Member_0_is_already_revoked.toLocalizedString(pattern));
        }
      }
      for(PersistentMemberPattern pattern: recoveredRevokedMembers) {
        revokedMembers.put(pattern, TOKEN);
      }
      revocationListeners.add(listener);
      return new HashSet<PersistentMemberPattern>(revokedMembers.keySet());
    }
  }
  
  public void removeRevocationListener(ProfileListener listener) {
    synchronized(this) {
      revocationListeners.remove(listener);
    }
  }
  
  public HashSet<PersistentMemberPattern> getRevokedMembers() {
    synchronized(this) {
      return new HashSet<PersistentMemberPattern>(revokedMembers.keySet());
    }
  }
  
  /**
   * Returns a map of the regions that are waiting to recover to
   * the persistent member ids that each region is waiting for.
   */
  public Map<String, Set<PersistentMemberID>> getWaitingRegions() {
    synchronized(this) {
      Map<String, Set<PersistentMemberID>> missingMemberIds = new HashMap<String, Set<PersistentMemberID>>();

      for(MemberRevocationListener listener : revocationListeners) {
        String regionPath = listener.getRegionPath();
        Set<PersistentMemberID> ids = listener.getMissingMemberIds();
        Set<PersistentMemberID> allIds = missingMemberIds.get(regionPath);
        if(ids != null) {
          if(allIds != null) {
            allIds.addAll(ids);
          } else {
            allIds = ids;
            missingMemberIds.put(regionPath, allIds);
          }
        }
      }
      return missingMemberIds;
    }
  }

  public Set<PersistentMemberID> getWaitingIds() {
    synchronized(this) {
      Set<PersistentMemberID> waitingIds = new HashSet<PersistentMemberID>();
      for(MemberRevocationListener listener : revocationListeners) {
        Set<PersistentMemberID> ids = listener.getMissingMemberIds();
        if(ids != null && ids.size() > 0) {
          listener.addPersistentIDs(waitingIds);
        }
      }
      return waitingIds;
    }
  }

  /**
   * Returns a set of the persistent ids that are running on this member.
   */
  public Set<PersistentMemberID> getPersistentIDs() {
    synchronized(this) {
      Set<PersistentMemberID> localData = new HashSet<PersistentMemberID>();
      for(MemberRevocationListener listener : revocationListeners) {
        String regionPath = listener.getRegionPath();
        listener.addPersistentIDs(localData);
      }
      return localData;
    }
  }
  
  public boolean isRevoked(String regionPath, PersistentMemberID id) {
    for(PersistentMemberPattern member : revokedMembers.keySet()) {
      if(member.matches(id)) {
        return true;
      }
    }
    return false;
  }

  public boolean isUnblocked(PersistentMemberID id) {
    for(PersistentMemberPattern member : doNotWait) {
      if(member.matches(id)) {
        return true;
      }
    }
    return false;
  }
  
  /**
   * Prepare the revoke of a persistent id.
   * @param pattern the pattern to revoke
   * @param dm the distribution manager
   * @param sender the originator of the prepare
   * @return true if this member is not currently running the chosen disk store.
   * false if the revoke should be aborted because the disk store is already running.
   */
  public boolean prepareRevoke(PersistentMemberPattern pattern, 
      DistributionManager dm, 
      InternalDistributedMember sender) {
    logger.fine("Preparing revoke if pattern " + pattern);
    
    
    PendingRevokeListener membershipListener= new PendingRevokeListener(pattern, sender, dm);
    synchronized(this) {
      for(MemberRevocationListener listener : revocationListeners) {
        if(listener.matches(pattern)) {
          return false;
        }
      }
      
      pendingRevokes.put(pattern, membershipListener);
    }
    
    //Add a membership listener to make sure we cancel the pending
    //revoke if the sender goes away.
    //DO this outside the synch block to avoid lock ordering issues.
    Set members = dm.addMembershipListenerAndGetDistributionManagerIds(membershipListener);
    if(! members.contains(sender) && sender.equals(dm.getId())) {
      cancelRevoke(pattern);
      return false;
    }
    
    return true;
  }
  
  public void cancelRevoke(PersistentMemberPattern pattern) {
    PendingRevokeListener listener;
    synchronized(this) {
      listener = pendingRevokes.remove(pattern);
    }
    if(listener != null) {
      logger.fine("Cancelling revoke of id " + pattern);
      listener.remove();
    }
  }
  
  public static interface MemberRevocationListener {
    public void revoked(PersistentMemberPattern pattern);

    /**
     * Add the persistent id(s) of this listener to
     * the passed in set.
     */
    public void addPersistentIDs(Set<PersistentMemberID> localData);

    /**
     * Return true if this is a listener for a resource that matches
     * the persistent member pattern in question.
     */
    public boolean matches(PersistentMemberPattern pattern);

    /**
     * Return the set of member ids which this resource knows are missing
     */
    public Set<PersistentMemberID> getMissingMemberIds();
    
    public String getRegionPath();

    public void unblock();
  }
  
  public class PendingRevokeListener implements MembershipListener {
    InternalDistributedMember sender;
    private PersistentMemberPattern pattern;
    private DistributionManager dm;

    public PendingRevokeListener(PersistentMemberPattern pattern, InternalDistributedMember sender, DistributionManager dm) {
      this.dm = dm;
      this.pattern = pattern;
      this.sender = sender;
    }

    @Override
    public void memberJoined(InternalDistributedMember id) {
      
    }

    @Override
    public void memberDeparted(InternalDistributedMember id, boolean crashed) {
      if(id.equals(sender)) {
        cancelRevoke(pattern);
      }
      
    }

    @Override
    public void memberSuspect(InternalDistributedMember id,
        InternalDistributedMember whoSuspected) {
    }
    
    @Override
    public void quorumLost(Set<InternalDistributedMember> failures, List<InternalDistributedMember> remaining) {
    }

    public void remove() {
      dm.removeAllMembershipListener(this);
    }

  }
}
