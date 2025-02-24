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
package com.gemstone.gemfire.cache.client.internal;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

import com.gemstone.gemfire.CancelException;
import com.gemstone.gemfire.GemFireException;
import com.gemstone.gemfire.SystemFailure;
import com.gemstone.gemfire.cache.InterestResultPolicy;
import com.gemstone.gemfire.cache.NoSubscriptionServersAvailableException;
import com.gemstone.gemfire.cache.client.ServerConnectivityException;
import com.gemstone.gemfire.cache.client.internal.PoolImpl.PoolTask;
import com.gemstone.gemfire.cache.client.internal.RegisterInterestTracker.RegionInterestEntry;
import com.gemstone.gemfire.cache.client.internal.ServerBlackList.BlackListListener;
import com.gemstone.gemfire.cache.client.internal.ServerBlackList.BlackListListenerAdapter;
import com.gemstone.gemfire.cache.client.internal.ServerBlackList.FailureTracker;
import com.gemstone.gemfire.cache.query.internal.CqQueryImpl;
import com.gemstone.gemfire.cache.query.internal.CqService;
import com.gemstone.gemfire.cache.query.internal.CqStateImpl;
import com.gemstone.gemfire.cache.query.internal.DefaultQueryService;
import com.gemstone.gemfire.distributed.internal.InternalDistributedSystem;
import com.gemstone.gemfire.distributed.internal.ServerLocation;
import com.gemstone.gemfire.i18n.LogWriterI18n;
import com.gemstone.gemfire.internal.Assert;
import com.gemstone.gemfire.internal.cache.BridgeObserver;
import com.gemstone.gemfire.internal.cache.BridgeObserverHolder;
import com.gemstone.gemfire.internal.cache.LocalRegion;
import com.gemstone.gemfire.internal.cache.tier.InterestType;
import com.gemstone.gemfire.internal.cache.tier.sockets.CacheClientUpdater;
import com.gemstone.gemfire.internal.cache.tier.sockets.ClientProxyMembershipID;
import com.gemstone.gemfire.internal.cache.tier.sockets.ServerQueueStatus;
import com.gemstone.gemfire.internal.i18n.LocalizedStrings;
import com.gemstone.gemfire.security.GemFireSecurityException;
import com.gemstone.org.jgroups.util.StringId;

/**
 * Manages Client Queues. Responsible for creating callback connections and
 * satisfying redundancy requirements.
 * 
 * @author dsmith
 * @since 5.7
 * 
 */
public class QueueManagerImpl implements QueueManager {

//  private static final long SERVER_LOCATION_TIMEOUT = Long.getLong(
//      "gemfire.QueueManagerImpl.SERVER_LOCATION_TIMEOUT", 120000).longValue();
  private static final Comparator QSIZE_COMPARATOR = new QSizeComparator();

  protected final long redundancyRetryInterval;
  private final EndpointManager endpointManager;
  private final EndpointManager.EndpointListenerAdapter endpointListener;
  private final ConnectionSource source;
  private final int redundancyLevel;
  protected final ConnectionFactory factory;
  protected final LogWriterI18n logger;
  private final LogWriterI18n securityLogger;
  private final ClientProxyMembershipID proxyId;
  protected final InternalPool pool;
  private final QueueStateImpl state;
  protected final ServerBlackList blackList;
  // Lock which guards updates to queueConnections.
  // Also threads calling getAllConnections will wait on this
  // lock until there is a primary.
  protected final Object lock = new Object();
  
  protected final CountDownLatch initializedLatch = new CountDownLatch(1);

  private ScheduledThreadPoolExecutor recoveryThread;
  private volatile boolean sentClientReady;

  // queueConnections in maintained by using copy-on-write
  protected volatile ConnectionList queueConnections = new ConnectionList();
  protected volatile RedundancySatisfierTask redundancySatisfierTask = null;
  private volatile boolean shuttingDown;

  public QueueManagerImpl(
      InternalPool pool, 
      EndpointManager endpointManager,
      ConnectionSource source, 
      ConnectionFactory factory,
      int queueRedundancyLevel, 
      long redundancyRetryInterval, 
      LogWriterI18n logger,
      LogWriterI18n securityLogger, 
      ClientProxyMembershipID proxyId) {
    this.pool = pool;
    this.endpointManager = endpointManager;
    this.source = source;
    this.factory = factory;
    this.redundancyLevel = queueRedundancyLevel;
    this.logger = logger;
    this.securityLogger = securityLogger;
    this.proxyId = proxyId;
    this.redundancyRetryInterval = redundancyRetryInterval;
    blackList = new ServerBlackList(logger, redundancyRetryInterval);
    
    
    this.endpointListener = new EndpointManager.EndpointListenerAdapter() {
      @Override
      public void endpointCrashed(Endpoint endpoint) {
        QueueManagerImpl.this.endpointCrashed(endpoint);
      }
    };
    
    this.state = new QueueStateImpl(this);
  }

  public InternalPool getPool() {
    return pool;
  }

  public boolean isPrimaryUpdaterAlive() {
    boolean result = false;
    QueueConnectionImpl primary = (QueueConnectionImpl)
      queueConnections.getPrimary();
    if (primary != null) {
      ClientUpdater cu = primary.getUpdater();
      if (cu != null) {
        result = ((CacheClientUpdater)cu).isAlive();
      }
    }
    return result;
  }
  
  public QueueConnections getAllConnectionsNoWait() {
      return queueConnections;
  }
  
  public QueueConnections getAllConnections() {

    ConnectionList snapshot = queueConnections;
    if (snapshot.getPrimary() == null) {
      // wait for a new primary to become available.
      synchronized (lock) {
        snapshot = queueConnections;
        while (snapshot.getPrimary() == null
            && !snapshot.primaryDiscoveryFailed() && !shuttingDown && pool.getPoolOrCacheCancelInProgress()==null) {
          try {
            lock.wait();
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            break;
          }
          snapshot = queueConnections;
        }
      }
    }
    
    if (snapshot.getPrimary() == null) {
      pool.getCancelCriterion().checkCancelInProgress(null);
      GemFireException exception  = snapshot.getPrimaryDiscoveryException();
      if(exception == null || exception instanceof NoSubscriptionServersAvailableException) {
        exception = new NoSubscriptionServersAvailableException(exception);
      }
      else { 
        exception = new ServerConnectivityException(exception.getMessage(), exception);
      }
      throw exception;
    }

    return snapshot;
  }

  public LogWriterI18n getLogger() {
    return logger;
  }
  
  public LogWriterI18n getSecurityLogger() {
    return securityLogger;
  }

  public void close(boolean keepAlive) {
    endpointManager.removeListener(endpointListener);
    synchronized (lock) {
      shuttingDown = true;
      if (redundancySatisfierTask != null) {
        redundancySatisfierTask.cancel();
      }
      lock.notifyAll();
    }
    if (recoveryThread != null) {
      // it will be null if we never called start
      recoveryThread.shutdown();
    }
    if (recoveryThread != null) {
      try {
        if(!recoveryThread.awaitTermination(PoolImpl.SHUTDOWN_TIMEOUT, TimeUnit.MILLISECONDS)) {
          logger.warning(LocalizedStrings.QueueManagerImpl_TIMEOUT_WAITING_FOR_RECOVERY_THREAD_TO_COMPLETE);
        }
      } catch (InterruptedException e1) {
        Thread.currentThread().interrupt();
        logger.fine("Interrupted waiting for recovery thread termination");
      }
    }
    
    QueueConnectionImpl primary = (QueueConnectionImpl) queueConnections
        .getPrimary();
    if(logger.fineEnabled()) {
      logger.fine("QueueManagerImpl - closing connections with keepAlive=" + keepAlive);
    }
    if (primary != null) {
      try {
        if(logger.fineEnabled()) {
          logger.fine("QueueManagerImpl - closing primary " + primary);
        }
        primary.internalClose(keepAlive);
      } catch (Exception e) {
        logger.warning(
          LocalizedStrings.QueueManagerImpl_ERROR_CLOSING_PRIMARY_CONNECTION_TO_0,
          primary.getEndpoint(), e);
      }
    }

    List backups = queueConnections.getBackups();
    for (Iterator itr = backups.iterator(); itr.hasNext();) {
      QueueConnectionImpl backup = (QueueConnectionImpl) itr.next();
      if (backup != null) {
        try {
          if(logger.fineEnabled()) {
            logger.fine("QueueManagerImpl - closing backup " + backup);
          }
          backup.internalClose(keepAlive);
        } catch (Exception e) {
          logger.warning(
              LocalizedStrings.QueueManagerImpl_ERROR_CLOSING_BACKUP_CONNECTION_TO_0,
              backup.getEndpoint(), e);
        }
      }
    }
  }
  
  
  public void emergencyClose() {
    shuttingDown = true;
    queueConnections.getPrimary().emergencyClose();
    List backups = queueConnections.getBackups();
    for(int i = 0; i < backups.size(); i++) {
      Connection backup = (Connection) backups.get(i);
      backup.emergencyClose();
    }
  }

  public void start(ScheduledExecutorService background) {
    try {
      blackList.start(background);
      endpointManager.addListener(endpointListener);

      // Use a separate timer for queue management tasks
      // We don't want primary recovery (and therefore user threads) to wait for
      // things like pinging connections for health checks.
      // this.background = background;
      final String name = "queueTimer-" + this.pool.getName();
      this.recoveryThread = new ScheduledThreadPoolExecutor(1, new ThreadFactory() {

        public Thread newThread(Runnable r) {
          Thread result = new Thread(r, name);
          result.setDaemon(true);
          return result;
        }


      });
      recoveryThread.setExecuteExistingDelayedTasksAfterShutdownPolicy(false);

//    TODO - use yet another Timer or the like for these tasks? We know
      //we don't want them in the recoveryThread, because the ThreadIdToSequenceIdExpiryTask
      //will wait for primary recovery.
      getState().start(background, getPool().getSubscriptionAckInterval());

      // initialize connections
      initializeConnections();

      scheduleRedundancySatisfierIfNeeded(redundancyRetryInterval);

      //When a server is removed from the blacklist, try again
      //to establish redundancy (if we need to)
      BlackListListener blackListListener = new BlackListListenerAdapter() {
        @Override
        public void serverRemoved(ServerLocation location) {
          QueueManagerImpl.this.scheduleRedundancySatisfierIfNeeded(0);
        }
      };

      blackList.addListener(blackListListener);
      factory.getBlackList().addListener(blackListListener);
    } finally {
      initializedLatch.countDown();
    }
  }
  
  

  public void readyForEvents(InternalDistributedSystem system) {
    synchronized(lock) {
      this.sentClientReady = true;
    }

    QueueConnectionImpl primary = null;
    while (primary == null) {
      try {
        primary = (QueueConnectionImpl) getAllConnections().getPrimary();
      } catch(NoSubscriptionServersAvailableException e) {
        primary = null;
        break;
      }
      if(primary.sendClientReady()) {
        try {
          if(logger.infoEnabled()) {
            logger.info(
              LocalizedStrings.QueueManagerImpl_SENDING_READY_FOR_EVENTS_TO_PRIMARY_0,
              primary);
          }
          ReadyForEventsOp.execute(pool, primary); 
        } catch(Exception e) {
          if(logger.fineEnabled()) {
            logger.fine("Error sending ready for events to " + primary, e);
          }
          primary.destroy();
          primary = null;
        }
      }
    }
  }
  
  public void readyForEventsAfterFailover(QueueConnectionImpl primary) {
    try {
      if(logger.infoEnabled()) {
        logger.info(
          LocalizedStrings.QueueManagerImpl_SENDING_READY_FOR_EVENTS_TO_PRIMARY_0, primary);
      }
      ReadyForEventsOp.execute(pool, primary);
    } catch(Exception e) {
      if(logger.fineEnabled()) {
        logger.fine("Error sending ready for events to " + primary, e);
      }
      primary.destroy();
    }
  }

  void connectionCrashed(Connection con) {
    // the endpoint has not crashed but this method does all the work
    // we need to do
    endpointCrashed(con.getEndpoint());
  }
  
  void endpointCrashed(Endpoint endpoint) {
    QueueConnectionImpl deadConnection = null;
    //We must be synchronized while checking to see if we have a queue connection for the endpoint,
    //because when we need to prevent a race between adding a queue connection to the map
    //and the endpoint for that connection crashing.
    synchronized (lock) {
      deadConnection = queueConnections.getConnection(endpoint);
      if (deadConnection != null) {
        queueConnections = queueConnections.removeConnection(deadConnection);
      }
    }
    if (deadConnection != null) {
      if (logger.fineEnabled()) {
        logger.fine("SubscriptionManager - Endpoint " + endpoint
                    + " crashed. Scheduling recovery");
      }
      scheduleRedundancySatisfierIfNeeded(0);
      deadConnection.internalDestroy();
    } else {
      if (logger.finerEnabled()) {
        logger.finer("Ignoring crashed endpoint " + endpoint
            + " it does not have a queue.");
      }
    }
  }

  private void initializeConnections() {
    logger.fine("SubscriptionManager - intitializing connections");

    int queuesNeeded = redundancyLevel == -1 ? -1 : redundancyLevel + 1;
    Set excludedServers = new HashSet(blackList.getBadServers());
    List servers = findQueueServers(excludedServers, queuesNeeded, true);

    if (servers == null || servers.isEmpty()) {
      logger.warning(
        LocalizedStrings.QueueManagerImpl_COULD_NOT_CREATE_A_QUEUE_NO_QUEUE_SERVERS_AVAILABLE);
      scheduleRedundancySatisfierIfNeeded(redundancyRetryInterval);
      synchronized (lock) {
        queueConnections = queueConnections.setPrimaryDiscoveryFailed(null);
        lock.notifyAll();
      }
      return;
    }

    if (logger.fineEnabled()) {
      logger.fine("SubscriptionManager - discovered subscription servers " + servers);
    }

    SortedMap/* <ServerQueueStatus,Connection> */oldQueueServers = new TreeMap(
        QSIZE_COMPARATOR);
    List nonRedundantServers = new ArrayList();

    for (Iterator itr = servers.iterator(); itr.hasNext();) {
      ServerLocation server = (ServerLocation) itr.next();
      Connection connection = null;
      try {
        connection = factory.createClientToServerConnection(server, true);
      } catch(GemFireSecurityException e) {
        throw e;
      } catch (Exception e) {
        if (logger.fineEnabled()) {
          logger.fine("SubscriptionManager - Error connected to server: " + server, e);
        }
      }
      if (connection != null) {
        ServerQueueStatus status = connection.getQueueStatus();
        if (status.isRedundant() || status.isPrimary()) {
          oldQueueServers.put(status, connection);
        } else {
          nonRedundantServers.add(connection);
        }
      }
    }

    // This ordering was determined from the old ConnectionProxyImpl code
    //
    // initialization order of the new redundant and primary server is
    // old redundant w/ second largest queue
    // old redundant w/ third largest queue
    // ...
    // old primary
    // non redundants in no particular order
    //
    // The primary is then chosen as
    // redundant with the largest queue
    // primary if there are no redundants
    // a non redundant

    // if the redundant with the largest queue fails, then we go and
    // make a new server a primary.

    Connection newPrimary = null;
    if (!oldQueueServers.isEmpty()) {
      newPrimary = (Connection) oldQueueServers.remove(oldQueueServers
          .lastKey());
    } else if (!nonRedundantServers.isEmpty()) {
      newPrimary = (Connection) nonRedundantServers.remove(0);
    }
    
    nonRedundantServers.addAll(0, oldQueueServers.values());

    for (Iterator itr = nonRedundantServers.iterator(); itr.hasNext();) {
      Connection connection = (Connection) itr.next();
      QueueConnectionImpl queueConnection = initializeQueueConnection(
          connection, false, null);
      if (queueConnection != null) {
        addToConnectionList(queueConnection, false);
      }
    }

    QueueConnectionImpl primaryQueue = null;
    if (newPrimary != null) {
      primaryQueue = initializeQueueConnection(newPrimary, true, null);
      if (primaryQueue == null) {
        newPrimary.destroy();
      } else {
        if(!addToConnectionList(primaryQueue, true)) {
          primaryQueue = null;
        }
      }
    }


    excludedServers.addAll(servers);

    // Make sure we have enough redundant copies. Some of the connections may
    // have failed
    // above.
    if (redundancyLevel != -1 && getCurrentRedundancy() < redundancyLevel) {
      logger
          .fine("SubscriptionManager - Some initial connections failed. Trying to create redundant queues");
      recoverRedundancy(excludedServers, false);
    }

    if (redundancyLevel != -1 && primaryQueue == null) {
      logger
          .fine("SubscriptionManager - Intial primary creation failed. Trying to create a new primary");
      while(primaryQueue == null) { 
        primaryQueue = createNewPrimary(excludedServers);
        if(primaryQueue == null) {
          //couldn't find a server to make primary
          break;
        }
        if(!addToConnectionList(primaryQueue, true)) {
          excludedServers.add(primaryQueue.getServer());
          primaryQueue = null;
        }
      }
    }

    if (primaryQueue == null) {
      logger
          .fine("SubscriptionManager - Unable to create a new primary queue, using one of the redundant queues");
      while(primaryQueue == null) {
        primaryQueue = promoteBackupToPrimary(queueConnections.getBackups());
        if(primaryQueue == null) {
          //no backup servers available
          break;
        }
        if(!addToConnectionList(primaryQueue, true)) {
          synchronized(lock) {
            //make sure we don't retry this same connection.
            queueConnections = queueConnections.removeConnection(primaryQueue);
          }
          primaryQueue = null;
        }
      }
    }

    if (primaryQueue == null) {
      logger.error(
          LocalizedStrings.QueueManagerImpl_COULD_NOT_INITIALIZE_A_PRIMARY_QUEUE_ON_STARTUP_NO_QUEUE_SERVERS_AVAILABLE);
      synchronized (lock) {
        queueConnections = queueConnections.setPrimaryDiscoveryFailed(
            new NoSubscriptionServersAvailableException(LocalizedStrings.QueueManagerImpl_COULD_NOT_INITIALIZE_A_PRIMARY_QUEUE_ON_STARTUP_NO_QUEUE_SERVERS_AVAILABLE.toLocalizedString()));
        lock.notifyAll();
      }
      //No primary queue was found, alert the affected cqs if necessary
      CqService.cqsDisconnected(pool);
    }
    else {
      //Primary queue was found, alert the affected cqs if necessary
      CqService.cqsConnected(pool);
    }

    if (getCurrentRedundancy() < redundancyLevel) {
      logger.warning(
          LocalizedStrings.QueueManagerImpl_UNABLE_TO_INITIALIZE_ENOUGH_REDUNDANT_QUEUES_ON_STARTUP_THE_REDUNDANCY_COUNT_IS_CURRENTLY_0,  
          getCurrentRedundancy());
    }
  }

  private int getCurrentRedundancy() {
    return queueConnections.getBackups().size();
  }
  
  /**
   * Make sure that we have enough backup servers.
   * 
   * Add any servers we fail to connect to to the excluded servers list.
   */
  protected boolean recoverRedundancy(Set excludedServers, boolean recoverInterest) {
    if(pool.getPoolOrCacheCancelInProgress() != null) {
      return true;
    }
    int additionalBackups;
    while (pool.getPoolOrCacheCancelInProgress()==null && ((additionalBackups = redundancyLevel - getCurrentRedundancy()) > 0
        || redundancyLevel == -1))  {

      List servers = findQueueServers(excludedServers, redundancyLevel == -1 ? -1 : additionalBackups, false);

      if (servers == null || servers.isEmpty()) {
        if (redundancyLevel != -1) {
          logger.info(
            LocalizedStrings.QueueManagerImpl_REDUNDANCY_LEVEL_0_IS_NOT_SATISFIED_BUT_THERE_ARE_NO_MORE_SERVERS_AVAILABLE_REDUNDANCY_IS_CURRENTLY_1,
            new Object[] { Integer.valueOf(redundancyLevel), Integer.valueOf(getCurrentRedundancy())});
        }
        return false;
      }
      excludedServers.addAll(servers);

      for (Iterator itr = servers.iterator(); itr.hasNext();) {
        ServerLocation server = (ServerLocation) itr.next();
        Connection connection = null;
        try {
          connection = factory.createClientToServerConnection(server, true);
        } catch(GemFireSecurityException e) {
          throw e;
        } catch (Exception e) {
          if (logger.fineEnabled()) {
            logger.fine("SubscriptionManager - Error connecting to server: " + server,
                e);
          }
        }
        if (connection == null) {
          continue;
        }

        QueueConnectionImpl queueConnection = initializeQueueConnection(
            connection, false, null);
        if (queueConnection != null) {
          boolean isFirstNewConnection = false;
          synchronized (lock) {
            if (recoverInterest && queueConnections.getPrimary() == null
                && queueConnections.getBackups().isEmpty()) {
              //  we lost our queue at some point. We Need to recover
              // interest. This server will be made primary after this method
              // finishes
              // because whoever killed the primary when this method started
              // should
              // have scheduled a task to recover the primary.
              isFirstNewConnection = true;
              // TODO - Actually, we need a better check than the above. There's
              // still a chance
              // that we haven't realized that the primary has died but it is
              // already gone. We should
              // get some information from the queue server about whether it was
              // able to copy the
              // queue from another server and decide if we need to recover our
              // interest based on
              // that information.
            }
          }
          boolean promotionFailed = false;
          if (isFirstNewConnection) {
            if (!promoteBackupCnxToPrimary(queueConnection)) {
              promotionFailed = true;
            }
          }
          if (!promotionFailed) {
            if (addToConnectionList(queueConnection, isFirstNewConnection)) {
              if (logger.fineEnabled()) {
                logger
                  .fine("SubscriptionManager redundancy satisfier - created a queue on server "
                        + queueConnection.getEndpoint());
              }
              // Even though the new redundant queue will usually recover
              // subscription information (see bug #39014) from its initial
              // image provider, in bug #42280 we found that this is not always
              // the case, so clients must always register interest with the new
              // redundant server.
              if(recoverInterest) {
                recoverInterest(queueConnection, isFirstNewConnection);
              }
            }
          } 
        }
      } 
    }

    return true;
  }

  private QueueConnectionImpl promoteBackupToPrimary(List backups) {
    QueueConnectionImpl primary = null;
    for (int i = 0; primary == null && i < backups.size(); i++) {
      QueueConnectionImpl lastConnection = (QueueConnectionImpl) backups.get(i);
      if (promoteBackupCnxToPrimary(lastConnection)) {
        primary = lastConnection;
      }
    }
    return primary;
  }

  private boolean promoteBackupCnxToPrimary(QueueConnectionImpl cnx) {
    boolean result = false;
    if (PoolImpl.BEFORE_PRIMARY_IDENTIFICATION_FROM_BACKUP_CALLBACK_FLAG) {
      BridgeObserver bo = BridgeObserverHolder.getInstance();
      bo.beforePrimaryIdentificationFromBackup();
    }
    try {
      boolean haveSentClientReady = this.sentClientReady;
      if(haveSentClientReady) {
        cnx.sendClientReady();
      }
      ClientUpdater updater = cnx.getUpdater();
      if(updater == null) {
        logger.fine("backup connection was destroyed before it could become the primary.");
        Assert.assertTrue(cnx.isDestroyed());
      } else {
        updater.setFailedUpdater(queueConnections.getFailedUpdater());
        MakePrimaryOp.execute(pool, cnx, haveSentClientReady);
        result = true;
        if (PoolImpl.AFTER_PRIMARY_IDENTIFICATION_FROM_BACKUP_CALLBACK_FLAG) {
          BridgeObserver bo = BridgeObserverHolder.getInstance();
          bo.afterPrimaryIdentificationFromBackup(cnx.getServer());
        }
      }
    } catch (Exception e) {
      if(pool.getPoolOrCacheCancelInProgress() == null) {
        logger.fine("Error making a backup server the primary server for client subscriptions", e);
      }
    }
    return result;
  }
  /**
   * Create a new primary server from a non-redundant server.
   * 
   * Add any failed servers to the excludedServers set.
   */
  private QueueConnectionImpl createNewPrimary(Set excludedServers) {
    QueueConnectionImpl primary = null;
    while (primary == null && pool.getPoolOrCacheCancelInProgress()==null) {
      List servers = findQueueServers(excludedServers, 1, false);
      if (servers == null || servers.isEmpty()) {
        break;
      }

      Connection connection = null;
      try {
        connection = factory
          .createClientToServerConnection((ServerLocation) servers.get(0), true);
      } catch (GemFireSecurityException e) {
        throw e;
      } catch(Exception e) {
        if(logger.fineEnabled()) {
          logger.fine("SubscriptionManagerImpl - error creating a connection to server " + servers.get(0));
        }
      }
      if (connection != null) {
        primary = initializeQueueConnection(connection, true, queueConnections.getFailedUpdater());
      }
      excludedServers.addAll(servers);
    }
    
    if(primary != null && sentClientReady && primary.sendClientReady()) {
      readyForEventsAfterFailover(primary);
    }
    return primary;
  }

  private List findQueueServers(Set excludedServers, int count,
      boolean findDurable) {
    List servers = null;
    try {
      if(pool.getPoolOrCacheCancelInProgress()!=null) {
        return null;
      }
      servers = source.findServersForQueue(excludedServers, count, proxyId, findDurable);
    } catch(GemFireSecurityException e) {
      //propagate the security exception immediately.
      throw e;
    } catch (Exception e) {
      logger
          .warning(
              LocalizedStrings.QueueManagerImpl_COULD_NOT_RETRIEVE_LIST_OF_SERVERS_FOR_SUBSCRIPTION_0,
              new Object[] { e.getMessage() });
    }
    return servers;
  }

  /**
   * Find a new primary, adding any failed servers we encounter to the excluded
   * servers list
   * 
   * First we try to make a backup server the primary, but if run out of backup
   * servers we will try to find a new server.
   */
  protected void recoverPrimary(Set excludedServers) {
    if(pool.getPoolOrCacheCancelInProgress() != null) {
      return;
    }
    if (queueConnections.getPrimary() != null) {
      logger.finest("Primary recovery not needed");
      return;
    }

    if (logger.fineEnabled()) {
      logger
          .fine("SubscriptionManager redundancy satisfier - primary endpoint has been lost. Attempting to recover");
    }

    QueueConnectionImpl newPrimary = null;
    while(newPrimary == null && pool.getPoolOrCacheCancelInProgress()==null) {
      List backups = queueConnections.getBackups();
      newPrimary = promoteBackupToPrimary(backups);
      if(newPrimary == null) {
        //could not find a backup to promote
        break;
      }
      if(!addToConnectionList(newPrimary, true)) {
        synchronized(lock) {
          //make sure we don't retry the same backup server
          queueConnections = queueConnections.removeConnection(newPrimary);
        }
        newPrimary = null;
      }

    }
    
    if(newPrimary != null) {
      if (logger.fineEnabled()) {
        logger
            .fine("SubscriptionManager redundancy satisfier - Switched backup server to primary: "
                + newPrimary.getEndpoint());
      }
      if (PoolImpl.AFTER_PRIMARY_RECOVERED_CALLBACK_FLAG) {
        BridgeObserver bo = BridgeObserverHolder.getInstance();
        bo.afterPrimaryRecovered(newPrimary.getServer());
      }
     
      //new primary from back up server was found, alert affected cqs if necessary
      CqService.cqsConnected(pool);
      return;
    }

    while(newPrimary == null) {
      newPrimary = createNewPrimary(excludedServers);
      if(newPrimary == null) {
        //could not find a new primary to create
        break;
      }
      if(!addToConnectionList(newPrimary, true)) {
        excludedServers.add(newPrimary.getServer());
        newPrimary = null;
      }
      
      if (newPrimary != null) {
        if (logger.fineEnabled()) {
          logger
          .fine("SubscriptionManager redundancy satisfier - Non backup server was made primary. Recovering interest"
              + newPrimary.getEndpoint());
        }

        if(!recoverInterest(newPrimary, true)) {
          excludedServers.add(newPrimary.getServer());
          newPrimary = null;
        }
        //New primary queue was found from a non backup, alert the affected cqs
        CqService.cqsConnected(pool);
      }

      if (newPrimary != null && PoolImpl.AFTER_PRIMARY_RECOVERED_CALLBACK_FLAG) {
        BridgeObserver bo = BridgeObserverHolder.getInstance();
        bo.afterPrimaryRecovered(newPrimary.getServer());
      }
      
      return;
    }
    //No primary queue was found, alert the affected cqs
    CqService.cqsDisconnected(pool);    
    logger
        .fine("SubscriptionManager redundancy satisfier - Could not recover a new primary");
    synchronized (lock) {
      queueConnections = queueConnections.setPrimaryDiscoveryFailed(null);
      lock.notifyAll();
    }
  }

  private QueueConnectionImpl initializeQueueConnection(Connection connection,
      boolean isPrimary, ClientUpdater failedUpdater) {
    QueueConnectionImpl queueConnection = null;
    FailureTracker failureTracker = blackList.getFailureTracker(connection.getServer());
    try {
      ClientUpdater updater = factory.createServerToClientConnection(connection
          .getEndpoint(), this, isPrimary, failedUpdater);
      if (updater != null) {
        queueConnection = new QueueConnectionImpl(this, connection, updater, failureTracker);
      } else {
        logger.warning(
            LocalizedStrings.QueueManagerImpl_UNABLE_TO_CREATE_A_SUBSCRIPTION_CONNECTION_TO_SERVER_0,
            connection.getEndpoint());
      }
    } catch (Exception e) {
      logger.fine("error creating subscription connection to server "
          + connection.getEndpoint(), e);
    }
    if (queueConnection == null) {
      failureTracker.addFailure();
      connection.destroy();
    }
    return queueConnection;
  }
  
  private boolean addToConnectionList(QueueConnectionImpl connection, boolean isPrimary) {
    boolean isBadConnection;
    synchronized(lock) {
      if(connection.getEndpoint().isClosed() || shuttingDown || pool.getPoolOrCacheCancelInProgress()!=null) {
        isBadConnection = true;
      } else {
        isBadConnection = false;
        if(isPrimary) {
          queueConnections = queueConnections.setPrimary(connection);
          lock.notifyAll();
        } else {
          queueConnections = queueConnections.addBackup(connection);
        }
      }
    }
    
    if(isBadConnection) {
      if(logger.fineEnabled()) {
        logger
            .fine("Endpoint "
                + connection.getEndpoint()
                + " crashed while creating a connection. The connection will be destroyed");
      }
      connection.internalDestroy();
    }
    
    return !isBadConnection;
  }

  protected void scheduleRedundancySatisfierIfNeeded(long delay) {
    if(shuttingDown) {
      return;
    }
    
    synchronized (lock) {
      if (queueConnections.getPrimary() == null
          || getCurrentRedundancy() < redundancyLevel || redundancyLevel == -1
          || queueConnections.primaryDiscoveryFailed()) {
        if (redundancySatisfierTask != null) {
          if (redundancySatisfierTask.getRemainingDelay() > delay) {
            redundancySatisfierTask.cancel();
          } else {
            return;
          }
        }

        redundancySatisfierTask = new RedundancySatisfierTask();
        try {
        ScheduledFuture future = recoveryThread.schedule(redundancySatisfierTask,
            delay, TimeUnit.MILLISECONDS);
        redundancySatisfierTask.setFuture(future);
        } catch(RejectedExecutionException e) {
          //ignore, the timer has been cancelled, which means we're shutting down.
        }
      }
    }
  }
  

  private boolean recoverInterest(final QueueConnectionImpl newConnection,
      final boolean isFirstNewConnection) {
    
    if(pool.getPoolOrCacheCancelInProgress() != null) {
      return true;
    }
    
    // recover interest
    try {
      recoverAllInterestTypes(newConnection, isFirstNewConnection);
      newConnection.getFailureTracker().reset();
      return true;
    } 
    catch (CancelException ignore) {
      return true;
      // ok to ignore we are being shutdown
    } catch (VirtualMachineError err) {
      SystemFailure.initiateFailure(err);
      // If this ever returns, rethrow the error. We're poisoned
      // now, so don't let this thread continue.
      throw err;
    } catch (Throwable t) {
      SystemFailure.checkFailure();
      pool.getCancelCriterion().checkCancelInProgress(t);
      logger.warning(
        LocalizedStrings.QueueManagerImpl_QUEUEMANAGERIMPL_FAILED_TO_RECOVER_INTEREST_TO_SERVER_0,
        newConnection.getServer(), t);
      newConnection.getFailureTracker().addFailure();
      newConnection.destroy();
      return false;
    }
  }

  public QueueState getState() {
    return this.state;
  }

  private void recoverSingleList(int interestType, Connection recoveredConnection,
      boolean isDurable, boolean receiveValues, boolean isFirstNewConnection) {
    Iterator i = this.getPool().getRITracker()
    .getRegionToInterestsMap(interestType, isDurable, !receiveValues).values().iterator();
    while (i.hasNext()) { // restore a region
      RegionInterestEntry e = (RegionInterestEntry) i.next();
      recoverSingleRegion(e.getRegion(), e.getInterests(), interestType,
          recoveredConnection, isDurable, receiveValues, isFirstNewConnection);
    } // restore a region
  }

  private void recoverCqs(Connection recoveredConnection, boolean isDurable) {
    Map cqs = this.getPool().getRITracker().getCqsMap();
    Iterator i = cqs.entrySet().iterator();
    while(i.hasNext()) {
      Map.Entry e = (Map.Entry)i.next();
      CqQueryImpl cqi = (CqQueryImpl)e.getKey();
      String name = cqi.getName();
      byte regionDataPolicyOrdinal = cqi.getCqBaseRegion() == null ? (byte)0
          : cqi.getCqBaseRegion().getAttributes().getDataPolicy().ordinal;
      if (this.pool.getMultiuserAuthentication()) {
        UserAttributes.userAttributes.set(((DefaultQueryService)this.pool
            .getQueryService()).getUserAttributes(name));
      }
      try {
        cqi.getCQProxy().createOn(name, recoveredConnection,
            cqi.getQueryString(), ((CqStateImpl)cqi.getState()).getState(),
            isDurable, regionDataPolicyOrdinal);
      } finally {
        UserAttributes.userAttributes.set(null);
      }
    }
  }
  
  // TODO this is distressingly similar to LocalRegion#processSingleInterest
  private void recoverSingleRegion(LocalRegion r, Map keys, int interestType,
      Connection recoveredConnection, boolean isDurable,
      boolean receiveValues, boolean isFirstNewConnection) {
    LogWriterI18n log = getLogger();

    if (log.fineEnabled()) {
      log.fine(this + ".recoverSingleRegion starting kind="
          + InterestType.getString(interestType) + " region=" + r.getFullPath()
          + ": " + keys);
    }
    
    // build a HashMap, key is policy, value is list
    HashMap policyMap = new HashMap();
    Iterator keysIter = keys.entrySet().iterator();
    while (keysIter.hasNext()) { // restore and commit an interest
      Map.Entry me = (Map.Entry) keysIter.next();
      Object key = me.getKey();
      InterestResultPolicy pol = (InterestResultPolicy) me.getValue();
      
      if (interestType == InterestType.KEY) {
    	// Gester: we only consolidate the key into list for InterestType.KEY
        LinkedList keyList = (LinkedList)policyMap.get(pol);
        if (keyList == null) {
          
          keyList = new LinkedList();
        }
        keyList.add(key);
        policyMap.put(pol, keyList);
      } else {
        // for other Interest type, do it one by one
    	recoverSingleKey(r, key, pol, interestType, recoveredConnection,
    	    isDurable, receiveValues, isFirstNewConnection);
      }
    }
    
    // Process InterestType.KEY: Iterator list for each each policy
    Iterator polIter = policyMap.entrySet().iterator();
    while (polIter.hasNext()) {
      Map.Entry me = (Map.Entry) polIter.next();
      LinkedList keyList = (LinkedList)me.getValue();
      InterestResultPolicy pol = (InterestResultPolicy)me.getKey();
      recoverSingleKey(r, keyList, pol, interestType, recoveredConnection,
          isDurable, receiveValues, isFirstNewConnection);
    }
  }

  private void recoverSingleKey(LocalRegion r, Object keys,
      InterestResultPolicy policy, int interestType, Connection recoveredConnection,
      boolean isDurable, boolean receiveValues, boolean isFirstNewConnection) {
	LogWriterI18n log = getLogger();
    r.startRegisterInterest();
    try {
      // Remove all matching values from local cache
      if (isFirstNewConnection) { // only if this recoveredEP
        // becomes primaryEndpoint
        r.clearKeysOfInterest(keys, interestType, policy);
        if (log.fineEnabled()) {
          log.fine(this
                   + ".recoverSingleRegion :Endpoint recovered is primary so clearing the keys of interest starting kind="
                   + InterestType.getString(interestType)
                   + " region=" + r.getFullPath()
                   + ": " + keys);
        }
      }
      // Register interest, get new values back
      List serverKeys;
      serverKeys = r.getServerProxy().registerInterestOn(recoveredConnection,
          keys, interestType, policy, isDurable, !receiveValues,
          r.getAttributes().getDataPolicy().ordinal);
      // Restore keys based on server's response
      if (isFirstNewConnection) {
        // only if this recoveredEP becomes primaryEndpoint
        r.refreshEntriesFromServerKeys(recoveredConnection, serverKeys, policy);
      }
    } finally {
      r.finishRegisterInterest();
    }
  }

  private void recoverInterestList(final Connection recoveredConnection,
      boolean durable, boolean receiveValues, boolean isFirstNewConnection) {
    recoverSingleList(InterestType.KEY, recoveredConnection, durable, receiveValues, isFirstNewConnection);
    recoverSingleList(InterestType.REGULAR_EXPRESSION, recoveredConnection, durable, receiveValues, isFirstNewConnection);
    recoverSingleList(InterestType.FILTER_CLASS, recoveredConnection, durable, receiveValues, isFirstNewConnection);
    recoverSingleList(InterestType.OQL_QUERY, recoveredConnection, durable, receiveValues, isFirstNewConnection);
    // VJR: Recover CQs moved to recoverAllInterestTypes() to avoid multiple
    // calls for receiveValues flag being true and false.
    //recoverCqs(recoveredConnection, durable);
    //recoverSingleList(InterestType.CQ, recoveredConnection, durable,isFirstNewConnection);
  }

  protected void recoverAllInterestTypes(final Connection recoveredConnection,
      boolean isFirstNewConnection) {
    if (PoolImpl.BEFORE_RECOVER_INTERST_CALLBACK_FLAG) {
      BridgeObserver bo = BridgeObserverHolder.getInstance();
      bo.beforeInterestRecovery();
    }
    recoverInterestList(recoveredConnection, false, true, isFirstNewConnection);
    recoverInterestList(recoveredConnection, false, false, isFirstNewConnection);
    recoverCqs(recoveredConnection, false);
    if ( getPool().isDurableClient()) {
      recoverInterestList(recoveredConnection, true, true, isFirstNewConnection);
      recoverInterestList(recoveredConnection, true, false, isFirstNewConnection);
      recoverCqs(recoveredConnection, true);
    }
  }

  
  /**
   * A comparator which sorts queue elements in the order of primary first
   * redundant with smallest queue size ... redundant with largest queue size
   * 
   * @author dsmith
   * 
   */
  protected static class QSizeComparator implements java.util.Comparator {
    public int compare(Object o1, Object o2) {
      ServerQueueStatus s1 = (ServerQueueStatus) o1;
      ServerQueueStatus s2 = (ServerQueueStatus) o2;
      // sort primaries to the front of the list
      if (s1.isPrimary() && !s2.isPrimary()) {
        return -1;
      } else if (!s1.isPrimary() && s2.isPrimary()) {
        return 1;
      } else {
        int diff =  s1.getServerQueueSize() - s2.getServerQueueSize();
        if(diff != 0) {
          return diff;
        } else {
          return s1.getMemberId().compareTo(s2.getMemberId());
        }
      }
    }
  }

  /**
   * A data structure for holding the current set of connections the
   * queueConnections reference should be maintained by making a copy of this
   * data structure for each change.
   * 
   * Note the the order of the backups is significant. The first backup in the
   * list is the first server that will be become primary after the primary
   * fails, etc.
   * 
   * The order of backups in this list is the reverse of the order or endpoints
   * from the old ConnectionProxyImpl .
   */
  public class ConnectionList implements QueueConnections {
    private final QueueConnectionImpl primary;
    private final Map/* <Endpoint, QueueConnection> */connectionMap;
    private final List/* <QueueConnection> */backups;
    /**
     * The primaryDiscoveryException flag is stronger than just not having any
     * queue connections It also means we tried all of the possible queue
     * servers and we'ren't able to connect.
     */
    private final GemFireException primaryDiscoveryException;
    private final QueueConnectionImpl failedPrimary;

    public ConnectionList() {
      primary = null;
      connectionMap = Collections.EMPTY_MAP;
      backups = Collections.EMPTY_LIST;
      primaryDiscoveryException = null;
      failedPrimary = null;
    }

    private ConnectionList(QueueConnectionImpl primary, List backups,
        GemFireException discoveryException, QueueConnectionImpl failedPrimary) {
      this.primary = primary;
      Map allConnectionsTmp = new HashMap();
      for (Iterator itr = backups.iterator(); itr.hasNext();) {
        QueueConnectionImpl nextConnection = (QueueConnectionImpl) itr.next();
        allConnectionsTmp.put(nextConnection.getEndpoint(), nextConnection);
      }
      if (primary != null) {
        allConnectionsTmp.put(primary.getEndpoint(), primary);
      }
      this.connectionMap = Collections.unmodifiableMap(allConnectionsTmp);
      this.backups = Collections.unmodifiableList(new ArrayList(backups));
      pool.getStats().setSubscriptionCount(connectionMap.size());
      this.primaryDiscoveryException = discoveryException;
      this.failedPrimary = failedPrimary;
    }

    public ConnectionList setPrimary(QueueConnectionImpl newPrimary) {
      List newBackups = backups;
      if (backups.contains(newPrimary)) {
        newBackups = new ArrayList(backups);
        newBackups.remove(newPrimary);
      }
      return new ConnectionList(newPrimary, newBackups, null, null);
    }

    public ConnectionList setPrimaryDiscoveryFailed(
        GemFireException p_discoveryException) {
      GemFireException discoveryException = p_discoveryException;
      if(discoveryException == null) {
        discoveryException = new NoSubscriptionServersAvailableException("Primary discovery failed.");
      }
      return new ConnectionList(primary, backups, discoveryException, failedPrimary);
    }

    public ConnectionList addBackup(QueueConnectionImpl queueConnection) {
      ArrayList newBackups = new ArrayList(backups);
      newBackups.add(queueConnection);
      return new ConnectionList(primary, newBackups, primaryDiscoveryException, failedPrimary);
    }

    public ConnectionList removeConnection(QueueConnectionImpl connection) {
      if (primary == connection) {
        return new ConnectionList(null, backups, primaryDiscoveryException, primary);
      } else {
        ArrayList newBackups = new ArrayList(backups);
        newBackups.remove(connection);
        return new ConnectionList(primary, newBackups, primaryDiscoveryException, failedPrimary);
      }
    }

    public Connection getPrimary() {
      return primary;
    }

    public List/* <QueueConnection> */getBackups() {
      return backups;
    }
    
    /**
     * Return the cache client updater from the previously
     * failed primary
     * @return the previous updater or null if there is no previous updater
     */
    public ClientUpdater getFailedUpdater() {
      if(failedPrimary != null) {
        return failedPrimary.getUpdater();
      } else {
        return null;
      }
    }

    public boolean primaryDiscoveryFailed() {
      return primaryDiscoveryException != null;
    }
    
    public GemFireException getPrimaryDiscoveryException() {
      return primaryDiscoveryException;
    }

    public QueueConnectionImpl getConnection(Endpoint endpoint) {
      return (QueueConnectionImpl) connectionMap.get(endpoint);
    }

    /** return a copy of the list of all server locations */
    public Set/* <ServerLocation> */getAllLocations() {
      HashSet locations = new HashSet();
      for (Iterator itr = connectionMap.keySet().iterator(); itr.hasNext();) {
        com.gemstone.gemfire.cache.client.internal.Endpoint endpoint = (com.gemstone.gemfire.cache.client.internal.Endpoint) itr.next();
        locations.add(endpoint.getLocation());
      }

      return locations;
    }
  }
  
  protected void logError(StringId message, Throwable t) {
    if(t instanceof GemFireSecurityException) {
      securityLogger.error(message, t);
    } else { 
      logger.error(message, t);
    }
  }

  /**
   * Asynchronous task which tries to restablish a primary connection and
   * satisfy redundant requirements.
   * 
   * This task should only be running in a single thread at a time. This task is
   * the only way that new queue servers will be added, and the only way that a
   * backup server can transistion to a primary server.
   * 
   */
  protected class RedundancySatisfierTask extends PoolTask {
    private boolean isCancelled;
    private ScheduledFuture future;

    public void setFuture(ScheduledFuture future) {
      this.future = future;
    }

    public long getRemainingDelay() {
      return future.getDelay(TimeUnit.MILLISECONDS);
    }
    
    @Override
    public LogWriterI18n getLogger() {
      return logger;
    }

    @Override
    public void run2() {
      try {
        initializedLatch.await();
        synchronized (lock) {
          if (isCancelled) {
            return;
          } else {
            redundancySatisfierTask = null;
          }
          if(pool.getPoolOrCacheCancelInProgress()!=null) {
	    /* wake up waiters so they can detect cancel */
	    lock.notifyAll();
            return;
          }
        }
        Set excludedServers = queueConnections.getAllLocations();
        excludedServers.addAll(blackList.getBadServers());
        excludedServers.addAll(factory.getBlackList().getBadServers());
        recoverPrimary(excludedServers);
        recoverRedundancy(excludedServers, true);
      } 
      catch (CancelException e) {
        throw e;
      } 
      catch (Throwable t) {
        Error err;
        if (t instanceof Error && SystemFailure.isJVMFailureError(
            err = (Error)t)) {
          SystemFailure.initiateFailure(err);
          // If this ever returns, rethrow the error. We're poisoned
          // now, so don't let this thread continue.
          throw err;
        }
        // Whenever you catch Error or Throwable, you must also
        // check for fatal JVM error (see above).  However, there is
        // _still_ a possibility that you are dealing with a cascading
        // error condition, so you also need to check to see if the JVM
        // is still usable:
        SystemFailure.checkFailure();
        synchronized (lock) {
          if(t instanceof GemFireSecurityException) {
            queueConnections = queueConnections.setPrimaryDiscoveryFailed((GemFireSecurityException) t);
          } else {
            queueConnections = queueConnections.setPrimaryDiscoveryFailed(null);
          }
          lock.notifyAll();
          pool.getCancelCriterion().checkCancelInProgress(t);
          logError(LocalizedStrings.QueueManagerImpl_ERROR_IN_REDUNDANCY_SATISFIER, t);
        }
      }

      scheduleRedundancySatisfierIfNeeded(redundancyRetryInterval);
    }

    public boolean cancel() {
      synchronized (lock) {
        if(isCancelled) {
          return false;
        }
        isCancelled = true;
        future.cancel(false);
        redundancySatisfierTask = null;
        return true;
      }
    }

  }

  public static void loadEmergencyClasses() {
    QueueConnectionImpl.loadEmergencyClasses();
  }
}
