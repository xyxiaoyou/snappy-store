/*
 * Copyright (c) 2017-2019 TIBCO Software Inc. All rights reserved.
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
package sql.snappy;

import java.util.Vector;

import hydra.BasePrms;
import hydra.HydraVector;

public class SnappyPrms extends BasePrms {

  /**
   * Parameter used to get the user specified script names.
   * (VectosetValues of Strings) A list of values for script Names to execute.
   */
  public static Long sqlScriptNames;

  /**
   * Parameter used to get the user specified data location List for the sql scripts.
   * (VectorsetValues of Strings) A list of values for dataLocation to be replaced in the
   * sql scripts.
   * If no parameter is required for sql script then expected value to be provided for param is :
   * Empty String : " " in case if user don't want to maintain the sequence.
   * Or else provide the script that does not require any parameter at the end in list of
   * sqlScriptNames parameter.
   * Framework will treat its corresponding parameter as " " string in this case.
   */
  public static Long dataLocation;

  /**
   * Parameter used to get the user specified persistence mode List for the sql scripts.
   * (VectorsetValues of Strings) A list of values for persistenceMode to be replaced in the
   * sql scripts.
   * If no parameter is required for sql script then expected value to be provided for param is :
   * Empty String : " " in case if user don't want to maintain the sequence.
   * Or else provide the script that does not require any parameter at the end in list of
   * sqlScriptNames parameter.
   * Framework will treat its corresponding parameter as "async" in this case.
   */
  public static Long persistenceMode;

  /**
   * Parameter used to get the user specified PARTITION_BY option List for the sql scripts.
   * (VectorsetValues of Strings) A list of values for PARTITION_BY option to be replaced in the
   * sql scripts.
   * If no parameter is required for sql script then expected value to be provided for param is :
   * Empty String : " " in case if user don't want to maintain the sequence.
   * Or else provide the script that does not require any parameter at the end in list of
   * sqlScriptNames parameter.
   * Framework will treat its corresponding parameter as " " string in this case.
   */
  public static Long partitionBy;

  /**
   * Parameter used to get the user specified BUCKETS option List for the sql scripts.
   * (VectorsetValues of Strings) A list of values for BUCKETS option to be replaced in the
   * sql scripts.
   * If no parameter is required for sql script then expected value to be provided for param is :
   * Empty String : " " in case if user don't want to maintain the sequence.
   * Or else provide the script that does not require any parameter at the end in list of
   * sqlScriptNames parameter.
   * Framework will treat its corresponding parameter as "113 " in this case.
   */
  public static Long numPartitions;

  /**
   * Parameter used to get the user specified colocation option List for the sql scripts.
   * (VectorsetValues of Strings) A list of values for COLOCATE_WITH option to be replaced in the
   * sql scripts.
   * If no parameter is required for sql script then expected value to be provided for param is :
   * Empty String : " " in case if user don't want to maintain the sequence.
   * Or else provide the script that does not require any parameter at the end in list of
   * sqlScriptNames parameter.
   * Framework will treat its corresponding parameter as "none" in this case.
   */
  public static Long colocateWith;

  /**
   * Parameter used to get the user specified REDUNDANCY option List for the sql scripts.
   * (VectorsetValues of Strings) A list of values for REDUNDANCY option to be replaced in the
   * sql scripts.
   * If no parameter is required for sql script then expected value to be provided for param is :
   * Empty String : " " in case if user don't want to maintain the sequence.
   * Or else provide the script that does not require any parameter at the end in list of
   * sqlScriptNames parameter.
   * Framework will treat its corresponding parameter as " " string in this case.
   */
  public static Long redundancy;

  /**
   * Parameter used to get the user specified RECOVER_DELAY option List for the sql scripts.
   * (VectorsetValues of Strings) A list of values for RECOVER_DELAY option to be replaced in the
   * sql scripts.
   * If no parameter is required for sql script then expected value to be provided for param is :
   * Empty String : " " in case if user don't want to maintain the sequence.
   * Or else provide the script that does not require any parameter at the end in list of
   * sqlScriptNames parameter.
   * Framework will treat its corresponding parameter as " " string in this case.
   */
  public static Long recoverDelay;

  /**
   * Parameter used to get the user specified MAX_PART_SIZE option List for the sql scripts.
   * (VectorsetValues of Strings) A list of values for MAX_PART_SIZE option to be replaced in the
   * sql scripts.
   * If no parameter is required for sql script then expected value to be provided for param is :
   * Empty String : " " in case if user don't want to maintain the sequence.
   * Or else provide the script that does not require any parameter at the end in list of
   * sqlScriptNames parameter.
   * Framework will treat its corresponding parameter as " " string in this case.
   */
  public static Long maxPartitionSize;

  /**
   * Parameter used to get the user specified EVICTION_BY option List for the sql scripts.
   * (VectorsetValues of Strings) A list of values for EVICTION_BY option to be replaced in the
   * sql scripts.
   * If no parameter is required for sql script then expected value to be provided for param is :
   * Empty String : " " in case if user don't want to maintain the sequence.
   * Or else provide the script that does not require any parameter at the end in list of
   * sqlScriptNames parameter.
   * Framework will treat its corresponding parameter as " " string in this case.
   */
  public static Long evictionBy;

  /**
   * Parameter used to get the user specified snappy job class names.
   * (VectosetValues of Strings) A list of values for snappy-job Names to execute.
   */
  public static Long jobClassNames;

  /**
   * Parameter used to get the user specified spark job class names.
   * (VectosetValues of Strings) A list of values for spark-job Names to execute.
   */
  public static Long sparkJobClassNames;

  /**
   * Parameter used to get the user specified snappy streaming job class names.
   * (VectosetValues of Strings) A list of values for snappy-job Names to execute.
   */
  public static Long streamingJobClassNames;

  /**
   * (boolean) for testing HA
   */
  public static Long cycleVms;

  /**
   * (String) cycleVMTarget - which node to be cycled "store, lead" etc
   */
  public static Long cycleVMTarget;

  /**
   * (String) e.g. simulateFileStream
   */
  public static Long simulateStreamScriptName;

  /**
   * (String) - destination folder to copy the streaming data. e.g. /home/swati
   */
  public static Long simulateStreamScriptDestinationFolder;

  /**
   * (boolean) - whether snappy servers and locators needs to be started using rowstore option.
   */
  public static Long useRowStore;

  /**
   * (boolean) - whether smart connector mode cluster needs to be started.
   */
  public static Long useSmartConnectorMode;

  /**
   * (boolean) - whether stop mode needs to be checked before deleting the config data if already
   * exists.
   * This is required in case user wants to start the cluster and then stop the same later on using
   * different script.
   * In this case, test should not delete the existing configuration data created by previous test.
   */
  public static Long isStopMode;

  /**
   * (boolean) - whether to start the snappy cluster forcefully.
   * This is required in case user wants to restart the cluster multiple times
   */
  public static Long forceStart;

  /**
   * (boolean) - whether to copy the config data forcefully.
   * This is required in case of lead, locator and server member's HA in same test
   */
  public static Long forceCopy;

  /**
   * (boolean) - whether created tables to be replicated or partitioned. snappy hydra already sets
   * the gemfirexd.table-default-partitioned to false.
   */
  public static Long tableDefaultPartitioned;

  /**
   * (boolean) - whether to enable/disable PERSIST-INDEXES. Product default value will be used in
   * case not provided.
   */
  public static Long persistIndexes;

  /**
   * (boolean) - whether test is long running.
   */
  public static Long isLongRunningTest;

  /**
   * (boolean) - whether to enable time statistics. snappy hydra already sets the
   * enable-time-statistics to true.
   */
  public static Long enableTimeStatistics;

  /**
   * (boolean) - whether to enable closedForm Estimates. Product default value will be used in
   * case not provided.
   */
  public static Long closedFormEstimates;

  /**
   * (boolean) - whether to enable zeppelin Interpreter. Product default value will be used in
   * case not provided.
   */
  public static Long zeppelinInterpreter;

  /**
   * (boolean) - whether to enable Java Flight Recorder (JFR) for collecting diagnostic and
   * profiling data while launching server and lead members in cluster. Defaults to false if not
   * provided.
   */
  public static Long enableFlightRecorder;

  /**
   * (boolean) - whether to enable GC options while launching server and lead members in cluster.
   * Defaults to false if not provided.
   */
  public static Long enableGCFlags;

  /**
   * (String) log level to be applied while generating logs for snappy members.
   * Defaults to config if not provided.
   */
  public static Long logLevel;

  /**
   * (String) userAppJar containing the user snappy job class. The wildcards in jar file name are
   * supported in order to removes the hard coding of jar version.
   * e.g. user can specify the jar file name as "snappydata-store-scala-tests*tests.jar" instead of
   * full jar name as "snappydata-store-scala-tests-0.1.0-SNAPSHOT-tests.jar".
   */
  public static Long userAppJar;

  /**
   * (String) AppName for the user app jar containing snappy job class.
   */
  public static Long userAppName;

  /**
   * (String) A unique identifier for the JAR installation. The identifier you provide must
   * specify a schema name delimiter. For example: APP.myjar.
   */
  public static Long jarIdentifier;

  /**
   * (String) args to be passed to the Spark App
   */
  public static Long userAppArgs;

  /**
   * (int) how long (milliseconds) it should wait for getting the job status
   */
  public static Long streamingJobExecutionTimeInMillis;

  /**
   * (int) how long (milliseconds) it should wait before Cycle VMs again
   */
  public static Long waitTimeBeforeNextCycleVM;

  /**
   * (int) how long (milliseconds) it should wait before retrieving snappy-job status
   */
  public static Long sleepTimeSecsForJobStatus;

  /**
   * (int) how long (seconds) it should wait before retrieving server status
   */
  public static Long sleepTimeSecsForMemberStatus;

  /**
   * (int) Number of times the test should retry submitting failed job in case of lead node failover.
   */
  public static Long numTimesToRetry;

  /**
   * (int) The number of VMs to stop (then restart) at a time.
   */
  public static Long numVMsToStop;

  /**
   * (int) The number of lead VMs to stop (then restart).
   */
  public static Long numLeadsToStop;

  /**
   * Parameter used to get the user APP_PROPS for snappy job.
   * (VectosetValues of Strings) A list of values for snappy-job.
   */
  public static Long appPropsForJobServer;

  /**
   * Parameter used to get the user list of pointLookUP queries to execute concurrently using
   * jdbc clients.
   * (VectorsetValues of Strings) A list of values for pointLookUp queries.
   */
  public static Long pointLookUpQueryList;

  /**
   * Parameter used to get the user list of analytical queries to execute concurrently using
   * jdbc clients.
   * (VectorsetValues of Strings) A list of values for analytical queries.
   */
  public static Long analyticalQueryList;


  /**
   * Parameter used to get the leaderLauncher properties specified by user while launching
   * the lead node.
   * (VectosetValues of Strings) A space seperated list of values for leaderLauncher properties.
   */
  public static Long leaderLauncherProps;

  /**
   * Parameter used to get the serverLauncher properties specified by user while launching
   * the dataStore node.
   * (VectosetValues of Strings) A space seperated list of values for serverLauncher properties.
   */
  public static Long serverLauncherProps;

  /**
   * Parameter used to get the locatorLauncher properties specified by user while launching
   * the locator node.
   * (VectosetValues of Strings) A space seperated list of values for locatorLauncher properties.
   */
  public static Long locatorLauncherProps;

  /**
   * Parameter used to get the list of parameters specified by user for spark-submit
   * (VectosetValues of Strings) A space seperated list of values of parameters for spark-submit.
   */
  public static Long sparkSubmitExtraPrms;

  /**
   * (int) number of executor cores to be used in test
   */
  public static Long executorCores;

  /**
   * (String) Maximun Result Size for Driver. Product default value will be used in case not
   * provided.
   */
  public static Long driverMaxResultSize;

  /**
   * (String) Local Memory. Defaults to 1GB if not provided.
   */
  public static Long locatorMemory;

  /**
   * (String) Memory to be used while starting the Server process. Defaults to 1GB if not provided.
   */
  public static Long serverMemory;

  /**
   * (String) criticalHeapPercentage to be used while starting the Server process. Defaults to 90%
   * if not provided.
   */
  public static Long criticalHeapPercentage;

  /**
   * (String) evictionHeapPercentage to be used while starting the Server process. Defaults to 90%
   * of critical-heap-percentage if not provided.
   */
  public static Long evictionHeapPercentage;

  /**
   * (String) Memory to be used while starting the Lead process. Defaults to 1GB if not provided.
   */
  public static Long leadMemory;

  /**
   * (String) sparkSchedulerMode. Product default value will be used in case not provided.
   */
  public static Long sparkSchedulerMode;

  /**
   * (int) sparkSqlBroadcastJoinThreshold
   */
  public static Long sparkSqlBroadcastJoinThreshold;

  /**
   * (boolean) - whether in-memory Columnar store needs to be compressed . Defaults to false if not
   * provided.
   */
  public static Long compressedInMemoryColumnarStorage;

  /**
   * (long) columnBatchSize. Product default value will be used in case not provided
   */
  public static Long columnBatchSize;

  /**
   * (boolean) - whether to use conserveSockets. Product default value will be used in case not
   * provided.
   */
  public static Long conserveSockets;

  /**
   * (int) number of BootStrap trials to be used in test.
   */
  public static Long numBootStrapTrials;

  /**
   * (int) number of shuffle partitions to be used in test
   */
  public static Long shufflePartitions;

  /**
   * (String) path for kafka directory
   */
  public static Long kafkaDir;

  /**
   * (String) snappy-poc jar path
   */
  public static Long snappyPocJarPath;

  /**
   * (String) log file name where the output of task(snappy-shell output/snappyJob/sparkApp) to
   * be written
   */
  public static Long logFileName;

  /**
   * kafka topic name
   */
  public static Long kafkaTopic;

  /**
   * (String) Memory to be used for spark executor while executing spark-submit. Defaults to
   * 1GB if not provided.
   */
  public static Long executorMemory;

  /**
   * (Boolean) parameter to have dynamic APP_PROPS, other than setting using taskTab.
   */
  public static Long hasDynamicAppProps;

  /**
   * (Boolean) parameter to enable security for snappyJob,by default it is false.
   */
  public static Long isSecurity;

  /**
   * (String) User credentials that will be used when submittimg a snappyJob to a secure cluster
   */
  public static Long credentialFile;

  /**
   * Parameter used to get the user specified table List required for validation.
   * (VectorsetValues of Strings) A list of values for table List
   */
  public static Long tableList;

  /**
   * Parameter used to get the user specified index List required for validation.
   * (VectorsetValues of Strings) A list of values for index List
   */
  public static Long indexList;

  /**
   * Parameter used to get the user specified List of connetcion properties and set them on the
   * jdbc connection.
   * (VectorsetValues of Strings) A list of values for connetcion properties list
   */
  public static Long connPropsList;

  /**
   * Parameter used to get the number of Rows in each table provided in table List. This is
   * required for validating recovery after cluster restart.
   * (VectorsetValues of Strings) A list of values for number of rows in each table in table list
   */
  public static Long numRowsList;


  public static String getCredentialFile() {
    Long key = credentialFile;
    return BasePrms.tasktab().stringAt(key, BasePrms.tab().stringAt(key, null));
  }


  public static boolean isSecurityOn() {
    Long key = isSecurity;
    return tasktab().booleanAt(key, tab().booleanAt(key, false));
  }

  public static int getRetryCountForJob() {
    Long key = numTimesToRetry;
    return tasktab().intAt(key, tab().intAt(key, 5));
  }

  public static int getSleepTimeSecsForJobStatus() {
    Long key = sleepTimeSecsForJobStatus;
    return tasktab().intAt(key, tab().intAt(key, 120));
  }

  public static int getSleepTimeSecsForMemberStatus() {
    Long key = sleepTimeSecsForMemberStatus;
    return tasktab().intAt(key, tab().intAt(key, 30));
  }

  public static String getExecutorCores() {
    String numExecutorCores = tasktab().stringAt(executorCores, tab().stringAt(executorCores,
        null));
    if (numExecutorCores == null) return "";
    String sparkExecutorCores = " -spark.executor.cores=" + numExecutorCores;
    return sparkExecutorCores;
  }

  public static String getDriverMaxResultSize() {
    String maxResultSize = tasktab().stringAt(driverMaxResultSize, tab().stringAt
        (driverMaxResultSize, null));
    if (maxResultSize == null) return "";
    String sparkDriverMaxResultSize = " -spark.driver.maxResultSize=" + maxResultSize;
    return sparkDriverMaxResultSize;
  }

  public static String getLocatorMemory() {
    Long key = locatorMemory;
    String locatorHeapSize = BasePrms.tasktab().stringAt(key, BasePrms.tab().stringAt(key,
        null));
    if (locatorHeapSize == null) return "";
    locatorHeapSize = " -heap-size=" + locatorHeapSize;
    return locatorHeapSize;
  }

  public static String getServerMemory() {
    Long key = serverMemory;
    String serverHeapSize = BasePrms.tasktab().stringAt(key, BasePrms.tab().stringAt(key,
        null));
    if (serverHeapSize == null) return "";
    serverHeapSize = " -heap-size=" + serverHeapSize;
    return serverHeapSize;
  }

  public static String getCriticalHeapPercentage() {
    String criticalHeapPercentageString = " -critical-heap-percentage=" + tab().stringAt
        (criticalHeapPercentage, "90");
    return criticalHeapPercentageString;
  }

  public static String calculateDefaultEvictionPercentage() {
    int criticalHeapPercent = Integer.parseInt(tab().stringAt(criticalHeapPercentage, "90"));
    int evictionHeapPercent = (criticalHeapPercent * 90) / 100;
    String evictionHeapPercentString = String.valueOf(evictionHeapPercent);
    return evictionHeapPercentString;
  }

  public static String getEvictionHeapPercentage() {
    String evictionHeapPercentageString = " -eviction-heap-percentage=" + tab().stringAt
        (evictionHeapPercentage, calculateDefaultEvictionPercentage());
    return evictionHeapPercentageString;
  }

  public static String getLeadMemory() {
    Long key = leadMemory;
    String leadHeapSize = BasePrms.tasktab().stringAt(key, BasePrms.tab().stringAt(key,
        null));
    if (leadHeapSize == null) return "";
    leadHeapSize = " -heap-size=" + leadHeapSize;
    return leadHeapSize;
  }

  public static String getSparkSchedulerMode() {
    String schedulerMode = tasktab().stringAt(sparkSchedulerMode, tab().stringAt
        (sparkSchedulerMode, null));
    if (schedulerMode == null) return "";
    String sparkSchedulerMode = " -spark.scheduler.mode=" + schedulerMode;
    return sparkSchedulerMode;
  }

  public static String getSparkSqlBroadcastJoinThreshold() {
    String broadcastJoinThreshold = tasktab().stringAt(sparkSqlBroadcastJoinThreshold, tab()
        .stringAt(sparkSqlBroadcastJoinThreshold, null));
    if (broadcastJoinThreshold == null) return "";
    String sparkSqlBroadcastJoinThreshold = " -spark.sql.autoBroadcastJoinThreshold=" +
        broadcastJoinThreshold;
    return sparkSqlBroadcastJoinThreshold;
  }

  public static boolean getCompressedInMemoryColumnarStorage() {
    Long key = compressedInMemoryColumnarStorage;
    return tasktab().booleanAt(key, tab().booleanAt(key, false));
  }

  public static String getColumnBatchSize() {
    String snappyColumnBatchSize = tasktab().stringAt(columnBatchSize, tab().stringAt
        (columnBatchSize, null));
    if (snappyColumnBatchSize == null) return "";
    String columnBatchSize = " -snappydata.column.batchSize=" + snappyColumnBatchSize;
    return columnBatchSize;
  }

  public static String getConserveSockets() {
    String isConserveSockets = tasktab().stringAt(conserveSockets, tab().stringAt
        (conserveSockets, null));
    if (isConserveSockets == null) return "";
    String conserveSockets = " -conserve-sockets=" + isConserveSockets;
    return conserveSockets;
  }

  public static int getShufflePartitions() {
    Long key = shufflePartitions;
    return tasktab().intAt(key, tab().intAt(key, 200));
  }

  public static String getCommaSepAPPProps() {
    Long key = appPropsForJobServer;
    return BasePrms.tasktab().stringAt(key, BasePrms.tab().stringAt(key, null));
  }

  public static Vector getPointLookUpQueryList() {
    Long key = pointLookUpQueryList;
    return BasePrms.tasktab().vecAt(key, BasePrms.tab().vecAt(key, new HydraVector()));
  }

  public static Vector getAnalyticalQueryList() {
    Long key = analyticalQueryList;
    return BasePrms.tasktab().vecAt(key, BasePrms.tab().vecAt(key, new HydraVector()));
  }

  public static String getLeaderLauncherProps() {
    Long key = leaderLauncherProps;
    String leaderLauncherPropList = BasePrms.tasktab().stringAt(key, BasePrms.tab().stringAt(key,
        null));
    if (leaderLauncherPropList == null) return "";
    else return leaderLauncherPropList;
  }

  public static String getServerLauncherProps() {
    Long key = serverLauncherProps;
    String serverLauncherPropList = BasePrms.tasktab().stringAt(key, BasePrms.tab().stringAt(key,
        null));
    if (serverLauncherPropList == null) return "";
    else return serverLauncherPropList;
  }

  public static String getLocatorLauncherProps() {
    Long key = locatorLauncherProps;
    String locatorLauncherPropList = BasePrms.tasktab().stringAt(key, BasePrms.tab().stringAt(key,
        null));
    if (locatorLauncherPropList == null) return "";
    else return locatorLauncherPropList;
  }

  public static String getSparkSubmitExtraPrms() {
    Long key = sparkSubmitExtraPrms;
    String sparkSubmitExtraPrmList = BasePrms.tasktab().stringAt(key, BasePrms.tab().stringAt(key,
        null));
    if (sparkSubmitExtraPrmList == null) return "";
    else return sparkSubmitExtraPrmList;
  }

  public static boolean getTableDefaultDataPolicy() {
    Long key = tableDefaultPartitioned;
    return tasktab().booleanAt(key, tab().booleanAt(key, false));
  }

  public static String getPersistIndexes() {
    Long key = persistIndexes;
    boolean persistIndexesFlag = tasktab().booleanAt(key, tab().booleanAt(key, true));
    String persistIndexes = " -J-Dgemfirexd.persist-indexes=" + persistIndexesFlag;
    return persistIndexes;
  }

  public static String getTimeStatistics() {
    boolean enableTimeStats = tasktab().booleanAt(enableTimeStatistics, tab().booleanAt
        (enableTimeStatistics, true));
    String timeStatistics = " -enable-time-statistics=" + enableTimeStats + " " +
        "-statistic-archive-file=statArchive.gfs";
    return timeStatistics;
  }

  public static String getClosedFormEstimates() {
    String enableClosedFormEstimates = tasktab().stringAt(closedFormEstimates, tab().stringAt
        (closedFormEstimates, null));
    if (enableClosedFormEstimates == null) return "";
    String closedFormEstimates = " -spark.sql.aqp.closedFormEstimates=" + enableClosedFormEstimates;
    return closedFormEstimates;
  }

  public static String getZeppelinInterpreter() {
    String enableZeppelinInterpreter = tasktab().stringAt(zeppelinInterpreter, tab().stringAt
        (zeppelinInterpreter, null));
    if (enableZeppelinInterpreter == null) return "";
    String zeppelinInterpreter = " -zeppelin.interpreter.enable=" + enableZeppelinInterpreter;
    return zeppelinInterpreter;
  }

  public static String getFlightRecorderOptions(String dirPath) {
    boolean flightRecorder = tasktab().booleanAt(enableFlightRecorder, tab().booleanAt
        (enableFlightRecorder, false));
    if (flightRecorder) {
      String flightRecorderOptions = " -J-XX:+UnlockCommercialFeatures -J-XX:+FlightRecorder  " +
          "-J-XX:FlightRecorderOptions=defaultrecording=true,disk=true,repository=" +
          dirPath + ",maxage=6h,dumponexit=true,dumponexitpath=flightRecorder.jfr";
      return flightRecorderOptions;
    } else return "";
  }

  public static String getGCOptions(String dirPath) {
    boolean gcFlags = tasktab().booleanAt(enableGCFlags, tab().booleanAt(enableGCFlags, false));
    if (gcFlags) {
      String gcOptions = " -J-verbose:gc -J-Xloggc:" + dirPath + "/gc.out -J-XX:+PrintGCDetails " +
          " -J-XX:+PrintGCTimeStamps  -J-XX:+PrintGCDateStamps";
      return gcOptions;
    } else return "";
  }

  public static String getNumBootStrapTrials() {
    String bootStrapTrials = tasktab().stringAt(numBootStrapTrials, tab().stringAt
        (numBootStrapTrials, null));
    if (bootStrapTrials == null) return "";
    String numBootStrapTrials = " -spark.sql.aqp.numBootStrapTrials=" + bootStrapTrials;
    return numBootStrapTrials;
  }

  public static String getLogLevel() {
    String snappyLogLevel = " -log-level=" + tab().stringAt(logLevel, "config");
    return snappyLogLevel;
  }

  public static Vector getSQLScriptNames() {
    Long key = sqlScriptNames;
    return BasePrms.tasktab().vecAt(key, BasePrms.tab().vecAt(key, null));
  }

  public static String getUserAppJar() {
    Long key = userAppJar;
    return BasePrms.tasktab().stringAt(key, BasePrms.tab().stringAt(key, null));
  }

  public static String getUserAppName() {
    Long key = userAppName;
    return BasePrms.tasktab().stringAt(key, BasePrms.tab().stringAt(key, "myApp"));
  }

  public static String getJarIdentifier() {
    Long key = jarIdentifier;
    return BasePrms.tasktab().stringAt(key, BasePrms.tab().stringAt(key, null));
  }

  public static String getUserAppArgs() {
    Long key = userAppArgs;
    return BasePrms.tasktab().stringAt(key, BasePrms.tab().stringAt(key, " "));
  }

  public static Vector getDataLocationList() {
    Long key = dataLocation;
    return BasePrms.tasktab().vecAt(key, BasePrms.tab().vecAt(key, new HydraVector()));
  }

  public static Vector getPersistenceModeList() {
    Long key = persistenceMode;
    return BasePrms.tasktab().vecAt(key, BasePrms.tab().vecAt(key, new HydraVector()));
  }

  public static Vector getColocateWithOptionList() {
    Long key = colocateWith;
    return BasePrms.tasktab().vecAt(key, BasePrms.tab().vecAt(key, new HydraVector()));
  }

  public static Vector getPartitionByOptionList() {
    Long key = partitionBy;
    return BasePrms.tasktab().vecAt(key, BasePrms.tab().vecAt(key, new HydraVector()));
  }

  public static Vector getNumPartitionsList() {
    Long key = numPartitions;
    return BasePrms.tasktab().vecAt(key, BasePrms.tab().vecAt(key, new HydraVector()));
  }

  public static Vector getRedundancyOptionList() {
    Long key = redundancy;
    return BasePrms.tasktab().vecAt(key, BasePrms.tab().vecAt(key, new HydraVector()));
  }

  public static Vector getRecoverDelayOptionList() {
    Long key = recoverDelay;
    return BasePrms.tasktab().vecAt(key, BasePrms.tab().vecAt(key, new HydraVector()));
  }

  public static Vector getMaxPartitionSizeList() {
    Long key = maxPartitionSize;
    return BasePrms.tasktab().vecAt(key, BasePrms.tab().vecAt(key, new HydraVector()));
  }

  public static Vector getEvictionByOptionList() {
    Long key = evictionBy;
    return BasePrms.tasktab().vecAt(key, BasePrms.tab().vecAt(key, new HydraVector()));
  }

  public static Vector getSnappyJobClassNames() {
    Long key = jobClassNames;
    return BasePrms.tasktab().vecAt(key, BasePrms.tab().vecAt(key, null));
  }

  public static Vector getSparkJobClassNames() {
    Long key = sparkJobClassNames;
    return BasePrms.tasktab().vecAt(key, BasePrms.tab().vecAt(key, null));
  }

  public static Vector getSnappyStreamingJobClassNames() {
    Long key = streamingJobClassNames;
    return BasePrms.tasktab().vecAt(key, BasePrms.tab().vecAt(key, null));
  }

  public static String getExecutorMemory() {
    Long key = executorMemory;
    String executorMem;
    String heapSize = tasktab().stringAt(key, BasePrms.tab().stringAt(key, null));
    if (heapSize == null) {
      int cores = Runtime.getRuntime().availableProcessors();
      long defaultMem;
      defaultMem = ((cores * 64) + 1024);
      executorMem = " --executor-memory " + defaultMem + "m";
      return executorMem;
    }
    executorMem = " --executor-memory " + heapSize;
    return executorMem;
  }

  public static boolean hasDynamicAppProps() {
    Long key = hasDynamicAppProps;
    return tasktab().booleanAt(key, tab().booleanAt(key, false));
  }

  public static Vector getKafkaTopic() {
    Long key = kafkaTopic;
    return BasePrms.tasktab().vecAt(key, BasePrms.tab().vecAt(key, null));
  }

  public static Vector getTableList() {
    Long key = tableList;
    return BasePrms.tasktab().vecAt(key, BasePrms.tab().vecAt(key, new HydraVector()));
  }

  public static Vector getNumRowsList() {
    Long key = numRowsList;
    return BasePrms.tasktab().vecAt(key, BasePrms.tab().vecAt(key, new HydraVector()));
  }

  public static Vector getIndexList() {
    Long key = indexList;
    return BasePrms.tasktab().vecAt(key, BasePrms.tab().vecAt(key, new HydraVector()));
  }

  public static Vector getConnPropsList() {
    Long key = connPropsList;
    return BasePrms.tasktab().vecAt(key, BasePrms.tab().vecAt(key, new HydraVector()));
  }

  static {
    BasePrms.setValues(SnappyPrms.class);
  }

  public static void main(String args[]) {
    BasePrms.dumpKeys();
  }

}
