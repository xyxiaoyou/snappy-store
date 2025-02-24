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
package com.gemstone.gemfire.admin.stats;

import com.gemstone.gemfire.Statistics;
import com.gemstone.gemfire.StatisticsType;
import com.gemstone.gemfire.admin.AdminDUnitTestCase;
import com.gemstone.gemfire.admin.SystemMember;
import com.gemstone.gemfire.cache.CacheException;
import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.cache30.CacheSerializableRunnable;
import com.gemstone.gemfire.distributed.internal.DistributionConfig;
import com.gemstone.gemfire.distributed.internal.InternalDistributedSystem;

import java.util.Properties;

import dunit.DistributedTestCase;
import dunit.Host;
import dunit.SerializableRunnable;
import dunit.VM;

/**
 * Tests the functionality of the <code>VMStats</code> and
 * <code>PartitionedRegionStats</code>
 *
 * @author Harsh Khanna
 * @since 5.7
 */
public class GemFireMemberandVMStatsDUnitTest
  extends AdminDUnitTestCase {
//
  ////////  Constructors

  /**
   * Creates a new <code>GemFireMemberStatsDUnitTest</code>
   */
  public GemFireMemberandVMStatsDUnitTest(String name) {
    super(name);
  }
  
  protected SystemMember getMember() throws Exception  {
    getLogWriter().info("[getMember]");

    // Create Member an Cache
    Host host = Host.getHost(0);
    VM vm = host.getVM(0);
    
    final String testName = this.getName();

    vm.invoke(new SerializableRunnable() {
        public void run() {
          Properties props = getDistributedSystemProperties();
          props.setProperty(DistributionConfig.NAME_NAME, testName);
          props.setProperty(DistributionConfig.ENABLE_TIME_STATISTICS_NAME, "true");
          props.setProperty(DistributionConfig.STATISTIC_SAMPLING_ENABLED_NAME, "true");
          getSystem(props);
        }
      });
    getLogWriter().info("Test: Created DS");

    vm.invoke(new CacheSerializableRunnable
        ("Create cache") {
      public void run2() throws CacheException {
        CacheFactory.create(getSystem());
      }
    });
    getLogWriter().info("Test: Created Cache");
    
    Thread.sleep(2000);
    
    SystemMember[] members = this.tcSystem.getSystemMemberApplications();
    if (members.length != 1) {
      StringBuffer sb = new StringBuffer();
      sb.append("Expected 1 member, got " + members.length + ": ");
      for (int i = 0; i < members.length; i++) {
        SystemMember member = members[i];
        sb.append(member.getName());
        sb.append(" ");
      }

      fail(sb.toString());
    }

    getLogWriter().info("Test: Created Member");
    return members[0];
  }

  protected long getCpu() {
    final StatisticsType type = getSystem().findType("VMStats");
    assertNotNull(type);
    
    final Statistics[] stats = getSystem().findStatisticsByType(type);
    assert(stats.length!=0);

    final Statistics vmStats = stats[0];
    return vmStats.getLong("processCpuTime");
  }
 
  public void testDummy(){
} 
  /**
   * Tests Getting CPUTime
   */
  public void _testProcessCPUTime() throws Exception {
    getLogWriter().info("[testProcessCPUTime]");

    // Should be not null 
    // Create Member an Cache
    Host host = Host.getHost(0);
    VM vm = host.getVM(0);

    getMember();
    
//    final String testName = this.getName();
    
    vm.invoke(new SerializableRunnable() {
        public void run() {
          final InternalDistributedSystem s = getSystem();
          final long startTime = getCpu();
          WaitCriterion wc = new WaitCriterion() {
            public boolean done() {
              if (getCpu() != startTime) {
                return true;
              }
              
              // burn CPU
              s.getLogWriter().info("please ignore");
              long fac = 1;
              for (int i = 0; i < 100; i ++) {
                fac = fac * i + s.toString().length();
              }
              return false;
            }
            
            public String description() {
              return "cpu time did not change (is this an intelligent JIT?)";
            }
          };
          DistributedTestCase.waitForCriterion(wc, 60 * 1000, 200, true);
        }
    });
  }
}
