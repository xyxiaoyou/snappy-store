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
/*
 * IndependentOperandsInWhereClause.java
 *
 * Created on June 23, 2005, 4:24 PM
 */

package com.gemstone.gemfire.cache.query.functional;

import java.util.ArrayList;
import com.gemstone.gemfire.cache.Region;
import java.util.Collection;
import junit.framework.*;
import com.gemstone.gemfire.cache.query.*;
//import com.gemstone.gemfire.cache.query.types.StructType;
import com.gemstone.gemfire.cache.query.data.Portfolio;
import com.gemstone.gemfire.cache.query.internal.QueryObserverAdapter;
import com.gemstone.gemfire.cache.query.internal.QueryObserverHolder;
//import java.util.*;
//import com.gemstone.gemfire.cache.query.data.Position;

/**
 *
 * @author kdeshpan
 */
public class IndependentOperandsInWhereClause2Test extends TestCase {
    
    /** Creates a new instance of IndependentOperandsInWhereClause */
    public IndependentOperandsInWhereClause2Test(String testName) {
        super(testName);
    }
    
    
    protected void setUp() throws Exception {
        CacheUtils.startCache();
    }
    
    protected void tearDown() throws Exception {
        CacheUtils.closeCache();
    }
    
    public void testIndependentOperands() throws Exception {
        Region region = CacheUtils.createRegion("portfolios", Portfolio.class);
        for (int i = 0; i < 4; i++) {
            region.put("" + i, new Portfolio(i));
        }
        QueryService qs = CacheUtils.getQueryService();
        String queries[] = {
         /*1*/  "select distinct * from /portfolios pf, positions.values pos where  (pf.ID > 1 or status = 'active') or (true AND pos.secId ='IBM')",// size 6 noi 3
         /*2*/  "Select distinct structset.sos, structset.key " +
                "from /portfolios pfos, pfos.positions.values outerPos, " +
                "(SELECT DISTINCT key: key, sos: pos.sharesOutstanding "+
                "from /portfolios.entries pf, pf.value.positions.values pos " +
                "where outerPos.secId != 'IBM' AND " +
                "pf.key IN (select distinct * from pf.value.collectionHolderMap['0'].arr)) structset " +
                "where structset.sos > 2000",// size 6 numof indexes 0.
         /*3*/  "Select distinct * " +
                "from /portfolios pfos, pfos.positions.values outerPos, " +
                "(SELECT DISTINCT key: key, sos: pos.sharesOutstanding "+
                "from /portfolios.entries pf, pf.value.positions.values pos " +
                "where outerPos.secId != 'IBM' AND " +
                "pf.key IN (select distinct * from pf.value.collectionHolderMap['0'].arr)) structset " +
                "where structset.sos > 2000",// size 42 numof indexes 0.
         /*4*/  "select distinct * from /portfolios pf where true and ID = 0 ",//size 1, noi 1
         /*5*/  "select distinct * from /portfolios pf, positions.values pos where true = true and pos.secId ='IBM'",//size 1 noi 1
         /*6*/  "select distinct * from /portfolios pf, positions.values pos where false and pos.secId ='IBM'",//size 0 noi
         /*7*/  "select distinct * from /portfolios pf where true or ID = 0 ",//size 4
         /*8*/  "select distinct * from /portfolios pf, positions.values pos  where true = true and pf.ID > 1 and pos.secId ='IBM'",//size 0
         /*9*/  "select distinct * from /portfolios pf, positions.values pos where true = false and pf.ID > 1 and pos.secId ='IBM'",//size 0
         /*10*/ "select distinct * from /portfolios pf, positions.values pos where true = true and pf.ID > 1 or pos.secId ='IBM'",//size 5
         /*11*/ "select distinct * from /portfolios pf, positions.values pos where true = false and pf.ID > 1 or pos.secId ='IBM'",//size 1
         /*12*/ "select distinct * from /portfolios pf, positions.values pos where  (true AND pos.secId ='SUN') or (pf.ID > 1 and status != 'active')",//size 2
         /*13*/ "select distinct * from /portfolios pf, positions.values pos  where  (pf.ID > 1 or status = 'active') or (false AND pos.secId ='IBM')",//size 6
         /*14*/ "SELECT DISTINCT * FROM /portfolios pf, pf.positions.values position "+
         "WHERE true = null OR position.secId = 'SUN'", // size 1
         /*15*/ "SELECT DISTINCT * FROM /portfolios pf, pf.positions.values position "+
         "WHERE (true = null OR position.secId = 'SUN') AND true", //size 1
         /*16*/ "SELECT DISTINCT * FROM /portfolios pf, pf.positions.values position "+
         "WHERE (ID > 0 OR position.secId = 'SUN') OR false",    //size 6
         /*17*/ "SELECT DISTINCT * FROM /portfolios pf, pf.positions.values position "+
         "WHERE (true = null OR position.secId = 'SUN') OR true",         //size 8
         /*18*/ "SELECT DISTINCT * FROM /portfolios pf, pf.positions.values position "+
         "WHERE (true = null OR position.secId = 'SUN') OR false",      //size 1
         /*19*/ "select distinct * from /portfolios pf, positions.values pos " +
         "where (pf.ID < 1 and status = 'active') and (false or pos.secId = 'IBM')",// size 1
         /*20*/ "select distinct * from /portfolios pf where false and ID = 0 ",// size 0
         /*21*/ "select distinct * from /portfolios pf where false or ID = 0 ",// size 1
         /*22*/ "select distinct * from /portfolios pf where ID = 0 and false  ",// size 0
         /*23*/ "select distinct * from /portfolios pf where ID = 0 or false" ,// size 1
         /*24*/ "select distinct * from /portfolios pf, positions.values pos " +
         "where (ID = 2 and true) and (status = 'active' or (pos.secId != 'IBM' and true))  ",// size 2
         /*25*/ "select distinct * from /portfolios pf, positions.values pos " +
         "where (ID = 2 or false) or (status = 'active' and (pos.secId != 'IBM' or true))  ",// size 2
         /*26*/ "SELECT DISTINCT * FROM /portfolios pf,"
         + " (SELECT DISTINCT * FROM /portfolios ptf, ptf.positions pos where pf.ID != 1 and pos.value.sharesOutstanding > 2000) as x"
         + " WHERE pos.value.secId = 'IBM'", //size 0
         /*27*/ "SELECT DISTINCT * FROM /portfolios pf,"
         + " (SELECT DISTINCT * FROM /portfolios ptf, ptf.positions pos where pf.ID != 1 and pos.value.sharesOutstanding > 2000)as y"
         + " WHERE pos.value.secId = 'HP'", //size 3
        };
        
        int sizeOfResult[] = {6,6,42,1,1,0,4,0,0,5,1,2,6,1,1,6,8,1,1,0,1,0,1,2,4,0,3};
        SelectResults sr[][]= new SelectResults[queries.length][2];
        for (int i = 0; i < queries.length; i++) {
            Query q = null;
            q = CacheUtils.getQueryService().newQuery(queries[i]);
            QueryObserverImpl observer = new QueryObserverImpl();
            QueryObserverHolder.setInstance(observer);
            
            Object r = q.execute();
            sr[i][0] = (SelectResults) r;
            if (!observer.isIndexesUsed) {
                System.out.println("NO INDEX USED");
            }
            System.out.println(Utils.printResult(r));
            if(((Collection)r).size() != sizeOfResult[i]) {
                fail("SIZE NOT as expected for QUery no :" + (i+1));
            }
        }
        
        //Create an Index and Run the Same Query as above.
        //        Index index1 = (Index) qs.createIndex("secIdIndex", IndexType.FUNCTIONAL,
        //                "b.secId", "/portfolios.entries pf, pf.value.positions.values b");
        qs.createIndex("secIdIdx", IndexType.FUNCTIONAL,
                "b.secId", "/portfolios pf, pf.positions.values b");
        qs.createIndex("IdIdx", IndexType.FUNCTIONAL,
                "pf.ID", "/portfolios pf, pf.positions.values b");
        qs.createIndex("statusIdx", IndexType.FUNCTIONAL,
                "pf.status", "/portfolios pf, pf.positions.values b");
        qs.createIndex("Idindex2", IndexType.FUNCTIONAL, "pf.ID","/portfolios pf");
        for (int j = 0; j < queries.length; j++) {
            Query q2 = null;
            q2 = CacheUtils.getQueryService().newQuery(queries[j]);
            QueryObserverImpl observer2 = new QueryObserverImpl();
            QueryObserverHolder.setInstance(observer2);
            try {
            
                Object r2 = q2.execute();
                sr[j][1] = (SelectResults) r2;
                System.out.println("With Index ="+Utils.printResult(r2));
                if(((Collection)r2).size() != sizeOfResult[j]) {
                    fail("SIZE NOT as expected for QUery no :" + (j+1));
                }
            } catch(Exception e) {
                System.out.println("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!CAUGHT EXCPETION AT QUERY NO: " + (j+1));
                e.printStackTrace();
                fail();
            }
            //        if (observer2.isIndexesUsed == true)
            //            System.out.println("YES,INDEX IS USED!!");
            //        else {
            //            fail("FAILED: Index NOT Used");
            //        }
            //            System.out.println(Utils.printResult(r2));
        }
        CacheUtils.compareResultsOfWithAndWithoutIndex(sr, this);
        
        
    }
    
  public static Test suite(){
    TestSuite suite = new TestSuite(IndependentOperandsInWhereClause2Test.class);
    return suite;
  }//end of suite
  
  ////////// main method ///////////
  public static void main(java.lang.String[] args) {
    junit.textui.TestRunner.run(suite());
  }//end of main method
  
    
    class QueryObserverImpl extends QueryObserverAdapter {
        
        boolean isIndexesUsed = false;
        ArrayList indexesUsed = new ArrayList();
        
        public void beforeIndexLookup(Index index, int oper, Object key) {
            indexesUsed.add(index.getName());
        }
        
        public void afterIndexLookup(Collection results) {
            if (results != null) {
                isIndexesUsed = true;
            }
        }
    }
}