/*

   Derby - Class com.pivotal.gemfirexd.internal.jdbc.EmbedXAConnection

   Licensed to the Apache Software Foundation (ASF) under one or more
   contributor license agreements.  See the NOTICE file distributed with
   this work for additional information regarding copyright ownership.
   The ASF licenses this file to You under the Apache License, Version 2.0
   (the "License"); you may not use this file except in compliance with
   the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.

 */

/*
 * Changes for GemFireXD distributed data platform (some marked by "GemStone changes")
 *
 * Portions Copyright (c) 2010-2015 Pivotal Software, Inc. All rights reserved.
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

package com.pivotal.gemfirexd.internal.jdbc;


import com.pivotal.gemfirexd.internal.iapi.jdbc.EngineConnection;
import com.pivotal.gemfirexd.internal.iapi.jdbc.ResourceAdapter;
import com.pivotal.gemfirexd.internal.iapi.reference.SQLState;
import com.pivotal.gemfirexd.internal.impl.jdbc.Util;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.PreparedStatement;
import java.sql.CallableStatement;
import javax.transaction.xa.XAResource;

/** -- jdbc 2.0. extension -- */
import javax.sql.XAConnection;

/** 
 */
public class EmbedXAConnection extends EmbedPooledConnection
		implements XAConnection

{

        private EmbedXAResource xaRes;

	EmbedXAConnection(EmbeddedDataSource ds, ResourceAdapter ra, String u, String p, boolean requestPassword) throws SQLException
	{
		super(ds, u, p, requestPassword);
                xaRes = new EmbedXAResource (this, ra);
	}

    /**
     * Check if this connection is part of a global XA transaction.
     *
     * @return {@code true} if the transaction is global, {@code false} if the
     * transaction is local
     */
    private boolean isGlobal() {
        return xaRes.getCurrentXid () != null;
    }

	/*
	** XAConnection methods
	*/

	public final synchronized XAResource getXAResource() throws SQLException {
		checkActive();
		return xaRes;
	}

	/*
	** BrokeredConnectionControl api
	*/
	/**
		Allow control over setting auto commit mode.
	*/
	public void checkAutoCommit(boolean autoCommit) throws SQLException {
		if (autoCommit && isGlobal())
			throw Util.generateCsSQLException(SQLState.CANNOT_AUTOCOMMIT_XA);

		super.checkAutoCommit(autoCommit);
	}
	/**
		Are held cursors allowed. If the connection is attached to
        a global transaction then downgrade the result set holdabilty
        to CLOSE_CURSORS_AT_COMMIT if downgrade is true, otherwise
        throw an exception.
        If the connection is in a local transaction then the
        passed in holdabilty is returned.
	*/
	public int  checkHoldCursors(int holdability, boolean downgrade)
        throws SQLException
    {
		if (holdability == ResultSet.HOLD_CURSORS_OVER_COMMIT) {		
			if (isGlobal()) {
                if (!downgrade)
                    throw Util.generateCsSQLException(SQLState.CANNOT_HOLD_CURSOR_XA);
                
                holdability = ResultSet.CLOSE_CURSORS_AT_COMMIT;
            }
		}

		return super.checkHoldCursors(holdability, downgrade);
	}

	/**
		Allow control over creating a Savepoint (JDBC 3.0)
	*/
	public void checkSavepoint() throws SQLException {

		if (isGlobal())
			throw Util.generateCsSQLException(SQLState.CANNOT_ROLLBACK_XA);

		super.checkSavepoint();
	}

	/**
		Allow control over calling rollback.
	*/
	public void checkRollback() throws SQLException {

		if (isGlobal())
			throw Util.generateCsSQLException(SQLState.CANNOT_ROLLBACK_XA);

		super.checkRollback();
	}
	/**
		Allow control over calling commit.
	*/
	public void checkCommit() throws SQLException {

		if (isGlobal())
			throw Util.generateCsSQLException(SQLState.CANNOT_COMMIT_XA);

		super.checkCommit();
	}

    /**
     * @see com.pivotal.gemfirexd.internal.iapi.jdbc.BrokeredConnectionControl#checkClose()
     */
    public void checkClose() throws SQLException {
        if (isGlobal()) {
            // It is always OK to close a connection associated with a global
            // XA transaction, even if it isn't idle, since we still can commit
            // or roll back the global transaction with the XAResource after
            // the connection has been closed.
        } else {
            super.checkClose();
        }
    }

	public Connection getConnection() throws SQLException
	{
		Connection handle;

		// Is this just a local transaction?
		if (!isGlobal()) {
			handle = super.getConnection();
		} else {

			if (currentConnectionHandle != null) {
				// this can only happen if someone called start(Xid),
				// getConnection, getConnection (and we are now the 2nd
				// getConnection call).
				// Cannot yank a global connection away like, I don't think... 
				throw Util.generateCsSQLException(
// GemStone changes BEGIN
				    // should be 25001 (DERBY-1169)
				    SQLState.CANNOT_CLOSE_ACTIVE_CONNECTION);
				    /* (original code)
							   SQLState.CANNOT_CLOSE_ACTIVE_XA_CONNECTION);
				    */
// GemStone changes END
			}

			handle = getNewCurrentConnectionHandle();
		}

		currentConnectionHandle.syncState();

		// Gemstone changes BEGIN
		handle.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
		// Gemstone changes END
		return handle;
	}

	/**
		Wrap and control a Statement
	*/
	public Statement wrapStatement(Statement s) throws SQLException {
		XAStatementControl sc = new XAStatementControl(this, s);
		return sc.applicationStatement;
	}
	/**
		Wrap and control a PreparedStatement
	*/
	public PreparedStatement wrapStatement(PreparedStatement ps, String sql, Object generatedKeys) throws SQLException {
                ps = super.wrapStatement(ps,sql,generatedKeys);
		XAStatementControl sc = new XAStatementControl(this, ps, sql, generatedKeys);
		return (PreparedStatement) sc.applicationStatement;
	}
	/**
		Wrap and control a PreparedStatement
	*/
	public CallableStatement wrapStatement(CallableStatement cs, String sql) throws SQLException {
                cs = super.wrapStatement(cs,sql);
		XAStatementControl sc = new XAStatementControl(this, cs, sql);
		return (CallableStatement) sc.applicationStatement;
	}

	/**
		Override getRealConnection to create a a local connection
		when we are not associated with an XA transaction.

		This can occur if the application has a Connection object (conn)
		and the following sequence occurs.

		conn = xac.getConnection();
		xac.start(xid, ...)
		
		// do work with conn

		xac.end(xid, ...);

		// do local work with conn
		// need to create new connection here.
	*/
	public EngineConnection getRealConnection() throws SQLException
	{
        EngineConnection rc = super.getRealConnection();
		if (rc != null)
			return rc;

		openRealConnection();

		// a new Connection, set its state according to the application's Connection handle
		currentConnectionHandle.setState(true);

		return realConnection;
	}
}
