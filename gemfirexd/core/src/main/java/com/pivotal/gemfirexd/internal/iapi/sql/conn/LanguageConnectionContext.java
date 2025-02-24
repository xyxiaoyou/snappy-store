/*

   Derby - Class com.pivotal.gemfirexd.internal.iapi.sql.conn.LanguageConnectionContext

   Licensed to the Apache Software Foundation (ASF) under one or more
   contributor license agreements.  See the NOTICE file distributed with
   this work for additional information regarding copyright ownership.
   The ASF licenses this file to you under the Apache License, Version 2.0
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

package com.pivotal.gemfirexd.internal.iapi.sql.conn;





// GemStone changes BEGIN
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.TransactionFlag;
import com.gemstone.gemfire.internal.cache.Checkpoint;
import com.gemstone.gemfire.internal.cache.TXState;
import com.gemstone.gemfire.internal.cache.wan.GatewaySenderEventCallbackArgument;
import com.gemstone.gnu.trove.THashMap;
import com.pivotal.gemfirexd.internal.engine.access.GemFireTransaction;
import com.pivotal.gemfirexd.internal.engine.access.MemConglomerate;
import com.pivotal.gemfirexd.internal.impl.sql.compile.StatementNode;
// GemStone changes END
import com.pivotal.gemfirexd.internal.catalog.UUID;
import com.pivotal.gemfirexd.internal.iapi.db.Database;
import com.pivotal.gemfirexd.internal.iapi.db.TriggerExecutionContext;
import com.pivotal.gemfirexd.internal.iapi.error.StandardException;
import com.pivotal.gemfirexd.internal.iapi.services.context.Context;
import com.pivotal.gemfirexd.internal.iapi.services.loader.GeneratedClass;
import com.pivotal.gemfirexd.internal.iapi.sql.Activation;
import com.pivotal.gemfirexd.internal.iapi.sql.LanguageFactory;
import com.pivotal.gemfirexd.internal.iapi.sql.ParameterValueSet;
import com.pivotal.gemfirexd.internal.iapi.sql.PreparedStatement;
import com.pivotal.gemfirexd.internal.iapi.sql.Statement;
import com.pivotal.gemfirexd.internal.iapi.sql.compile.CompilerContext;
import com.pivotal.gemfirexd.internal.iapi.sql.compile.OptimizerFactory;
import com.pivotal.gemfirexd.internal.iapi.sql.depend.Provider;
import com.pivotal.gemfirexd.internal.iapi.sql.dictionary.DataDictionary;
import com.pivotal.gemfirexd.internal.iapi.sql.dictionary.SchemaDescriptor;
import com.pivotal.gemfirexd.internal.iapi.sql.dictionary.TableDescriptor;
import com.pivotal.gemfirexd.internal.iapi.sql.dictionary.TriggerDescriptor;
import com.pivotal.gemfirexd.internal.iapi.sql.execute.ConstantAction;
import com.pivotal.gemfirexd.internal.iapi.sql.execute.CursorActivation;
import com.pivotal.gemfirexd.internal.iapi.sql.execute.ExecutionStmtValidator;
import com.pivotal.gemfirexd.internal.iapi.sql.execute.RunTimeStatistics;
import com.pivotal.gemfirexd.internal.iapi.store.access.TransactionController;
import com.pivotal.gemfirexd.internal.iapi.types.DataValueFactory;
import com.pivotal.gemfirexd.internal.iapi.types.RowLocation;
import com.pivotal.gemfirexd.internal.impl.sql.GenericPreparedStatement;
import com.pivotal.gemfirexd.internal.impl.sql.GenericStatement;
import com.pivotal.gemfirexd.internal.impl.sql.StatementStats;
import com.pivotal.gemfirexd.internal.impl.sql.execute.BaseActivation;

import java.util.ArrayList;
import java.util.Collection;
import java.util.EnumSet;
import java.util.Map;
import java.util.Set;

/**
 * LanguageConnectionContext keeps the result sets,
 * and activations in use by the current connection.
 * <p>
 * More stable items, like other factories, are accessible through
 * the LanguageConnectionFactory or the LanguageFactory.
 *
 * @see LanguageConnectionFactory
 * @see com.pivotal.gemfirexd.internal.iapi.sql.LanguageFactory
 */
public interface LanguageConnectionContext extends Context {

	/**
	 * this is the ID we expect these contexts
	 * to be stored into a context manager under.
	 */
	public static final String CONTEXT_ID = com.pivotal.gemfirexd.internal.iapi.reference.ContextId.LANG_CONNECTION;

	public	static	final	int	OUTERMOST_STATEMENT = 1;

    // Constants describing how this connection handles schemas
    public static final int SQL92_SCHEMAS = 0;
    public static final int USER_NAME_SCHEMA = 1; // User names are schema names.
    public static final int NO_SCHEMAS = 2; // Schemas not implemented.

	/* String for logStatementText output */
	public static final String xidStr = "(XID = ";
	public static final String lccStr = "(SESSIONID = ";
	public static final String dbnameStr = "(DATABASE = ";
	public static final String drdaStr = "(DRDAID = ";

	// Lock Management

	public	static	final	int	SINGLE_TRANSACTION_LOCK = 1;
	public	static	final	int	MULTI_TRANSACTION_LOCK = 2;

	// controls casing of NON-delimited identifiers. ANSI casing forces all
	// non-delimited identifiers to be lower case.

	public	static	final	int	UNKNOWN_CASING = -1;
	public	static	final	int	ANSI_CASING = 0;
	public	static	final	int	ANTI_ANSI_CASING = 1;

	/**
	 * Initialize. For use after pushing the contexts that initialization needs.
	 *
	 *
	 * @exception StandardException thrown if something goes wrong
	 */
	void initialize() throws StandardException;

	/**
	 * Get value of logStatementText.
	 * (Whether or not to write info on currently
	 * executing statement to error log.)
	 *
	 * @return value of logStatementText
	 */
	public boolean getLogStatementText();

	/**
	 * Set value of logStatementText
	 * (Whether or not to write info on currently
	 * executing statement to error log.)
	 *
	 * @param logStatementText	Whether or not logStatementText property is set.
	 */
	public void setLogStatementText(boolean logStatementText);

	/**
	 * Get value of logQueryPlan.
	 * (Whether or not to write query plan info on currently
	 * executing statement to error log.)
	 *
	 * @return value of logQueryPlan
	 */
	public boolean getLogQueryPlan();

	/**
	 * get the lock escalation threshold to use with this connection.
	 */
	int getLockEscalationThreshold();

	/**
	 * Add the activation to those known about by this connection.
	 *
	 */
	void addActivation(Activation a)
		throws StandardException;

	/**
	 * Make a note that some activations are marked unused
	 */
	void notifyUnusedActivation();

	/**
	 * Remove the activation from those known about by this connection.
	 *
	 * @exception StandardException thrown if something goes wrong
	 */
	void removeActivation(Activation a)
		throws StandardException;

	/**
	 * Return the number of activation known for this connection.
	 *
	 */
	int getActivationCount();

	/**
	 * See if a given cursor is available for use.  This is used
	 * to locate the cursor during its execution.
	 *
	 * @return the activation for the given cursor, null if none exists.
	 */
	CursorActivation lookupCursorActivation(String cursorName);

	/**
	 * Return the last activation added
	 * This is used to find the drop activation in dropViewCascade
	 * so we can add warning messages to the activation
	 *
	 */
	public Activation getLastActivation();

	/**
		Get a connection unique system generated name for a cursor.
	*/
	public String getUniqueCursorName();

	/**
		Get a connection unique system generated name for an unnamed savepoint.
	*/
	public String getUniqueSavepointName();

	/**
		Get a connection unique system generated id for an unnamed savepoint.
	*/
	public int getUniqueSavepointID();

	/**
	 * Check if there are any global temporary tables declared for this connection.
	 * @return true if there are declared temp tables for this connectoin else false
	 *
	 */
	public boolean checkIfAnyDeclaredGlobalTempTablesForThisConnection();

	/**
	 * Mark the passed temporary table as modified in the current unit of work. That information will be used at rollback time
	 * The compile phase will generate code to call this method if the DML is on a temporary table
	 *
	 * @param tableName Mark the passed temporary table name as modified
	 */
	public void markTempTableAsModifiedInUnitOfWork(String tableName);
  
	/**
	 * Add the declared global temporary table to the list of temporary tables known by this connection.
	 * @param td Corresponding to the temporary table
	 * @param conglomId TODO
	 *
	 */
	public void addDeclaredGlobalTempTable(TableDescriptor td, long conglomId) throws StandardException;

	/**
	 * Drop (mark the declared global temporary table for dropping) from the list of temporary tables known by this connection.
	 * @param tableName look for this table name in the saved list and drop it if found
	 * @return true if dropped the temporary table. False if no such temporary table exists.
	 *
	 * @see com.pivotal.gemfirexd.internal.impl.sql.conn.TempTableInfo
	 */
	public boolean dropDeclaredGlobalTempTable(String tableName);

	/**
	 * Get table descriptor for the declared global temporary table from the list of temporary
	 * tables known by this connection.
	 * @param tableName Get table descriptor for the passed table name
	 * @return TableDescriptor if found the temporary table. Else return null
	 *
	 */
	public TableDescriptor getTableDescriptorForDeclaredGlobalTempTable(String tableName);

	/**
		Reset the connection before it is returned (indirectly) by
		a PooledConnection object. See EmbeddedConnection.
	 */
	public void resetFromPool()
		 throws StandardException;

	/**
		Do a commit, as internally needed by Derby.  E.g.
	 	a commit for sync, or a commit for autocommit.  Skips
		checks that a user isn't doing something bad like issuing
		a commit in a nested xact.

		@param	commitStore	true if we should commit the Store transaction

		@exception StandardException thrown if something goes wrong
	 */
	void internalCommit( boolean commitStore )
		 throws StandardException;

	/**
		Similar to internalCommit() but has logic for an unsynchronized commit

		@param	commitflag	the flags to pass to commitNoSync in the store's
							TransactionController

		@exception StandardException	thrown if something goes wrong
	 */
	void internalCommitNoSync(int commitflag) throws StandardException;

	/**
		Do a commit, as issued directly by a user (e.g. via Connection.commit()
		or the JSQL 'COMMIT' statement.

		@exception StandardException thrown if something goes wrong
	 */
	void userCommit() throws StandardException;

	/**
		Commit a distrubuted transaction.

		@param onePhase if true, allow it to commit without first going thru a
		prepared state.

		@exception StandardException	thrown if something goes wrong
	 */
	void xaCommit(boolean onePhase) throws StandardException;


	/**
		Do a rollback, as internally needed by Derby.  E.g.
	 	a rollback for sync, or a rollback for an internal error.  Skips
		checks that a user isn't doing something bad like issuing
		a rollback in a nested xact.

		@exception StandardException thrown if something goes wrong
	 */
	void internalRollback() throws StandardException;

	/**
		Do a rollback, as issued directly by a user (e.g. via Connection.rollback()
		or the JSQL 'ROLLBACK' statement.

		@exception StandardException thrown if something goes wrong
	 */
	void userRollback() throws StandardException;

	/**
	 * Let the context deal with a rollback to savepoint
	 *
	 * @param	savepointName	Name of the savepoint that needs to be rolled back
	 * @param	refreshStyle	boolean indicating whether or not the controller should close
	 * open conglomerates and scans. Also used to determine if language should close
	 * open activations.
	 * @param	kindOfSavepoint	 A NULL value means it is an internal savepoint (ie not a user defined savepoint)
	 * Non NULL value means it is a user defined savepoint which can be a SQL savepoint or a JDBC savepoint
	 *   A String value for kindOfSavepoint would mean it is SQL savepoint
	 *   A JDBC Savepoint object value for kindOfSavepoint would mean it is JDBC savepoint
	 *
	 * @exception StandardException thrown if something goes wrong
	 */
	void internalRollbackToSavepoint
	( String savepointName, boolean refreshStyle, Object kindOfSavepoint ) throws StandardException;

	/**
	 * Let the context deal with a release of a savepoint
	 *
	 * @param	savepointName	Name of the savepoint that needs to be released
	 * @param	kindOfSavepoint	 A NULL value means it is an internal savepoint (ie not a user defined savepoint)
	 * Non NULL value means it is a user defined savepoint which can be a SQL savepoint or a JDBC savepoint
	 *   A String value for kindOfSavepoint would mean it is SQL savepoint
	 *   A JDBC Savepoint object value for kindOfSavepoint would mean it is JDBC savepoint
	 *
	 * @exception StandardException thrown if something goes wrong
	 */
	void releaseSavePoint ( String savepointName, Object kindOfSavepoint ) throws StandardException;


	/**
		Roll back a distrubuted transaction.

		@exception StandardException	thrown if something goes wrong
	 */
	void xaRollback() throws StandardException;

	/**
	  Sets a savepoint. Causes the Store to set a savepoint.

	  @param	savepointName	name of savepoint
	  @param	kindOfSavepoint	 A NULL value means it is an internal savepoint (ie not a user defined savepoint)
	  Non NULL value means it is a user defined savepoint which can be a SQL savepoint or a JDBC savepoint
	  A String value for kindOfSavepoint would mean it is SQL savepoint
	  A JDBC Savepoint object value for kindOfSavepoint would mean it is JDBC savepoint

		@exception StandardException thrown if something goes wrong
	  */
	void	languageSetSavePoint( String savepointName, Object kindOfSavepoint )  throws StandardException;

	/**
	 * Begin a nested transaction.
	 *
	 * @param readOnly The nested transaction would be read only if param value true
	 *
	 * @exception StandardException on error.
	 * @see TransactionController#startNestedUserTransaction
	 */

	void beginNestedTransaction(boolean readOnly) throws StandardException;

	/**
	 * commit a nested transaction.
	 * We do not provide a abortNestedTransaction. 
	 * If a nested xaction is aborted, then this results in the parent xaction
	 * also being aborted. This is not what we need for releasing
	 * compile time locks or autoincrement-- hence we do not provide 
	 * abortNestedTransaction. 
	 *
	 * @exception StandardException thrown on erro
	 *
	 * @see TransactionController#startNestedUserTransaction
	 */
	void commitNestedTransaction() throws StandardException;

	/**
		Get the transaction controller to use with this language connection
		context at compile time.
	 */
	TransactionController getTransactionCompile();

	/**
		Get the transaction controller to use with this language connection
		context during execute time.
	 */

	TransactionController getTransactionExecute();
   
	/**
		Get the data dictionary 

		@return the data dictionary

	 */
	public DataDictionary getDataDictionary();

	/**
		Get the data value factory to use with this language connection
		context.
	 */
	DataValueFactory getDataValueFactory();

	/**
		Get the language factory to use with this language connection
		context.
	 */
	LanguageFactory getLanguageFactory();

	/**
	 * get the optimizer factory to use with this language connection context.
	 */
	OptimizerFactory getOptimizerFactory();
	
	/**
		Get the language connection factory to use with this language connection
		context.
	 */
	LanguageConnectionFactory getLanguageConnectionFactory();

	/**
	 *	Get the Authorization Id
	 *
	 * @return String	the authorization id
	 */
	public String getAuthorizationId();

	/**
	 *	Get the current default schema for the connection.
	 *
	 * @return SchemaDescriptor	the current schema
	 */
	public SchemaDescriptor getDefaultSchema(); 

	/**
	 *	Set the current default schema
	 *
	 * @param sd the new default schema
	 *
	 * @exception StandardException thrown on failure
	 */
	public void setDefaultSchema(SchemaDescriptor sd)
		throws StandardException;

	/**
	 *	Get the current schema name
	 *
	 * @return SchemaDescriptor	the current schema
	 */
	public String getCurrentSchemaName();


	/**
	 * Return true if this schema name is the initial default schema for the
	 * current session.
	 * @param schemaName
	 * @return true
	 */
	public boolean isInitialDefaultSchema(String schemaName);

	/**
	 * Get the identity column value most recently generated.
	 *
	 * @return the generated identity column value
	 */
	public Long getIdentityValue();

	/**
	 * Set the field of most recently generated identity column value.
	 *
	 * @param val the generated identity column value
	 */
	public void setIdentityValue(long val);

	/**
	 * Verify that there are no activations with open result sets
	 * on the specified prepared statement.
	 *
	 * @param pStmt		The prepared Statement
	 * @param provider	The object precipitating a possible invalidation
	 * @param action	The action causing the possible invalidation
	 *
	 * @return Nothing.
	 *
	 * @exception StandardException thrown on failure
	 */
	boolean verifyNoOpenResultSets(PreparedStatement pStmt, Provider provider, 
									   int action)
			throws StandardException;

	/**
	 * Verify that there are no activations with open held result sets.
	 *
	 * @return boolean  Found no open resultsets.
	 *
	 * @exception StandardException thrown on failure
	 */
	public boolean verifyAllHeldResultSetsAreClosed()
			throws StandardException;

	/**
	 * Push a CompilerContext on the context stack with
	 * the current default schema as the default schema
	 * which we compile against.
	 *
	 * @return the compiler context
	 *
	 * @exception StandardException thrown on failure
	 */
	public	CompilerContext pushCompilerContext();

	/**
	 * Push a CompilerContext on the context stack with
	 * the passed in default schema as the default schema
	 * we compile against.
	 *
	 * @param sd the default schema 
	 * @param isCompiling set to true when GenericStatement#prepMinion call is in progress, 
	 *        false if only statement matching is being done by this cc.
	 * @return the compiler context
	 *
	 * @exception StandardException thrown on failure
	 */
	public	CompilerContext pushCompilerContext(SchemaDescriptor sd, boolean isCompiling);

	/**
	 * Pop a CompilerContext off the context stack.
	 *
	 * @param compilerContext  The compiler context.
	 *
	 * @exception StandardException thrown on failure
	 */
	public void popCompilerContext(CompilerContext compilerContext);

	/**
	 * Push a StatementContext on the context stack.
	 *
	 * @param isAtomic whether a commit/rollback is permitted
	 *	from a nested connection under this statement
	 *
	 * @param stmtText the text of the statement.  Needed for any language
	 * 	statement (currently, for any statement that can cause a trigger
	 * 	to fire).  Please set this unless you are some funky jdbc setXXX
	 *	method or something.
	 *
	 * @param pvs parameter value set, if it has one
	 *
	 * @param rollbackParentContext True if 1) the statement context is
	 * 	NOT a top-level context, AND 2) in the event of a statement-level
	 *	exception, the parent context needs to be rolled back, too.
     *
     * @param timeoutMillis Timeout value for this statement, in milliseconds.
     *  Zero means no timeout.
	 *
	 * @return StatementContext	The statement context.
	 *
	 */
	StatementContext pushStatementContext(boolean isAtomic, boolean isForReadOnly, String stmtText,
		ParameterValueSet pvs, boolean rollbackParentContext, long timeoutMillis);

	/**
	 * Pop a StatementContext of the context stack.
	 *
	 * @param statementContext  The statement context.
	 * @param error				The error, if any  (Only relevant for DEBUG)
	 */
	public void popStatementContext(StatementContext statementContext,
									Throwable error);

	/**
	 * Push a new execution statement validator.  An execution statement 
	 * validator is an object that validates the current statement to
	 * ensure that it is permitted given the current execution context.
	 * An example of a validator a trigger ExecutionStmtValidator that
	 * doesn't allow ddl on the trigger target table.
	 * <p>
	 * Multiple ExecutionStmtValidators may be active at any given time.
	 * This mirrors the way there can be multiple connection nestings
	 * at a single time.  The validation is performed by calling each
	 * validator's validateStatement() method.  This yields the union
	 * of all validations.
	 *
	 * @param validator the validator to add
	 */
	public void pushExecutionStmtValidator(ExecutionStmtValidator validator);

	/**
	 * Remove the validator.  Does an object identity (validator == validator)
 	 * comparison.  Asserts that the validator is found.
	 *
	 * @param validator the validator to remove
	 *
	 * @exception StandardException on error
	 */
	public void popExecutionStmtValidator(ExecutionStmtValidator validator)
		throws StandardException;

	/**
	 * Validate a statement.  Does so by stepping through all the validators
	 * and executing them.  If a validator throws and exception, then the
	 * checking is stopped and the exception is passed up.
	 *
	 * @param constantAction the constantAction that is about to be executed (and
	 *	should be validated
 	 *
	 * @exception StandardException on validation failure
	 */
	public void validateStmtExecution(ConstantAction constantAction)
		throws StandardException;
	
	/**
	 * Push a new trigger execution context.
	 * <p>
	 * Multiple TriggerExecutionContexts may be active at any given time.
	 *
	 * @param tec the trigger execution context
	 *
	 * @exception StandardException on trigger recursion error
	 */
	public void pushTriggerExecutionContext(TriggerExecutionContext tec)
		throws StandardException;

	/**
	 * Remove the tec.  Does an object identity (tec == tec)
 	 * comparison.  Asserts that the tec is found.
	 *
	 * @param tec the tec to remove
	 *
	 * @exception StandardException on error
	 */
	public void popTriggerExecutionContext(TriggerExecutionContext tec)
		throws StandardException;

	/**
	 * Get the topmost tec.  
	 *
	 * @return the tec
	 */
	public TriggerExecutionContext getTriggerExecutionContext();

	/**
	 * Set the trigger table descriptor.  Used to compile
	 * statements that may special trigger pseudo tables.
	 *
	 * @param td the table that the trigger is 
	 * defined upon
	 *
	 */
	public void pushTriggerTable(TableDescriptor td);

	/**
	 * Remove the trigger table descriptor.
	 *
	 * @param td the table to remove from the stack.
	 */
	public void popTriggerTable(TableDescriptor td);

	/**
	 * Get the topmost trigger table descriptor
	 *
	 * @return the table descriptor, or null if we
	 * aren't in the middle of compiling a create
	 * trigger.
	 */
	public TableDescriptor getTriggerTable();

	/**
	 * Increment the DataDictionary bind count.  This is for keeping track
	 * of nested binding, which can happen if SQL statements are bound from
	 * within static initializers.
	 *
	 * @return	The new bind count
	 */
	int incrementBindCount();

	/**
	 * Decrement the DataDictionary bind count.
	 *
	 * @return	The new bind count
	 */
	int decrementBindCount();

	/**
	 * Get the DataDictionary bind count.
	 *
	 * @return	The current bind count.
	 */
	int getBindCount();

	/**
	 * Remember that the DataDictionary is in write mode, so we can take
	 * it out of write mode at the end of the transaction.
	 */
	void setDataDictionaryWriteMode();

	/**
	 * Return true if the data dictionary is in write mode (that is, this
	 * context was informed that is is in write mode by the method call
	 * setDataDictionaryWriteMode().
	 */
	boolean dataDictionaryInWriteMode();

	/**
	 * Turn RUNTIMESTATISTICS  on or off.
	 */
	public void setRunTimeStatisticsMode(boolean onOrOff, boolean force);

	/**
	 * Get the RUNTIMESTATISTICS mode.
	 */
	public boolean getRunTimeStatisticsMode();

	/**
	 * Turn STATISTICS TIMING on or off.
	 */
	public void setStatisticsTiming(boolean onOrOff);

	/**
	 * Get the STATISTICS TIMING mode.
	 */
	public boolean getStatisticsTiming();

	/** 
	 * Set the RUNTIMESTATISTICS object. 
	 */
	public void setRunTimeStatisticsObject(RunTimeStatistics runTimeStatisticsObject);

	/** 
	 * Get the RUNTIMESTATISTICS object. 
	 */
	public RunTimeStatistics getRunTimeStatisticsObject();


    /**
	  *	Reports how many statement levels deep we are.
	  *
	  *	@return	a statement level >= OUTERMOST_STATEMENT
	  */
	public	int		getStatementDepth();

	/**
	  Returns the Database of this connection.
     */
    public Database getDatabase();

	/**
	 * Returns true if isolation level has been set using JDBC/SQL.
	 */
	public boolean isIsolationLevelSetUsingSQLorJDBC();
	/**
	 * Reset the isolation level flag used to keep correct isolation level
	 * state in BrokeredConnection. This resetting will happen at the start 
	 * and end of a global transaction, after the BrokeredConection's 
	 * isolation level state is brought upto date with the EmbedConnection's
	 * isolation state.
	 * The flag gets set to true when isolation level is set using JDBC/SQL.
	 */
	public void resetIsolationLevelFlagUsedForSQLandJDBC();

	/**
	 * Set current isolation level.
	 *
	 * @param isolationLevel	The new isolationLevel.
	 */
	public void setIsolationLevel(int isolationLevel) throws StandardException;

	/**
	 * Get the current isolation level.
	 *
	 * @return The current isolation level.
	 */
	public int getCurrentIsolationLevel();

	public void setAutoCommit(boolean autoCommit);

	public boolean getAutoCommit();

	/**
	 * Get the current isolation level in DB2 format.
	 *
	 * @return The current isolation level as a 2 character string.
	 */
	public String getCurrentIsolationLevelStr();
	public void setPrepareIsolationLevel(int isolationLevel) ;

	/**
	 * Get the prepare isolation level.
	 * If the isolation level has been explicitly set with a SQL statement or
	 * embedded call to setTransactionIsolation, this will return
	 * ExecutionContext.UNSPECIFIED_ISOLATION_LEVEL 
	 * SET ISOLATION always takes priority.
	 * 
	 */
	public int getPrepareIsolationLevel();

	/**
	 * Set the readOnly status for the current connection. This can
	 * only be called when the current transaction has not done
	 * any work.
	 *
	 * @param onOrOff true sets the connection to be readOnly and
	 *                false sets it to readWrite.
	 *
	 * @exception StandardException The call failed and the readOnly
	 *                status has not changed.
	 */
	public void setReadOnly(boolean onOrOff) throws StandardException;

	/**
	  * Get the readOnly status for the current connection. 
	  */
	public boolean isReadOnly();

	/**
	 * Get an Authorizer for this connection.
	 */
	public Authorizer getAuthorizer(); 

	/**
	 *	Get the current StatementContext.
	 */
	StatementContext getStatementContext();

    /**
	 * Return a PreparedStatement object for the query.
	 * This method first tries to locate the PreparedStatement object from a statement
	 * cache.  If the statement is not found in the cache, the query will be compiled and
	 * put into the cache.
	 * @param compilationSchema schema
     * @param sqlText sql query string
     * @param isForReadOnly read only status for resultset. Set to true if the concurrency mode for the resultset 
 *                      is CONCUR_READ_ONLY
     * @param allowInternalSyntax If true, then this query is allowed to use internal 
 *                      sql syntax. One instance where this will be true is if a
 *                      metadata query is getting executed.
  // GemStone changes BEGIN
     * @param createQueryInfo If true, then this query will generate QueryInfo object                       
     * @param isPreparedStatement indicates whether the statement is getting compiled via EmbedStatement or EmbedPreparedStatement.
  // GemStone changes END                    
     * @param cc TODO
	 */
         public PreparedStatement prepareInternalStatement(SchemaDescriptor compilationSchema, 
         		String sqlText,  boolean allowInternalSyntax,
    // GemStone changes BEGIN
           short execFlags, CompilerContext cc, THashMap ncjMetaData)
    // GemStone changes END
	    throws StandardException;

        // GemStone changes BEGIN
        public CompilerContext lookupGeneralizedStatement(
            SchemaDescriptor compilationSchema, String sqlText,
           short execFlags) throws StandardException;
      
        public GenericPreparedStatement seekGenericPreparedStatement(
            final GenericStatement statement) throws StandardException;
        // GemStone changes END

        /**
	 * Return a PreparedStatement object for the query.
	 * This method first tries to locate the PreparedStatement object from a statement
	 * cache.  If the statement is not found in the cache, the query will be compiled and 
	 * put into the cache.
	 * The schema used when compiling the statement is the same schema as returned by
	 * getDefaultSchema().  For internal statements, the read only status is set to
	 * true.
	 * Calling this method is equivalent to calling 
	 * prepareExternalStatement(lcc.getDefaultSchema(), sqlText, true);
	 * 
	 * @param sqlText sql query string	 * 
   // GemStone changes BEGIN
   * @param createQueryInfo If true then theQueryInfo object will be generated
   * @param needGfxdSubactivation
   * @param flattenSubquery
   // GemStone changes END
	 */
        public PreparedStatement prepareInternalStatement(String sqlText,
   // GemStone changes BEGIN
           short execFlags)
   // GemStone changes END
	    throws StandardException;

	/**
	 * Control whether or not optimizer trace is on.
	 *
	 * @param onOrOff    Whether to turn optimizer trace on (true) or off (false).
	 *
	 * @return Whether or not the call was successful.  (false will be returned when optimizer tracing is not supported.)
	 */
	public boolean setOptimizerTrace(boolean onOrOff);

	/** 
	 * Get whether or not optimizer trace is on.
	 *
	 * @return Whether or not optimizer trace is on.
	 */
	public boolean getOptimizerTrace();

	/**
	 * Control whether or not optimizer trace is generated in html.
	 *
	 * @param onOrOff    Whether or not optimizer trace will be in html (true) or not (false).
	 *
	 * @return Whether or not the call was successful.  (false will be returned when optimizer tracing is not supported.)
	 */
	public boolean setOptimizerTraceHtml(boolean onOrOff);

	/** 
	 * Get whether or not optimizer trace html is on.
	 *
	 * @return Whether or not optimizer trace html is on.
	 */
	public boolean getOptimizerTraceHtml();

	/**
	 * Get the optimizer trace output for the last optimized query as a String.  If optimizer trace
	 * html is on, then the String will contain the html tags.
	 *
	 * @return The optimizer trace output for the last optimized query as a String.
	 *    Null will be returned if optimizer trace output is off or not supported 
	 *    or no trace output was found or an exception occurred.
	 */
	public String getOptimizerTraceOutput();

	/**
	 * Set the optimizer trace output to the specified String.
	 * (Done at the beginning of each statement.)
	 */
	public void setOptimizerTraceOutput(String startingText);

	/**
	 * Append the latest output to the optimizer trace output.
	 */
	public void appendOptimizerTraceOutput(String output);

    /**
	  *	Reports whether there is any outstanding work in the transaction.
	  *
	  *	@return		true if there is outstanding work in the transaction
	  *				false otherwise
	  */
	public	boolean	isTransactionPristine();


	/**
	 * Returns the last autoincrement value inserted by this connection.
	 * If no values have been inserted into the given column a NULL value
	 * is returned.
	 * 
	 * @param schemaName
	 * @param tableName
	 * @param columnName
	 */
	public Long lastAutoincrementValue(String schemaName,
									   String tableName,
									   String columnName);

	/**
	 * Sets autoincrementUpdate-- this variable allows updates to autoincrement
	 * columns if it is set to true. The default is ofcourse false; i.e 
	 * ai columns cannot be directly modified by the user. This is set to 
	 * true by AlterTableConstantAction, when a new ai column is being added 
	 * to an existing table.
	 * 
	 * @param flag 	the value for autoincrementUpdate (TRUE or FALSE)
	 * @see com.pivotal.gemfirexd.internal.impl.sql.execute.AlterTableConstantAction#updateNewAutoincrementColumn
	 *
	 */
	public void setAutoincrementUpdate(boolean flag);

	/**
	 * Returns the current value of autoincrementUpdate.
	 *
	 * @return true if updates to autoincrement columns is permitted.
	 */
	public boolean getAutoincrementUpdate();

	/**
	 * Copy a map of autoincrement key value pairs into the cache of
	 * ai values stored in the language connection context.
	 */
	public void copyHashtableToAIHT(Map from);
	
	/**
	 * returns the <b>next</b> value to be inserted into an autoincrement col.
	 * This is used internally by the system to generate autoincrement values
	 * which are going to be inserted into a autoincrement column. This is
	 * used when as autoincrement column is added to a table by an alter 
	 * table statemenet and during bulk insert.
	 *
	 * @param schemaName
	 * @param tableName
	 * @param columnName identify the column uniquely in the system.
	 *
	 * @exception StandardException on error.
	 */
	public long nextAutoincrementValue(String schemaName, String tableName,
									   String columnName)
		throws StandardException;

	/**
	 * Flush the cache of autoincrement values being kept by the lcc.
	 * This will result in the autoincrement values being written to the
	 * SYSCOLUMNS table as well as the mapping used by lastAutoincrementValue
	 * 
	 * @param tableUUID the table which is being flushed; we need this value to
	 * identify the table for which the autoincrement counter is being
	 * maintained.
	 *
	 * @exception StandardException thrown on error.
	 *
	 * @see LanguageConnectionContext#lastAutoincrementValue
	 * @see com.pivotal.gemfirexd.internal.impl.sql.conn.GenericLanguageConnectionContext#lastAutoincrementValue
	 * @see com.pivotal.gemfirexd.internal.iapi.db.ConnectionInfo#lastAutoincrementValue
	 */
	public void autoincrementFlushCache(UUID tableUUID)
		throws StandardException;

	/**
	 * Create an autoincrement counter to be used on behalf of a SQL-J 
	 * statement. The counter is identified by (schemaName, tableName,
	 * columnName). The counter must be freed up by calling 
	 * autoincrementFlushCache at the end of the statement. It is expected
	 * that a ai-counter with the same signaure doesn't exist when the 
	 * method is called.
	 * 
	 * @param 		s					SchemaName
	 * @param		t					TableName
	 * @param		c					ColumnName
	 * @param		initialValue		initial value of the counter.
	 * @param		increment			increment for the counter.
	 * @param		position			column position (1-based).
	 */
	public void autoincrementCreateCounter(String s, String t, String c,
										   Long initialValue, long increment,
										   int position);
	
	/**
	 * Get the instance number of this LCC.
	 *
	 * @return instance number of this LCC.
	 */
	public int getInstanceNumber();

	/**
	 * Get the DRDA ID of this LCC.
	 *
	 * @return DRDA ID this LCC.
	 */
	public String getDrdaID();

	/**
	 * Set the DRDA ID of this LCC.
	 *
	 * @param drdaID DRDA ID.
	 */
	public void setDrdaID(String drdaID);

	/**
	 * Get the database name of this LCC.
	 *
	 * @return database name of this LCC.
	 */
	public String getDbname();

	/**
	 * Check if in SQL standard mode, with support for Grant & Revoke
	 *
	 * @return True if SQL standard permissions are being used
	 */
	public boolean usesSqlAuthorization();

	/**
	 * Close any unused activations in this connection context.
	 */
	public void closeUnusedActivations() throws StandardException;

	/**
	 * Remember most recent (call stack top) caller's activation when
	 * invoking a method, see CallStatementResultSet#open.
	 */
	public void pushCaller(Activation a);

	/**
	 * Companion of pushCaller. See usage in CallStatementResultSet#open.
	 */
	public void popCaller();

	/**
	 * Get most recent (call stack top) caller's activation
	 */
	public Activation getCaller();

    /**
	 * Set the current role
	 *
	 * @param a activation of set role statement
	 * @param role  the id of the role to be set to current
	 */
    public void setCurrentRole(Activation a, String role);

	/**
	 * Get the current role authorization identifier of the dynamic
	 * call context associated with this activation.
	 *
	 * @param a activation of statement needing current role
	 * @return String	the role id
	 */
	public String getCurrentRoleId(Activation a);
	
    /**
     * sets the XplainOnlyMode.
     *
     * If a connection is in XplainOnlyMode, then the statements are not
     * actually being executed, but are just being compiled and the
     * runtime statistics collected into the XPLAIN tables. This can be
     * set on and off by calling SYSCS_SET_XPLAIN_MODE.
     *
     * @param onOrOff true if statements are to be XPLAINed only.
     */
    public void setExplainConnection(boolean onOrOff);

    /**
     * gets the current set XplainOnlyMode
     */
    public boolean explainConnection();
    
    /**
     * sets the ExplainSchema. 
     * prefer using SystemProcedure.SET_EXPLAIN_SCHEMA call instead
     * of directly invoking it to enable explain mode appropriately.
     * 
     * @param schema the schema to use for storing XPLAIN'd statements
     * null means don't store the xplain information
     * non-null means persistent style, use the indicated schema
     */
    public void setExplainSchema(boolean schema);

    /**
     * gets the current set XplainSchema
     * @return the Schema of Xplain, may be null.
     */
    public boolean getExplainSchema();
    public void setExplainStatement(String key, String stmt);
    public String getExplainStatement(String key);
	

// GemStone changes BEGIN

  public boolean explainStatementsExists();

  /**
   * @return true for DDL/DMLs received from a remote node by distribution
   */
  public boolean isConnectionForRemote();

  /** set the remote node connection property */
  public void setIsConnectionForRemote(boolean forRemote);

  /**
   * @return true for DDLs received from a remote node by distribution
   */
  public boolean isConnectionForRemoteDDL();

  /** set the remote DDL execution property */
  public void setIsConnectionForRemoteDDL(boolean forRemote);

  /**
   * @return true when read/write locks should be skipped for current
   *         transaction (e.g. for DDL from a remote node)
   */
  public boolean skipLocks();

  /** set the skip locks connection property */
  public void setSkipLocks(boolean skip);

  /**
   * set the skip locks property for connection globally; only allowed for the
   * admin user
   */
  public void setSkipLocksForConnection(boolean skip) throws StandardException;

  public long getConnectionId();

  public void setStatementId(long id);

  public long getStatementId();

  /**
   * Start a new nested <code>Transaction</code> for execution. The
   * {@link #beginNestedTransaction(boolean)} only starts a new nested
   * Transaction for compilation which is not used for execution. Only one level
   * of nesting is allowed.
   * 
   * @param readOnly
   *          if param value true then the nested transaction would be read-only
   * @param transactional
   *          if true then the transaction isolation-level is inherited else it
   *          is non-transactional and current transaction is committed
   * @param opLogName
   *          if the current transaction was committed this is the name of
   *          current operation that is logged in the warning
   * 
   * 
   * @throws StandardException
   *           on an error
   */
  public void beginNestedTransactionExecute(boolean readOnly,
      boolean transactional, String opLogName) throws StandardException;

  /**
   * Get the parent of a nested transaction started using
   * {@link #beginNestedTransactionExecute}.
   */
  public GemFireTransaction getParentOfNestedTransactionExecute();

  /**
   * Commit the nested <code>Transaction</code> for execution started by
   * {@link #beginNestedTransactionExecute}.
   * 
   * @throws StandardException
   *           on an error
   */
  public void commitNestedTransactionExecute() throws StandardException;

  /**
   * Abort the nested <code>Transaction</code> for execution started by
   * {@link #beginNestedTransactionExecute}.
   * 
   * @throws StandardException
   *           on an error
   */
  public void abortNestedTransactionExecute() throws StandardException;

  /**
   * Cleanup the nested <code>Transaction</code> for execution started by
   * {@link #beginNestedTransactionExecute}. Should typically be invoked
   * after {@link #abortNestedTransactionExecute()}.
   */
  public void cleanupNestedTransactionExecute();

  public void internalCleanup() throws StandardException;

  public void setPossibleDuplicate(boolean dup);

  public boolean isPossibleDuplicate();

  /** any other context object to be set to be retrieved from LCC later */
  public void setContextObject(Object ctx);

  /** get the context object last set in LCC by {@link #setContextObject} */
  public Object getContextObject();

  /**
   * Get the identity column value most recently generated as primitive long.
   * 
   * @return the generated identity column value
   */
  public long getIdentityVal();

  /** get the list of pending read locks in this transaction */
  public TXState.ArrayListAppend[] getPendingReadLocks();

  /** set the enable streaming property for this connection */
  public void setEnableStreaming(boolean enableStreaming);

  /**
   * Returns false if the property
   * {@link com.pivotal.gemfirexd.Attribute#DISABLE_STREAMING}
   * is set for this connection and true otherwise.
   */
  public boolean streamingEnabled();

  /** set the skip-listeners property for this connection */
  public void setSkipListeners();

  /**
   * 
   * @return true if the writers/async event listeners/dbsynchronizer call needs
   *         to be skipped
   */
  public boolean isSkipListeners();

  /**
   * Set the {@link TransactionFlag}s to be used for all transactions on the
   * connection.
   */
  public void setTXFlags(EnumSet<TransactionFlag> flags);

  /**
   * Get the {@link TransactionFlag}s to be used for all transactions on the
   * connection.
   */
  public EnumSet<TransactionFlag> getTXFlags();

  /**
   * Set the active {@link StatementStats} for this connection.
   */
  public void setActiveStats(StatementStats stats);

  /**
   * Get the active {@link StatementStats} for this connection.
   */
  public StatementStats getActiveStats();

  /**
   * Enable/disable StatementStats and time statistics for this connection.
   * @param explainConnection TODO
   */
  public void setStatsEnabled(boolean enableStats, boolean enableTimeStats, boolean explainConnection);

  /**
   * Returns true if the property
   * {@link com.pivotal.gemfirexd.internal.engine.GfxdConstants#GFXD_ENABLE_STATS}
   * is set for this connection.
   */
  public boolean statsEnabled();

  /**
   * Returns true if the property
   * {@link com.pivotal.gemfirexd.internal.engine.GfxdConstants#GFXD_ENABLE_TIMESTATS}
   * is set for this connection.
   */
  public boolean timeStatsEnabled();
  
  /**
   * Enable/disable bulk foreign key checks for put all  
   */
  public void setEnableBulkFkChecks(boolean val);
  
  /**
   * Returns true of the property
   * {@link com.pivotal.gemfirexd.internal.engine.GfxdConstants#GFXD_ENABLE_BULK_FK_CHECKS}
   * is set for this connection. 
   */
  public boolean bulkFkChecksEnabled();


  public ArrayList<Activation> getAllActivations();

  StatementContext pushStatementContext(boolean isAtomic,
      boolean isForReadOnly, String stmtText, ParameterValueSet pvs,
      boolean rollbackParentContext, long timeoutMillis, boolean isLightWeight);

  public Statement createGenericStatement(String sqlText);

  public PreparedStatement getTriggerActionPrepStmnt(TriggerDescriptor td,
      String actionText) throws StandardException;

  public void setGatewaySenderEventCallbackArg(GatewaySenderEventCallbackArgument arg);

  public GatewaySenderEventCallbackArgument getGatewaySenderEventCallbackArg();
  
  public BaseActivation getExpressionActivation(GeneratedClass gc, int prId)
      throws StandardException;

  public void setConstantValueSet(CompilerContext cc,
      ParameterValueSet constantValues);

  public ParameterValueSet getConstantValueSet(Activation activation);

  public void unlinkConstantValueSet();

  public MemConglomerate getConglomerateForDeclaredGlobalTempTable(
      long conglomId);

  public void setReferencedKeyCheckRows(Collection<RowLocation> rows);

  public Collection<RowLocation> getReferencedKeyCheckRows();

  public void clearDataDictionaryWriteMode();

  public void setTransientStatementNode(StatementNode qt);

  public StatementNode getTransientStatementNode();

  public void setIgnoreWhereCurrentOfUnsupportedException(boolean b);

  /**
   * Get the RUNTIMESTATISTICS mode set explicitly via SET_RUNTIMESTATISTICS.
   */
  public boolean getRunTimeStatisticsModeExplicit();

  public boolean ignoreWhereCurrentOfUnsupportedException();

  /** combine many of the above boolean flags in an integer and return */
  public int getFlags();

  /** set the flags to that returned by a previous call to {@link #getFlags} */
  public void setFlags(int flags);

  public void setSendSingleHopInformation(boolean b);
  
  public boolean getSendSingleHopInformation();

  public void setExecuteLocally(Set<Integer> bucketIds, Region<?, ?> region,
      boolean dbSync, Checkpoint cp);

  public void setBucketRetentionForLocalExecution(boolean retain);

  public void clearExecuteLocally();

  public Set<Integer> getBucketIdsForLocalExecution();
  
  public Region<?, ?> getRegionForBucketSet();

  public void setSkipRegionInitialization(boolean flag);

  public boolean skipRegionInitialization();
  
  public boolean dbSyncToBeDone();
  
  public Checkpoint getCheckpoint();
  
  public void setQueryHDFS(boolean val);
  
  public void setTriggerBody(boolean flag);
  
  public boolean isTriggerBody();
  
  public boolean getQueryHDFS();  
  
  void setHDFSSplit(Object path);
  
  Object getHDFSSplit();

  public void setSkipConstraintChecks(boolean skipConstraints);

  public boolean isSkipConstraintChecks();
  
  public void setLastStatementQueryTimeOut(long timeOutMillis);
  
  public long getQueryTimeOutForLastUsedStatement();
  
  public void setDefaultQueryTimeOut(int timeout);
  
  public int getDeafultQueryTimeOut();
  
  public void setNcjBatchSize(int batchSize);
  
  public int getNcjBatchSize();
  
  public void setNcjCacheSize(int cacheSize);
  
  public int getNcjCacheSize();
  
  /**
   * see {@link #getDroppedFKConstraints()}
   * 
   */
  public void setDroppedFKConstraints(Set<String> droppedFKList);
  
  /**
   * Used during DDL replay for CREATE TABLE and ALTER TABLE ADD FK
   * CONSTRAINT, if the constraint has been dropped later. In such a case, 
   * parent table on which the constraint was defined may also be 
   * dropped so ignore the error while defining FK constraint 
   * while creating the table or adding FK (#50116)  
   */
  public Set<String> getDroppedFKConstraints();

	/**
   * Query routing will be attempted only when this flag is true
   * @param routeQuery
   */
   void setQueryRoutingFlag(boolean routeQuery);

   boolean isQueryRoutingFlagTrue();

   void setSnappyInternalConnection(boolean internalConnection);

   boolean isSnappyInternalConnection();

   void setAllowExplicitCommit(boolean allowExplicitCommit);

   boolean isAllowExplicitCommitTrue();

	/**
	 * If set then all tables created on this connection will be PERSISTENT
	 * by default even if not specified in the DDL.
	 */
	void setDefaultPersistent(boolean b);

	boolean isDefaultPersistent();

	/**
	 * If set then metastore tables created on this connection will be stored
	 * in DataDictionary (default is true).
	 */
	void setPersistMetaStoreInDataDictionary(boolean b);

	boolean isPersistMetaStoreInDataDictionary();
// GemStone changes END

}
