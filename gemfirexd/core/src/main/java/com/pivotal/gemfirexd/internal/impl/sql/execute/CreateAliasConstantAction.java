/*

   Derby - Class com.pivotal.gemfirexd.internal.impl.sql.execute.CreateAliasConstantAction

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

package com.pivotal.gemfirexd.internal.impl.sql.execute;

import com.pivotal.gemfirexd.internal.catalog.AliasInfo;
import com.pivotal.gemfirexd.internal.catalog.UUID;
import com.pivotal.gemfirexd.internal.catalog.types.RoutineAliasInfo;
import com.pivotal.gemfirexd.internal.catalog.types.SynonymAliasInfo;
import com.pivotal.gemfirexd.internal.iapi.error.StandardException;
import com.pivotal.gemfirexd.internal.iapi.reference.ClassName;
import com.pivotal.gemfirexd.internal.iapi.reference.SQLState;
import com.pivotal.gemfirexd.internal.iapi.services.loader.ClassInspector;
import com.pivotal.gemfirexd.internal.iapi.services.sanity.SanityManager;
import com.pivotal.gemfirexd.internal.iapi.sql.Activation;
import com.pivotal.gemfirexd.internal.iapi.sql.conn.LanguageConnectionContext;
import com.pivotal.gemfirexd.internal.iapi.sql.dictionary.AliasDescriptor;
import com.pivotal.gemfirexd.internal.iapi.sql.dictionary.DataDescriptorGenerator;
import com.pivotal.gemfirexd.internal.iapi.sql.dictionary.DataDictionary;
import com.pivotal.gemfirexd.internal.iapi.sql.dictionary.SchemaDescriptor;
import com.pivotal.gemfirexd.internal.iapi.sql.dictionary.TableDescriptor;
import com.pivotal.gemfirexd.internal.iapi.sql.execute.ConstantAction;
import com.pivotal.gemfirexd.internal.iapi.store.access.TransactionController;

/**
 *	This class performs actions that are ALWAYS performed for a
 *	CREATE FUNCTION, PROCEDURE or SYNONYM Statement at execution time.
 *  These SQL objects are stored in the SYS.SYSALIASES table and
 *  represented as AliasDescriptors.
 *
 */
//GemStone changes BEGIN
//made public
public final
//GemStone changes END
class CreateAliasConstantAction extends DDLConstantAction
{

	private final String					aliasName;
	private final String					schemaName;
	private final String					javaClassName;
	private final char					aliasType;
	private final char					nameSpace;
	private final AliasInfo				aliasInfo;

	// CONSTRUCTORS

	/**
	 *	Make the ConstantAction for a CREATE alias statement.
	 *
	 *  @param aliasName		Name of alias.
	 *  @param schemaName		Name of alias's schema.
	 *  @param javaClassName	Name of java class.
	 *  @param aliasInfo		AliasInfo
	 *  @param aliasType		The type of the alias
	 */
	CreateAliasConstantAction(
								String	aliasName,
								String	schemaName,
								String	javaClassName,
								AliasInfo	aliasInfo,
								char	aliasType)
	{
		this.aliasName = aliasName;
		this.schemaName = schemaName;
		this.javaClassName = javaClassName;
		this.aliasInfo = aliasInfo;
		this.aliasType = aliasType;
		switch (aliasType)
		{
			case AliasInfo.ALIAS_TYPE_PROCEDURE_AS_CHAR:
				nameSpace = AliasInfo.ALIAS_NAME_SPACE_PROCEDURE_AS_CHAR;
				break;

			case AliasInfo.ALIAS_TYPE_FUNCTION_AS_CHAR:
				nameSpace = AliasInfo.ALIAS_NAME_SPACE_FUNCTION_AS_CHAR;
				break;

			case AliasInfo.ALIAS_TYPE_SYNONYM_AS_CHAR:
				nameSpace = AliasInfo.ALIAS_NAME_SPACE_SYNONYM_AS_CHAR;
				break;

			case AliasInfo.ALIAS_TYPE_UDT_AS_CHAR:
				nameSpace = AliasInfo.ALIAS_NAME_SPACE_UDT_AS_CHAR;
				break;
// GemStone changes BEGIN
			case AliasInfo.ALIAS_TYPE_RESULT_PROCESSOR_AS_CHAR:
			        nameSpace=AliasInfo.ALIAS_NAME_SPACE_RESULT_PROCESSOR_AS_CHAR;
				break;
// GemStone changes END
			default:
				if (SanityManager.DEBUG)
				{
					SanityManager.THROWASSERT(
						"Unexpected value for aliasType (" + aliasType + ")");
				}
				nameSpace = '\0';
				break;
		}
	}

	// OBJECT SHADOWS

	public	String	toString()
	{
		// Do not put this under SanityManager.DEBUG - it is needed for
		// error reporting.
		String type = null;

		switch (aliasType)
		{
			case AliasInfo.ALIAS_TYPE_PROCEDURE_AS_CHAR:
				type = "CREATE PROCEDURE ";
				break;

			case AliasInfo.ALIAS_TYPE_FUNCTION_AS_CHAR:
				type = "CREATE FUNCTION ";
				break;

			case AliasInfo.ALIAS_TYPE_SYNONYM_AS_CHAR:
				type = "CREATE SYNONYM ";
				break;

			case AliasInfo.ALIAS_TYPE_UDT_AS_CHAR:
				type = "CREATE TYPE ";
				break;
// GemStone changes BEGIN
	                case AliasInfo.ALIAS_TYPE_RESULT_PROCESSOR_AS_CHAR:
	                        type="CREATE PROCEDURE RESULT PROCESSOR";
	                        break;
// GemStone changes END
				
			default:
				if (SanityManager.DEBUG)
				{
					SanityManager.THROWASSERT(
						"Unexpected value for aliasType (" + aliasType + ")");
				}
		}

		return	type + aliasName;
	}

	// INTERFACE METHODS


	/**
	 *	This is the guts of the Execution-time logic for
     *  CREATE FUNCTION, PROCEDURE, SYNONYM, and TYPE.
     *  <P>
     *  A function, procedure, or udt is represented as:
     *  <UL>
     *  <LI> AliasDescriptor
     *  </UL>
     *  Routine dependencies are created as:
     *  <UL>
     *  <LI> None
     *  </UL>
     *  
     *  <P>
     *  A synonym is represented as:
     *  <UL>
     *  <LI> AliasDescriptor
     *  <LI> TableDescriptor
     *  </UL>
     *  Synonym dependencies are created as:
     *  <UL>
     *  <LI> None
     *  </UL>
     *  
     *  In both cases a SchemaDescriptor will be created if
     *  needed. No dependency is created on the SchemaDescriptor.
     *  
	 * @see ConstantAction#executeConstantAction
     * @see AliasDescriptor
     * @see TableDescriptor
     * @see SchemaDescriptor
	 *
	 * @exception StandardException		Thrown on failure
	 */
	public void	executeConstantAction( Activation activation )
						throws StandardException
	{
		LanguageConnectionContext lcc =
            activation.getLanguageConnectionContext();

		DataDictionary dd = lcc.getDataDictionary();
		TransactionController tc = lcc.getTransactionExecute();

		// For routines no validity checking is made
        // on the Java method, that is checked when the
        // routine is executed.
        
		/*
		** Inform the data dictionary that we are about to write to it.
		** There are several calls to data dictionary "get" methods here
		** that might be done in "read" mode in the data dictionary, but
		** it seemed safer to do this whole operation in "write" mode.
		**
		** We tell the data dictionary we're done writing at the end of
		** the transaction.
		*/
		dd.startWriting(lcc);

		
		SchemaDescriptor sd =
            getSchemaDescriptorForCreate(dd, activation, schemaName);

// GemStone changes BEGIN
    // acquire exclusive lock on the alias object
    com.pivotal.gemfirexd.internal.engine.distributed.utils.GemFireXDUtils
        .lockObject(null, sd.getSchemaName() + '.' + this.aliasName, true, tc);
// GemStone changes END
		//
		// Create a new alias descriptor with aliasID filled in.
		// 
		UUID aliasID = dd.getUUIDFactory().createUUID();

		AliasDescriptor ads = new AliasDescriptor(dd, aliasID,
									 aliasName,
									 sd.getUUID(),
									 javaClassName,
									 aliasType,
									 nameSpace,
									 false,
									 aliasInfo, null);

		// perform duplicate rule checking
		switch (aliasType) {
		case AliasInfo.ALIAS_TYPE_UDT_AS_CHAR:

            AliasDescriptor duplicateUDT = dd.getAliasDescriptor( sd.getUUID().toString(), aliasName, nameSpace );
            if ( duplicateUDT != null ) { throw StandardException.newException( SQLState.LANG_OBJECT_ALREADY_EXISTS, ads.getDescriptorType(), aliasName ); }
            break;
            
		case AliasInfo.ALIAS_TYPE_PROCEDURE_AS_CHAR:
		case AliasInfo.ALIAS_TYPE_FUNCTION_AS_CHAR:
		{

			java.util.List list = dd.getRoutineList(
				sd.getUUID().toString(), aliasName, aliasType);
			for (int i = list.size() - 1; i >= 0; i--) {

				AliasDescriptor proc = (AliasDescriptor) list.get(i);

				RoutineAliasInfo procedureInfo = (RoutineAliasInfo) proc.getAliasInfo();
				int parameterCount = procedureInfo.getParameterCount();
				if (parameterCount != ((RoutineAliasInfo) aliasInfo).getParameterCount())
					continue;

				// procedure duplicate checking is simple, only
				// one procedure with a given number of parameters.
				throw StandardException.newException(SQLState.LANG_OBJECT_ALREADY_EXISTS,
												ads.getDescriptorType(),
												aliasName);
			}
		}
		break;
//Gemstone changes Begin
		case AliasInfo.ALIAS_TYPE_RESULT_PROCESSOR_AS_CHAR:                
                {

                        java.util.List list = dd.getRoutineList(
                                sd.getUUID().toString(), aliasName, aliasType);
                        for (int i = list.size() - 1; i >= 0; i--) {

                                AliasDescriptor proc = (AliasDescriptor) list.get(i);
                                                           
                              
                                if(proc.getObjectName().equals(aliasName)) {
                                            throw StandardException.newException(SQLState.LANG_OBJECT_ALREADY_EXISTS,
                                                                                                ads.getDescriptorType(),
                                                                                                aliasName);
                                }             
                        }

                        ClassInspector ci=lcc.getLanguageConnectionFactory().getClassFactory().getClassInspector();
                        if (!ci.assignableTo(ads.getJavaClassName(),
                            ClassName.ProcedureResultProcessor)) {
                          throw StandardException.newException(
                              SQLState.DAP_RESULT_PROCESSOR_INTERFACE_MISSING,
                              ads.getJavaClassName(),
                              ClassName.ProcedureResultProcessor);
                        }
                }
                break;
//Gemstone changes End		
		case AliasInfo.ALIAS_TYPE_SYNONYM_AS_CHAR:
			// If target table/view exists already, error.
			TableDescriptor targetTD = dd.getTableDescriptor(aliasName, sd, tc);
			if (targetTD != null)
			{
				throw StandardException.newException(
								SQLState.LANG_OBJECT_ALREADY_EXISTS,
								targetTD.getDescriptorType(),
								targetTD.getDescriptorName());
			}

			// Detect synonym cycles, if present.
			String nextSynTable = ((SynonymAliasInfo)aliasInfo).getSynonymTable();
			String nextSynSchema = ((SynonymAliasInfo)aliasInfo).getSynonymSchema();
			SchemaDescriptor nextSD;
			for (;;)
			{
				nextSD = dd.getSchemaDescriptor(nextSynSchema, tc, false);
				if (nextSD == null)
					break;
				
				AliasDescriptor nextAD = dd.getAliasDescriptor(nextSD.getUUID().toString(),
						 nextSynTable, nameSpace);
				if (nextAD == null)
					break;

				SynonymAliasInfo info = (SynonymAliasInfo) nextAD.getAliasInfo();
				nextSynTable = info.getSynonymTable();
				nextSynSchema = info.getSynonymSchema();

				if (aliasName.equals(nextSynTable) && schemaName.equals(nextSynSchema))
					throw StandardException.newException(SQLState.LANG_SYNONYM_CIRCULAR,
							aliasName, ((SynonymAliasInfo)aliasInfo).getSynonymTable());
			}

			// If synonym final target is not present, raise a warning
			if (nextSD != null)
				targetTD = dd.getTableDescriptor(nextSynTable, nextSD, tc);
			if (nextSD == null || targetTD == null)
				activation.addWarning(
					StandardException.newWarning(SQLState.LANG_SYNONYM_UNDEFINED,
								aliasName, nextSynSchema+"."+nextSynTable));

			// To prevent any possible deadlocks with SYSTABLES, we insert a row into
			// SYSTABLES also for synonyms. This also ensures tables/views/synonyms share
			// same namespace
			TableDescriptor td;
			DataDescriptorGenerator ddg = dd.getDataDescriptorGenerator();
			td = ddg.newTableDescriptor(aliasName, sd, TableDescriptor.SYNONYM_TYPE,
						TableDescriptor.DEFAULT_LOCK_GRANULARITY, TableDescriptor.DEFAULT_ROW_LEVEL_SECURITY_ENABLED);
			dd.addDescriptor(td, sd, DataDictionary.SYSTABLES_CATALOG_NUM, false, tc);
            break;
		
		default:
			break;
		}

		dd.addDescriptor(ads, null, DataDictionary.SYSALIASES_CATALOG_NUM,
						 false, tc);

        adjustUDTDependencies( lcc, dd, ads, true );
	}
// GemStone changes BEGIN

  @Override
  public final String getSchemaName() {
    return this.schemaName;
  }

  @Override
  public final String getObjectName() {
    return this.aliasName;
  }
// GemStone changes END
}
