/*

   Derby - Class com.pivotal.gemfirexd.internal.impl.sql.catalog.SYSSCHEMASRowFactory

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

package com.pivotal.gemfirexd.internal.impl.sql.catalog;

import java.sql.Types;

import com.pivotal.gemfirexd.internal.catalog.UUID;
import com.pivotal.gemfirexd.internal.engine.store.ServerGroupUtils;
import com.pivotal.gemfirexd.internal.iapi.error.StandardException;
import com.pivotal.gemfirexd.internal.iapi.services.sanity.SanityManager;
import com.pivotal.gemfirexd.internal.iapi.services.uuid.UUIDFactory;
import com.pivotal.gemfirexd.internal.iapi.sql.dictionary.CatalogRowFactory;
import com.pivotal.gemfirexd.internal.iapi.sql.dictionary.DataDescriptorGenerator;
import com.pivotal.gemfirexd.internal.iapi.sql.dictionary.DataDictionary;
import com.pivotal.gemfirexd.internal.iapi.sql.dictionary.SchemaDescriptor;
import com.pivotal.gemfirexd.internal.iapi.sql.dictionary.SystemColumn;
import com.pivotal.gemfirexd.internal.iapi.sql.dictionary.TupleDescriptor;
import com.pivotal.gemfirexd.internal.iapi.sql.execute.ExecRow;
import com.pivotal.gemfirexd.internal.iapi.sql.execute.ExecutionFactory;
import com.pivotal.gemfirexd.internal.iapi.types.DataTypeDescriptor;
import com.pivotal.gemfirexd.internal.iapi.types.DataValueDescriptor;
import com.pivotal.gemfirexd.internal.iapi.types.DataValueFactory;
import com.pivotal.gemfirexd.internal.iapi.types.SQLChar;
import com.pivotal.gemfirexd.internal.iapi.types.SQLVarchar;
import com.pivotal.gemfirexd.internal.shared.common.SharedUtils;

/**
 * Factory for creating a SYSSCHEMAS row.
 *
 *
 * @version 0.1
 */

public class SYSSCHEMASRowFactory extends CatalogRowFactory
{
// GemStone changes BEGIN
	public static final String	TABLENAME_STRING = "SYSSCHEMAS";

	public static final int	SYSSCHEMAS_COLUMN_COUNT = 4;
        /*
	private	static	final	String	TABLENAME_STRING = "SYSSCHEMAS";

	public	static	final	int		SYSSCHEMAS_COLUMN_COUNT = 3;
	*/
// GemStone changes END
	/* Column #s for sysinfo (1 based) */
	public	static	final	int		SYSSCHEMAS_SCHEMAID = 1;
	public	static	final	int		SYSSCHEMAS_SCHEMANAME = 2;
	public	static	final	int		SYSSCHEMAS_SCHEMAAID = 3;

	protected static final int		SYSSCHEMAS_INDEX1_ID = 0;
	protected static final int		SYSSCHEMAS_INDEX2_ID = 1;


	private static final int[][] indexColumnPositions =
	{
		{SYSSCHEMAS_SCHEMANAME},
		{SYSSCHEMAS_SCHEMAID}
	};
	
    private	static	final	boolean[]	uniqueness = null;

	private	static	final	String[]	uuids =
	{
		 "80000022-00d0-fd77-3ed8-000a0a0b1900"	// catalog UUID
		,"8000002a-00d0-fd77-3ed8-000a0a0b1900"	// heap UUID
		,"80000024-00d0-fd77-3ed8-000a0a0b1900"	// SYSSCHEMAS_INDEX1
		,"80000026-00d0-fd77-3ed8-000a0a0b1900"	// SYSSCHEMAS_INDEX2
	};

	/////////////////////////////////////////////////////////////////////////////
	//
	//	CONSTRUCTORS
	//
	/////////////////////////////////////////////////////////////////////////////

    SYSSCHEMASRowFactory(UUIDFactory uuidf, ExecutionFactory ef, DataValueFactory dvf)
	{
		super(uuidf,ef,dvf);
		initInfo(SYSSCHEMAS_COLUMN_COUNT, TABLENAME_STRING, 
				 indexColumnPositions, uniqueness, uuids );
	}

	/////////////////////////////////////////////////////////////////////////////
	//
	//	METHODS
	//
	/////////////////////////////////////////////////////////////////////////////

  /**
	 * Make a SYSSCHEMAS row
	 *
	 * @return	Row suitable for inserting into SYSSCHEMAS.
	 *
	 * @exception   StandardException thrown on failure
	 */

	public ExecRow makeRow(TupleDescriptor td, TupleDescriptor parent)
					throws StandardException
	{
		DataTypeDescriptor		dtd;
		ExecRow    				row;
		DataValueDescriptor		col;
		String					name = null;
		UUID						oid = null;
		String					uuid = null;	
		String					aid = null;

		if (td != null)
		{
			SchemaDescriptor	schemaDescriptor = (SchemaDescriptor)td;

			name = schemaDescriptor.getSchemaName();
			oid = schemaDescriptor.getUUID();
			if ( oid == null )
		    {
				oid = getUUIDFactory().createUUID();
				schemaDescriptor.setUUID(oid);
			}
			uuid = oid.toString();

			aid = schemaDescriptor.getAuthorizationId();
		}

		/* Build the row to insert */
		row = getExecutionFactory().getValueRow(SYSSCHEMAS_COLUMN_COUNT);

		/* 1st column is SCHEMAID */
		row.setColumn(1, new SQLChar(uuid));

		/* 2nd column is SCHEMANAME */
		row.setColumn(2, new SQLVarchar(name));

		/* 3rd column is SCHEMAAID */
		row.setColumn(3, new SQLVarchar(aid));

// GemStone changes BEGIN
    /* 4th column is default server group of this schema */
    String defaultSGs = "";
    if (td != null) {
      defaultSGs = SharedUtils.toCSV(
          ((SchemaDescriptor)td).getDefaultServerGroups());
    }
    row.setColumn(4, new SQLVarchar(defaultSGs));
// GemStone changes END
		return row;
	}


	///////////////////////////////////////////////////////////////////////////
	//
	//	ABSTRACT METHODS TO BE IMPLEMENTED BY CHILDREN OF CatalogRowFactory
	//
	///////////////////////////////////////////////////////////////////////////

	/**
	 * Make an  Tuple Descriptor out of a SYSSCHEMAS row
	 *
	 * @param row 					a SYSSCHEMAS row
	 * @param parentTupleDescriptor	unused
	 * @param dd 					dataDictionary
	 *
	 * @return	a  descriptor equivalent to a SYSSCHEMAS row
	 *
	 * @exception   StandardException thrown on failure
	 */
	public TupleDescriptor buildDescriptor(
		ExecRow					row,
		TupleDescriptor			parentTupleDescriptor,
		DataDictionary 			dd )
					throws StandardException
	{
		DataValueDescriptor			col;
		SchemaDescriptor			descriptor;
		String						name;
		UUID							id;
		String						aid;
		String						uuid;
		DataDescriptorGenerator		ddg = dd.getDataDescriptorGenerator();

		if (SanityManager.DEBUG)
		{
			SanityManager.ASSERT(row.nColumns() == SYSSCHEMAS_COLUMN_COUNT, 
								 "Wrong number of columns for a SYSSCHEMAS row");
		}

		// first column is schemaid (UUID - char(36))
		col = row.getColumn(1);
		uuid = col.getString();
		id = getUUIDFactory().recreateUUID(uuid);

		// second column is schemaname (varchar(128))
		col = row.getColumn(2);
		name = col.getString();

		// third column is auid (varchar(128))
		col = row.getColumn(3);
		aid = col.getString();

		descriptor = ddg.newSchemaDescriptor(name, aid, id);

// GemStone changes BEGIN
    col = row.getColumn(4);
    descriptor.setDefaultServerGroups(ServerGroupUtils.getServerGroups(col
        .toString()));
// GemStone changes END
		return descriptor;
	}

	/**
	 * Builds a list of columns suitable for creating this Catalog.
	 *
	 *
	 * @return array of SystemColumn suitable for making this catalog.
	 */
	public SystemColumn[]	buildColumnList() 
        throws StandardException
	{
            return new SystemColumn[] {
                SystemColumnImpl.getUUIDColumn("SCHEMAID", false),
                SystemColumnImpl.getIdentifierColumn("SCHEMANAME", false),
                SystemColumnImpl.getIdentifierColumn("AUTHORIZATIONID", false),
// GemStone changes BEGIN
                SystemColumnImpl.getColumn("DEFAULTSERVERGROUPS", Types.VARCHAR, false),
// GemStone changes END
            };
	}
}
