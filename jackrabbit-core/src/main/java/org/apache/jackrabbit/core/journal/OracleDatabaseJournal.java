/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.jackrabbit.core.journal;

import javax.sql.DataSource;

import org.apache.jackrabbit.core.util.db.CheckSchemaOperation;
import org.apache.jackrabbit.core.util.db.ConnectionHelper;
import org.apache.jackrabbit.core.util.db.OracleConnectionHelper;
import org.apache.jackrabbit.core.util.db.OracleDatabaseConfig;

/**
 * It has the following properties in addition to those of the DatabaseJournal:
 * <ul>
 * <li><code>tablespace</code>: the tablespace to use for tables</li>
 * <li><code>indexTablespace</code>: the tablespace to use for indexes</li>
 * </ul>
 */
public class OracleDatabaseJournal extends DatabaseJournal {

    /**
     * Name of the replacement variable in the DDL for {@link #tablespace}.
     */
    protected static final String TABLESPACE_VARIABLE = "${tablespace}";

    /**
     * Name of the replacement variable in the DDL for {@link #indexTablespace}.
     */
    protected static final String INDEX_TABLESPACE_VARIABLE = "${indexTablespace}";

    protected OracleDatabaseConfig dbConfig = new OracleDatabaseConfig();

    /**
     * {@inheritDoc}
     */
    @Override
    protected CheckSchemaOperation createCheckSchemaOperation() {
        String tablespace = dbConfig.getTablespace();
        String indexTablespace = dbConfig.getIndexTablespace();
        String defaultTableSpaceClause = OracleDatabaseConfig.DEFAULT_TABLESPACE_CLAUSE;

        if (defaultTableSpaceClause.equals(indexTablespace) && !defaultTableSpaceClause.equals(tablespace)) {
            // tablespace was set but not indexTablespace : use the same for both
            indexTablespace = tablespace;
        }
        return super.createCheckSchemaOperation().addVariableReplacement(TABLESPACE_VARIABLE, tablespace)
                .addVariableReplacement(INDEX_TABLESPACE_VARIABLE, indexTablespace);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected ConnectionHelper createConnectionHelper(DataSource dataSrc) throws Exception {
        OracleConnectionHelper helper = new OracleConnectionHelper(dataSrc, false);
        helper.init();
        return helper;
    }

    // ------ Bean setters
    /**
     * Sets the Oracle tablespace for tables.
     * @param tablespaceName the Oracle tablespace for tables.
     */
    public void setTablespace(String tablespaceName) {
        dbConfig.setTablespace(tablespaceName);
    }

    /**
     * Sets the Oracle tablespace for indexes.
     * @param tablespace the Oracle tablespace for indexes.
     */
    public void setIndexTablespace(String tablespaceName) {
        dbConfig.setIndexTablespace(tablespaceName);
    }

}
