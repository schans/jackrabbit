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

import org.apache.jackrabbit.core.util.db.CheckSchemaOperation;

/**
 * It has the following property in addition to those of the DatabaseJournal:
 * <ul>
 * <li><code>tableSpace</code>: the MS SQL tablespace to use</li>
 * </ul>
 */
public class MSSqlDatabaseJournal extends DatabaseJournal {

    protected MSSqlDatabaseConfig dbConfig = new MSSqlDatabaseConfig();

    /**
     * Initialize this instance with the default schema and
     * driver values.
     */
    public MSSqlDatabaseJournal() {
        setDriver("com.microsoft.sqlserver.jdbc.SQLServerDriver");
        setDatabaseType("mssql");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected CheckSchemaOperation createCheckSchemaOperation() {
        return super.createCheckSchemaOperation().addVariableReplacement(
            CheckSchemaOperation.TABLE_SPACE_VARIABLE, dbConfig.getTableSpace());
    }

    /**
     * Sets the MS SQL table space.
     * @param tableSpace the MS SQL table space.
     */
    public void setTableSpace(String tableSpace) {
        dbConfig.setTableSpace(tableSpace);
    }
}
