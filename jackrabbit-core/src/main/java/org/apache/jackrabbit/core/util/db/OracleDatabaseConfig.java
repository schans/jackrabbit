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
package org.apache.jackrabbit.core.util.db;

/**
 * It has the following properties in addition to those of the DatabaseConfig:
 * <ul>
 * <li><code>tablespace</code>: the tablespace to use for tables</li>
 * <li><code>indexTablespace</code>: the tablespace to use for indexes</li>
 * </ul>
 */
public class OracleDatabaseConfig extends DatabaseConfig {

    /**
     * The default tablespace clause used when {@link #tablespace} or {@link #indexTablespace}
     * are not specified.
     */
    protected static final String DEFAULT_TABLESPACE_CLAUSE = "";

    /** The Oracle tablespace to use for tables */
    protected String tablespace;

    /** The Oracle tablespace to use for indexes */
    protected String indexTablespace;

    public OracleDatabaseConfig() {
        setDatabaseType("oracle");
        setDriver("oracle.jdbc.OracleDriver");
        setSchemaObjectPrefix("");
        tablespace = DEFAULT_TABLESPACE_CLAUSE;
        indexTablespace = DEFAULT_TABLESPACE_CLAUSE;
    }

    /**
     * Returns the configured Oracle tablespace for tables.
     * @return the configured Oracle tablespace for tables.
     */
    public String getTablespace() {
        return tablespace;
    }

    /**
     * Sets the Oracle tablespace for tables.
     * @param tablespaceName the Oracle tablespace for tables.
     */
    public void setTablespace(String tablespaceName) {
        this.tablespace = this.buildTablespaceClause(tablespaceName);
    }

    /**
     * Returns the configured Oracle tablespace for indexes.
     * @return the configured Oracle tablespace for indexes.
     */
    public String getIndexTablespace() {
        return indexTablespace;
    }

    /**
     * Sets the Oracle tablespace for indexes.
     * @param tablespace the Oracle tablespace for indexes.
     */
    public void setIndexTablespace(String tablespaceName) {
        this.indexTablespace = this.buildTablespaceClause(tablespaceName);
    }

    /**
     * Constructs the <code>tablespace &lt;tbs name&gt;</code> clause from
     * the supplied tablespace name. If the name is empty, {@link #DEFAULT_TABLESPACE_CLAUSE}
     * is returned instead.
     * 
     * @param tablespaceName A tablespace name
     * @return A tablespace clause using the supplied name or
     * <code>{@value #DEFAULT_TABLESPACE_CLAUSE}</code> if the name is empty
     */
    private String buildTablespaceClause(String tablespaceName) {
        if (tablespaceName == null || tablespaceName.trim().length() == 0) {
            return DEFAULT_TABLESPACE_CLAUSE;
        } else {
            return "tablespace " + tablespaceName.trim();
        }
    }

}
