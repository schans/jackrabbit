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

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.sql.ResultSet;
import java.sql.SQLException;

import javax.sql.DataSource;

import org.apache.commons.io.IOUtils;
import org.apache.jackrabbit.core.util.db.CheckSchemaOperation;
import org.apache.jackrabbit.core.util.db.ConnectionFactory;
import org.apache.jackrabbit.core.util.db.ConnectionHelper;
import org.apache.jackrabbit.core.util.db.DatabaseAware;
import org.apache.jackrabbit.core.util.db.DatabaseConfig;
import org.apache.jackrabbit.core.util.db.DatabaseConfigException;
import org.apache.jackrabbit.core.util.db.DbUtility;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class manages the local revision of the cluster node. It
 * persists the local revision in the LOCAL_REVISIONS table in the
 * clustering database.
 * 
 * 
 * 
 * Database-based instance revision implementation. Stores the revision inside a database
 * table named <code>LOCAL_REVISIONS</code>, whereas the table <code>GLOBAL_REVISION</code>
 * contains the highest available revision number. These tables are located inside the schema
 * specified in <code>schemaObjectPrefix</code>.
 * <p/>
 * It is configured through the following properties:
 * <ul>
 * <li><code>driver</code>: the JDBC driver class name to use; this is a required
 * property with no default value</li>
 * <li><code>url</code>: the JDBC connection url; this is a required property with
 * no default value </li>
 * <li><code>databaseType</code>: the database type to be used; if not specified, this is the
 * second field inside the JDBC connection url, delimited by colons</li>
 * <li><code>schemaObjectPrefix</code>: the schema object prefix to be used;
 * defaults to an empty string</li>
 * <li><code>user</code>: username to specify when connecting</li>
 * <li><code>password</code>: password to specify when connecting</li>
 * <li><code>schemaCheckEnabled</code>:  whether the schema check during initialization is enabled
 * (default = <code>true</code>)</li>
 * <p>
 * JNDI can be used to get the connection. In this case, use the javax.naming.InitialContext as the driver,
 * and the JNDI name as the URL. If the user and password are configured in the JNDI resource,
 * they should not be configured here. Example JNDI settings:
 * <pre>
 * &lt;param name="driver" value="javax.naming.InitialContext" />
 * &lt;param name="url" value="java:comp/env/jdbc/Test" />
 * </pre> *
 * </ul>
 */
public class DatabaseRevision implements InstanceRevision, DatabaseAware {

    /**
     * Local revisions table name, used to check schema completeness.
     */
    private static final String LOCAL_REVISIONS_TABLE = "LOCAL_REVISIONS";

    /**
     * Logger.
     */
    static Logger log = LoggerFactory.getLogger(DatabaseRevision.class);

    /**
     * The cached local revision of this cluster node.
     */
    private long localRevision;

    /**
     * Journal identifier (the cluster node name).
     */
    private final String journalId;

    /**
     * Indicates whether the init method has been called.
     */
    private boolean initialized = false;

    /**
     * The connection helper
     */
    ConnectionHelper conHelper;

    /**
     * SQL statement returning the local revision of this cluster node.
     */
    protected String getLocalRevisionStmtSQL;

    /**
     * SQL statement for inserting the local revision of this cluster node.
     */
    protected String insertLocalRevisionStmtSQL;

    /**
     * SQL statement for updating the local revision of this cluster node.
     */
    protected String updateLocalRevisionStmtSQL;

    /**
     * The repositories {@link ConnectionFactory}.
     */
    private ConnectionFactory connectionFactory;


    /**
     * The {@link DatabaseConfig}.
     */
    private DatabaseConfig dbConfig = new DatabaseConfig();
    
    /**
     * @Inherited
     */
    public void setConnectionFactory(ConnectionFactory connectionFactory) {
        this.connectionFactory = connectionFactory;
    }

    public DatabaseRevision(String id) {
        this.journalId = id;
    }

    public DatabaseRevision(String id, DatabaseConfig dbConfig) {
        this(id);
        this.dbConfig = dbConfig;
    }

    /**
     * Checks whether there's a local revision value in the database for this
     * cluster node. If not, it writes the given default revision to the database.
     *
     * @param revision the default value for the local revision counter
     * @return the local revision
     * @throws JournalException on error
     */
    protected synchronized long init(long revision) throws JournalException {
        try {
            dbConfig.initDatabaseParameters(connectionFactory);
        } catch (DatabaseConfigException e) {
            String msg = "Unable to initialize database instance revision.";
            throw new JournalException(msg, e);
        }
        initDatabaseConnection();
        initDatabaseRevision(revision);
        log.info("DatabaseRevision initialized and local revision to " + localRevision);
        return revision;
    }

    /**
     * {@inheritDoc}
     */
    public synchronized long get() {
        if (!initialized) {
            throw new IllegalStateException("instance has not yet been initialized");
        }
        return localRevision;
    }

    /**
     * {@inheritDoc}
     */
    public synchronized void set(long revision) throws JournalException {

        if (!initialized) {
            throw new IllegalStateException("instance has not yet been initialized");
        }

        // Update the cached value and the table with local revisions.
        try {
            conHelper.exec(updateLocalRevisionStmtSQL, revision, getId());
            localRevision = revision;
        } catch (SQLException e) {
            log.warn("Failed to update local revision.", e);
            throw new JournalException("Failed to update local revision.", e);
        }
    }

    public void close() {
        // nothing to do
    }

    /**
     * Return this journal's identifier.
     *
     * @return journal identifier
     */
    public String getId() {
        return journalId;
    }

    /**
     * This method is called from the {@link #initDatabaseConnection()} method of this class and
     * returns a {@link ConnectionHelper} instance which is assigned to the {@code conHelper} field.
     * Subclasses may override it to return a specialized connection helper.
     *
     * @param dataSrc the {@link DataSource} of this persistence manager
     * @return a {@link ConnectionHelper}
     * @throws Exception on error
     */
    protected ConnectionHelper createConnectionHelper(DataSource dataSrc) throws Exception {
        return new ConnectionHelper(dataSrc, false);
    }

    protected void initDatabaseConnection() throws JournalException {
        try {
            conHelper = createConnectionHelper(dbConfig.getDataSource(connectionFactory));

            // FIXME: move prepareDbIdentifier() to dbConig?
            // make sure schemaObjectPrefix consists of legal name characters only
            dbConfig.setSchemaObjectPrefix(conHelper.prepareDbIdentifier(dbConfig.getSchemaObjectPrefix()));

            // Make sure that the LOCAL_REVISIONS table exists (see JCR-1087)
            if (dbConfig.isSchemaCheckEnabled()) {
                checkLocalRevisionSchema();
            }

            buildSQLStatements();
        } catch (Exception e) {
            String msg = "Unable to create connection.";
            throw new JournalException(msg, e);
        }
    }

    protected void initDatabaseRevision(long revision) throws JournalException {
        ResultSet rs = null;
        try {
            // Check whether there is an entry in the database.
            rs = conHelper.exec(getLocalRevisionStmtSQL, new Object[] { getId() }, false, 0);
            boolean exists = rs.next();
            if (exists) {
                revision = rs.getLong(1);
            }

            // Insert the given revision in the database
            if (!exists) {
                conHelper.exec(insertLocalRevisionStmtSQL, revision, getId());
            }

            // Set the cached local revision and return
            localRevision = revision;
            initialized = true;
        } catch (SQLException e) {
            log.warn("Failed to initialize local revision.", e);
            throw new JournalException("Failed to initialize local revision", e);
        } finally {
            DbUtility.close(rs);
        }
    }

    /**
     * Checks if the local revision schema objects exist and creates them if they
     * don't exist yet.
     *
     * @throws Exception if an error occurs
     */
    private void checkLocalRevisionSchema() throws Exception {
        InputStream localRevisionDDLStream = null;
        InputStream in = DatabaseRevision.class.getResourceAsStream(dbConfig.getDatabaseType() + ".ddl");
        try {
            BufferedReader reader = new BufferedReader(new InputStreamReader(in));
            String sql = reader.readLine();
            while (sql != null) {
                // Skip comments and empty lines, and select only the statement to create the LOCAL_REVISIONS
                // table.
                if (!sql.startsWith("#") && sql.length() > 0 && sql.indexOf(LOCAL_REVISIONS_TABLE) != -1) {
                    localRevisionDDLStream = new ByteArrayInputStream(sql.getBytes());
                    break;
                }
                // read next sql stmt
                sql = reader.readLine();
            }
        } finally {
            IOUtils.closeQuietly(in);
        }
        // Run the schema check for the single table
        new CheckSchemaOperation(conHelper, localRevisionDDLStream, dbConfig.getSchemaObjectPrefix() + LOCAL_REVISIONS_TABLE)
                .addVariableReplacement(CheckSchemaOperation.SCHEMA_OBJECT_PREFIX_VARIABLE, dbConfig.getSchemaObjectPrefix()).run();
    }

    /**
     * Builds the SQL statements. May be overridden by subclasses to allow
     * different table and/or column names.
     */
    protected void buildSQLStatements() {
        getLocalRevisionStmtSQL = "select REVISION_ID from " + dbConfig.getSchemaObjectPrefix() + "LOCAL_REVISIONS "
                + "where JOURNAL_ID = ?";
        insertLocalRevisionStmtSQL = "insert into " + dbConfig.getSchemaObjectPrefix() + "LOCAL_REVISIONS "
                + "(REVISION_ID, JOURNAL_ID) values (?,?)";
        updateLocalRevisionStmtSQL = "update " + dbConfig.getSchemaObjectPrefix() + "LOCAL_REVISIONS "
                + "set REVISION_ID = ? where JOURNAL_ID = ?";
    }


    // ------ Bean setters

    public void setDriver(String driver) {
        dbConfig.setDriver(driver);
    }

    public void setUrl(String url) {
        dbConfig.setUrl(url);
    }

    public void setUser(String user) {
        dbConfig.setUrl(user);
    }

    public void setPassword(String password) {
        dbConfig.setPassword(password);
    }

    public void setDatabaseType(String databaseType) {
        dbConfig.setDatabaseType(databaseType);
    }

    public void setDataSourceName(String dataSourceName) {
        dbConfig.setDataSourceName(dataSourceName);
    }

    public void setSchemaObjectPrefix(String schemaObjectPrefix) {
        dbConfig.setSchemaObjectPrefix(schemaObjectPrefix);
    }

    public void setSchemaCheckEnabled(boolean enabled) {
        dbConfig.setSchemaCheckEnabled(enabled);
    }

}
