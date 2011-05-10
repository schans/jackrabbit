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

import java.io.File;
import java.io.InputStream;
import java.sql.SQLException;

import javax.jcr.RepositoryException;
import javax.sql.DataSource;

import org.apache.jackrabbit.core.util.db.ConnectionFactory;
import org.apache.jackrabbit.core.util.db.ConnectionHelper;
import org.apache.jackrabbit.core.util.db.DatabaseAware;
import org.apache.jackrabbit.spi.commons.namespace.NamespaceResolver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Database-based journal implementation. Stores records inside a database table named
 * <code>JOURNAL</code>, whereas the table <code>GLOBAL_REVISION</code> contains the
 * highest available revision number. These tables are located inside the schema specified
 * in <code>schemaObjectPrefix</code>.
 * <p/>
 * It is configured through the following properties:
 * <ul>
 * <li><code>revision</code>: the filename where the parent cluster node's revision
 * file should be written to; this is a required property with no default value</li>
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
 * <li><code>reconnectDelayMs</code>: number of milliseconds to wait before
 * trying to reconnect to the database.</li>
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
public class DatabaseSlaveJournal extends AbstractJournal implements DatabaseAware {

    /**
     * Default instance revision file name.
     */
    public static final String DEFAULT_INSTANCE_FILE_NAME = "revision.log";

    /**
     * Logger.
     */
    static Logger log = LoggerFactory.getLogger(DatabaseSlaveJournal.class);

    /**
     * Driver name, bean property.
     */
    private String driver;

    /**
     * Connection URL, bean property.
     */
    private String url;

    /**
     * Database type, bean property.
     */
    private String databaseType;

    /**
     * User name, bean property.
     */
    private String user;

    /**
     * Password, bean property.
     */
    private String password;

    /**
     * DataSource logical name, bean property.
     */
    private String dataSourceName;

    /**
     * The connection helper
     */
    ConnectionHelper conHelper;

    /**
     * Whether the schema check must be done during initialization.
     */
    private boolean schemaCheckEnabled = true;

    /**
     * SQL statement returning all revisions within a range.
     */
    protected String selectRevisionsStmtSQL;

    /**
     * SQL statement returning the global revision.
     */
    protected String selectGlobalStmtSQL;

    /**
     * Schema object prefix, bean property.
     */
    protected String schemaObjectPrefix;

    /**
     * The repositories {@link ConnectionFactory}.
     */
    private ConnectionFactory connectionFactory;

    public DatabaseSlaveJournal() {
        databaseType = "default";
        schemaObjectPrefix = "";
    }

    /**
     * {@inheritDoc}
     */
    public void setConnectionFactory(ConnectionFactory connnectionFactory) {
        this.connectionFactory = connnectionFactory;
    }

    /**
     * {@inheritDoc}
     */
    public void init(String id, NamespaceResolver resolver)
            throws JournalException {

        super.init(id, resolver);

        init();

        try {
            conHelper = createConnectionHelper(getDataSource());

            // make sure schemaObjectPrefix consists of legal name characters only
            schemaObjectPrefix = conHelper.prepareDbIdentifier(schemaObjectPrefix);

            buildSQLStatements();
        } catch (Exception e) {
            String msg = "Unable to create connection.";
            throw new JournalException(msg, e);
        }
        log.info("DatabaseJournal initialized.");
        
        if (getRevision() == null) {
            File repHome = getRepositoryHome();
            if (repHome == null) {
                String msg = "Revision not specified.";
                throw new JournalException(msg);
            }
            String revision = new File(repHome, DEFAULT_INSTANCE_FILE_NAME).getPath();
            log.info("Revision not specified, using: " + revision);
            setRevision(revision);
        }
    }

    private DataSource getDataSource() throws Exception {
        if (getDataSourceName() == null || "".equals(getDataSourceName())) {
            return connectionFactory.getDataSource(getDriver(), getUrl(), getUser(), getPassword());
        } else {
            return connectionFactory.getDataSource(dataSourceName);
        }
    }

    /**
     * This method is called from the {@link #init(String, NamespaceResolver)} method of this class and
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

    /**
     * Completes initialization of this database journal. Base implementation
     * checks whether the required bean properties <code>driver</code> and
     * <code>url</code> have been specified and optionally deduces a valid
     * database type. Should be overridden by subclasses that use a different way to
     * create a connection and therefore require other arguments.
     *
     * @see #getConnection()
     * @throws JournalException if initialization fails
     */
    protected void init() throws JournalException {
        if (driver == null && dataSourceName == null) {
            String msg = "Driver not specified.";
            throw new JournalException(msg);
        }
        if (url == null && dataSourceName == null) {
            String msg = "Connection URL not specified.";
            throw new JournalException(msg);
        }
        if (dataSourceName != null) {
            try {
                String configuredDatabaseType = connectionFactory.getDataBaseType(dataSourceName);
                if (DatabaseSlaveJournal.class.getResourceAsStream(configuredDatabaseType + ".ddl") != null) {
                    setDatabaseType(configuredDatabaseType);
                }
            } catch (RepositoryException e) {
                throw new JournalException("failed to get database type", e);
            }
        }
        if (databaseType == null) {
            try {
                databaseType = getDatabaseTypeFromURL(url);
            } catch (IllegalArgumentException e) {
                String msg = "Unable to derive database type from URL: " + e.getMessage();
                throw new JournalException(msg);
            }
        }
    }

    /**
     * {@inheritDoc}
     */
    public InstanceRevision getInstanceRevision() throws JournalException {
        return new FileRevision(new File(getRevision()));
    }

    /**
     * Derive a database type from a JDBC connection URL. This simply treats the given URL
     * as delimeted by colons and takes the 2nd field.
     *
     * @param url JDBC connection URL
     * @return the database type
     * @throws IllegalArgumentException if the JDBC connection URL is invalid
     */
    private static String getDatabaseTypeFromURL(String url) throws IllegalArgumentException {
        int start = url.indexOf(':');
        if (start != -1) {
            int end = url.indexOf(':', start + 1);
            if (end != -1) {
                return url.substring(start + 1, end);
            }
        }
        throw new IllegalArgumentException(url);
    }

    /**
     * {@inheritDoc}
     */
    public RecordIterator getRecords(long startRevision) throws JournalException {
        try {
            return new DatabaseRecordIterator(conHelper.exec(selectRevisionsStmtSQL, new Object[]{new Long(
                    startRevision)}, false, 0), getResolver(), getNamePathResolver());
        } catch (SQLException e) {
            throw new JournalException("Unable to return record iterator.", e);
        }
    }

    /**
     * {@inheritDoc}
     */
    public RecordIterator getRecords() throws JournalException {
        try {
            return new DatabaseRecordIterator(conHelper.exec(selectRevisionsStmtSQL, new Object[]{new Long(
                    Long.MIN_VALUE)}, false, 0), getResolver(), getNamePathResolver());
        } catch (SQLException e) {
            throw new JournalException("Unable to return record iterator.", e);
        }
    }

    /**
     * Disable writes. This method is only called from {@link DefaultRecordProducer#append()} method.
     */
    @Override
    public void lockAndSync() throws JournalException {
        throw new JournalException("Writes are not allowed on the slave node.");
    }

    /**
     * Disable writes. This method is only called from {@link DefaultRecordProducer#append()} method.
     */
    protected void doLock() throws JournalException {
        throw new JournalException("Writes are not allowed on the slave node.");
    }

    /**
     * {@inheritDoc}
     */
    protected void doUnlock(boolean successful) {
    }


    /**
     * {@inheritDoc}
     * <p/>
     * Save away the locked revision inside the newly appended record.
     */
    protected void appending(AppendRecord record) {
//        record.setRevision(lockedRevision);
    }

    /**
     * {@inheritDoc}
     * <p/>
     * We have already saved away the revision for this record.
     */
    protected void append(AppendRecord record, InputStream in, int length)
            throws JournalException {
//
//        try {
//            conHelper.exec(insertRevisionStmtSQL, record.getRevision(), getId(), record.getProducerId(),
//                new StreamWrapper(in, length));
//
//        } catch (SQLException e) {
//            String msg = "Unable to append revision " + lockedRevision + ".";
//            throw new JournalException(msg, e);
//        }
    }

    /**
     * {@inheritDoc}
     */
    public void close() {
    }

    /**
     * Builds the SQL statements. May be overridden by subclasses to allow
     * different table and/or column names.
     */
    protected void buildSQLStatements() {
        selectRevisionsStmtSQL =
            "select REVISION_ID, JOURNAL_ID, PRODUCER_ID, REVISION_DATA from "
            + schemaObjectPrefix + "JOURNAL where REVISION_ID > ? order by REVISION_ID";
        selectGlobalStmtSQL =
            "select REVISION_ID from "
            + schemaObjectPrefix + "GLOBAL_REVISION";
    }

    /**
     * Bean getters
     */
    public String getDriver() {
        return driver;
    }

    public String getUrl() {
        return url;
    }

    /**
     * Get the database type.
     *
     * @return the database type
     */
    public String getDatabaseType() {
        return databaseType;
    }

    /**
     * Get the database type.
     * @deprecated
     * This method is deprecated; {@link #getDatabaseType} should be used instead.
     *
     * @return the database type
     */
    public String getSchema() {
        return databaseType;
    }

    public String getSchemaObjectPrefix() {
        return schemaObjectPrefix;
    }

    public String getUser() {
        return user;
    }

    public String getPassword() {
        return password;
    }

    /**
     * Bean setters
     */
    public void setDriver(String driver) {
        this.driver = driver;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    /**
     * Set the database type.
     *
     * @param databaseType the database type
     */
    public void setDatabaseType(String databaseType) {
        this.databaseType = databaseType;
    }

    /**
     * Set the database type.
    * @deprecated
    * This method is deprecated; {@link #getDatabaseType} should be used instead.
     *
     * @param databaseType the database type
     */
    public void setSchema(String databaseType) {
        this.databaseType = databaseType;
    }

    public void setSchemaObjectPrefix(String schemaObjectPrefix) {
        this.schemaObjectPrefix = schemaObjectPrefix.toUpperCase();
    }

    public void setUser(String user) {
        this.user = user;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public String getDataSourceName() {
        return dataSourceName;
    }

    public void setDataSourceName(String dataSourceName) {
        this.dataSourceName = dataSourceName;
    }

    /**
     * @return whether the schema check is enabled
     */
    public final boolean isSchemaCheckEnabled() {
        return schemaCheckEnabled;
    }

    /**
     * @param enabled set whether the schema check is enabled
     */
    public final void setSchemaCheckEnabled(boolean enabled) {
        schemaCheckEnabled = enabled;
    }

}
