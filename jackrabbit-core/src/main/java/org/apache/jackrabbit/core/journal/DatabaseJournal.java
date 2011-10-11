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
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Calendar;

import javax.sql.DataSource;

import org.apache.jackrabbit.core.util.db.CheckSchemaOperation;
import org.apache.jackrabbit.core.util.db.ConnectionFactory;
import org.apache.jackrabbit.core.util.db.ConnectionHelper;
import org.apache.jackrabbit.core.util.db.DatabaseAware;
import org.apache.jackrabbit.core.util.db.DatabaseConfig;
import org.apache.jackrabbit.core.util.db.DbUtility;
import org.apache.jackrabbit.core.util.db.StreamWrapper;
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
 * <li><code>janitorEnabled</code>: specifies whether the clean-up thread for the
 * journal table is enabled (default = <code>false</code>)</li>
 * <li><code>janitorSleep</code>: specifies the sleep time of the clean-up thread
 * in seconds (only useful when the clean-up thread is enabled, default = 24 * 60 * 60,
 * which equals 24 hours)</li>
 * <li><code>janitorFirstRunHourOfDay</code>: specifies the hour at which the clean-up
 * thread initiates its first run (default = <code>3</code> which means 3:00 at night)</li>
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
public class DatabaseJournal extends AbstractJournal implements DatabaseAware {

    /**
     * Default journal table name, used to check schema completeness.
     */
    private static final String DEFAULT_JOURNAL_TABLE = "JOURNAL";

    /**
     * Logger.
     */
    static Logger log = LoggerFactory.getLogger(DatabaseJournal.class);

    /**
     * The connection helper
     */
    ConnectionHelper conHelper;

    /**
     * Auto commit level.
     */
    private int lockLevel;

    /**
     * Locked revision.
     */
    private long lockedRevision;

    /**
     * Whether the revision table janitor thread is enabled.
     */
    private boolean janitorEnabled = false;

    /**
     * The sleep time of the revision table janitor in seconds, 1 day default.
     */
    int janitorSleep = 60 * 60 * 24;

    /**
     * Indicates when the next run of the janitor is scheduled.
     * The first run is scheduled by default at 03:00 hours.
     */
    Calendar janitorNextRun = Calendar.getInstance();

    {
        if (janitorNextRun.get(Calendar.HOUR_OF_DAY) >= 3) {
            janitorNextRun.add(Calendar.DAY_OF_MONTH, 1);
        }
        janitorNextRun.set(Calendar.HOUR_OF_DAY, 3);
        janitorNextRun.set(Calendar.MINUTE, 0);
        janitorNextRun.set(Calendar.SECOND, 0);
        janitorNextRun.set(Calendar.MILLISECOND, 0);
    }

    private Thread janitorThread;

    /**
     * The instance that manages the local revision.
     */
    private InstanceRevision instanceRevision;

    /**
     * SQL statement returning all revisions within a range.
     */
    protected String selectRevisionsStmtSQL;

    /**
     * SQL statement updating the global revision.
     */
    protected String updateGlobalStmtSQL;

    /**
     * SQL statement returning the global revision.
     */
    protected String selectGlobalStmtSQL;

    /**
     * SQL statement appending a new record.
     */
    protected String insertRevisionStmtSQL;

    /**
     * SQL statement returning the minimum of the local revisions.
     */
    protected String selectMinLocalRevisionStmtSQL;

    /**
     * SQL statement removing a set of revisions with from the journal table.
     */
    protected String cleanRevisionStmtSQL;

    /**
     * The repositories {@link ConnectionFactory}.
     */
    private ConnectionFactory connectionFactory;

    /**
     * The {@link DatabaseConfig}.
     */
    protected DatabaseConfig dbConfig = new DatabaseConfig();

    /**
     * {@inheritDoc}
     */
    public void setConnectionFactory(ConnectionFactory connectionFactory) {
        this.connectionFactory = connectionFactory;
    }

    /**
     * {@inheritDoc}
     */
    public void init(String id, NamespaceResolver resolver)
            throws JournalException {

        super.init(id, resolver);

        try {
            dbConfig.init(connectionFactory);
            conHelper = createConnectionHelper(dbConfig.getDataSource(connectionFactory));

            // FIXME: move prepareDbIdentifier() to dbConig?
            // make sure schemaObjectPrefix consists of legal name characters only
            dbConfig.setSchemaObjectPrefix(conHelper.prepareDbIdentifier(dbConfig.getSchemaObjectPrefix()));

            // check if schema objects exist and create them if necessary
            if (dbConfig.isSchemaCheckEnabled() && !isReadOnly()) {
                createCheckSchemaOperation().run();
            }

            buildSQLStatements();
            initInstanceRevision();
            initJanitor();
        } catch (Exception e) {
            String msg = "Unable to create connection.";
            throw new JournalException(msg, e);
        }
        log.info("DatabaseJournal initialized.");
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
     * This method is called from {@link #init(String, NamespaceResolver)} after the
     * {@link #createConnectionHelper(DataSource)} method, and returns a default {@link CheckSchemaOperation}.
     * Subclasses can override this implementation to get a customized implementation.
     *
     * @return a new {@link CheckSchemaOperation} instance
     */
    protected CheckSchemaOperation createCheckSchemaOperation() {
        InputStream in = DatabaseJournal.class.getResourceAsStream(dbConfig.getDatabaseType() + ".ddl");
        return new CheckSchemaOperation(conHelper, in, dbConfig.getSchemaObjectPrefix() + DEFAULT_JOURNAL_TABLE).addVariableReplacement(
            CheckSchemaOperation.SCHEMA_OBJECT_PREFIX_VARIABLE, dbConfig.getSchemaObjectPrefix());
    }

    
    /**
     * Initialize the instance revision for managing the local revision
     * of the cluster node.
     * @throws JournalException
     */
    protected void initInstanceRevision() throws JournalException {
        // TODO: Create own config for instance revision.
        if (isReadOnly()) {
            // TODO: add option for storing revisions in master db.
            instanceRevision = new FileRevision(getFileRevisionFile());
        } else {
            // This is the normal cluster behavior, which uses one database for the 
            // global and local revisions.
            DatabaseRevision databaseRevision = new DatabaseRevision(getId(), dbConfig);
            databaseRevision.setConnectionFactory(connectionFactory);
            // Now write the localFileRevision (or 0 if it does not exist) to the LOCAL_REVISIONS
            // table, but only if the LOCAL_REVISIONS table has no entry yet for this cluster node
            databaseRevision.init(getInitialRevision());
            instanceRevision = databaseRevision;
        }
    }

    private File getFileRevisionFile() throws JournalException {
        if (getRevision() == null) {
            File repHome = getRepositoryHome();
            if (repHome == null) {
                String msg = "Revision not specified.";
                throw new JournalException(msg);
            }
            String revision = new File(repHome, FileRevision.DEFAULT_INSTANCE_FILE_NAME).getPath();
            log.info("Revision not specified, using: " + revision);
            setRevision(revision);
        }
        return new File(getRevision());
    }

    /**
     * Get the initial default revision. The default is zero unless this
     * repository is getting upgraded. In that case the revision is read
     * from the file system. See JCR-1087 for the full story.
     * @return 0L or the file based revision number for upgrading.
     * @throws JournalException
     */
    private long getInitialRevision() throws JournalException {
        long localFileRevision = 0L;
        if (getRevision() != null) {
            InstanceRevision currentFileRevision = new FileRevision(new File(getRevision()));
            localFileRevision = currentFileRevision.get();
            currentFileRevision.close();
        }
        return localFileRevision;
    }

    /* (non-Javadoc)
     * @see org.apache.jackrabbit.core.journal.Journal#getInstanceRevision()
     */
    public InstanceRevision getInstanceRevision() throws JournalException {
        return instanceRevision;
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
     * Synchronize contents from journal. May be overridden by subclasses.
     * Override to do it in batchMode, since some databases (PSQL) when
     * not in transactional mode, load all results in memory which causes
     * out of memory.
     *
     * @param startRevision start point (exclusive)
     * @throws JournalException if an error occurs
     */
    @Override
    protected void doSync(long startRevision) throws JournalException {
        try {
            startBatch();
            try {
                super.doSync(startRevision);
            } finally {
                endBatch(true);
            }
        } catch (SQLException e) {
            throw new JournalException("Couldn't sync the cluster node", e);
        }
    }

    /**
     * {@inheritDoc}
     * <p/>
     * This journal is locked by incrementing the current value in the table
     * named <code>GLOBAL_REVISION</code>, which effectively write-locks this
     * table. The updated value is then saved away and remembered in the
     * appended record, because a save may entail multiple appends (JCR-884).
     */
    protected void doLock() throws JournalException {
        ResultSet rs = null;
        boolean succeeded = false;

        try {
            startBatch();
        } catch (SQLException e) {
            throw new JournalException("Unable to set autocommit to false.", e);
        }

        try {
            conHelper.exec(updateGlobalStmtSQL);
            rs = conHelper.exec(selectGlobalStmtSQL, null, false, 0);
            if (!rs.next()) {
                 throw new JournalException("No revision available.");
            }
            lockedRevision = rs.getLong(1);
            succeeded = true;
        } catch (SQLException e) {
            throw new JournalException("Unable to lock global revision table.", e);
        } finally {
            DbUtility.close(rs);
            if (!succeeded) {
                doUnlock(false);
            }
        }
    }

    /**
     * {@inheritDoc}
     */
    protected void doUnlock(boolean successful) {
        endBatch(successful);
    }

    private void startBatch() throws SQLException {
        if (lockLevel++ == 0) {
            conHelper.startBatch();
        }
    }

    private void endBatch(boolean successful) {
        if (--lockLevel == 0) {
            try {
                conHelper.endBatch(successful);;
            } catch (SQLException e) {
                log.error("failed to end batch", e);
            }
        }
    }

    /**
     * {@inheritDoc}
     * <p/>
     * Save away the locked revision inside the newly appended record.
     */
    protected void appending(AppendRecord record) {
        record.setRevision(lockedRevision);
    }

    /**
     * {@inheritDoc}
     * <p/>
     * We have already saved away the revision for this record.
     */
    protected void append(AppendRecord record, InputStream in, int length)
            throws JournalException {

        try {
            conHelper.exec(insertRevisionStmtSQL, record.getRevision(), getId(), record.getProducerId(),
                new StreamWrapper(in, length));

        } catch (SQLException e) {
            String msg = "Unable to append revision " + lockedRevision + ".";
            throw new JournalException(msg, e);
        }
    }

    /**
     * {@inheritDoc}
     */
    public void close() {
        if (janitorThread != null) {
            janitorThread.interrupt();
        }
    }

    /**
     * Builds the SQL statements. May be overridden by subclasses to allow
     * different table and/or column names.
     */
    protected void buildSQLStatements() {
        selectRevisionsStmtSQL =
            "select REVISION_ID, JOURNAL_ID, PRODUCER_ID, REVISION_DATA from "
            + dbConfig.getSchemaObjectPrefix() + "JOURNAL where REVISION_ID > ? order by REVISION_ID";
        updateGlobalStmtSQL =
            "update " + dbConfig.getSchemaObjectPrefix() + "GLOBAL_REVISION"
            + " set REVISION_ID = REVISION_ID + 1";
        selectGlobalStmtSQL =
            "select REVISION_ID from "
            + dbConfig.getSchemaObjectPrefix() + "GLOBAL_REVISION";
        insertRevisionStmtSQL =
            "insert into " + dbConfig.getSchemaObjectPrefix() + "JOURNAL"
            + " (REVISION_ID, JOURNAL_ID, PRODUCER_ID, REVISION_DATA) "
            + "values (?,?,?,?)";
        selectMinLocalRevisionStmtSQL =
            "select MIN(REVISION_ID) from " + dbConfig.getSchemaObjectPrefix() + "LOCAL_REVISIONS";
        cleanRevisionStmtSQL =
            "delete from " + dbConfig.getSchemaObjectPrefix() + "JOURNAL " + "where REVISION_ID < ?";
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

    // ------ JANITOR

    public boolean getJanitorEnabled() {
        return janitorEnabled;
    }

    public int getJanitorSleep() {
        return janitorSleep;
    }

    public int getJanitorFirstRunHourOfDay() {
        return janitorNextRun.get(Calendar.HOUR_OF_DAY);
    }

    public void setJanitorEnabled(boolean enabled) {
        this.janitorEnabled = enabled;
    }

    public void setJanitorSleep(int sleep) {
        this.janitorSleep = sleep;
    }

    /**
     * Initialize the instance revision manager and the janitor thread.
     *
     * @throws JournalException on error
     */
    protected void initJanitor() throws Exception {
        // Start the clean-up thread if necessary.
        if (janitorEnabled && !isReadOnly()) {
            janitorThread = new Thread(new RevisionTableJanitor(), "Jackrabbit-ClusterRevisionJanitor");
            janitorThread.setDaemon(true);
            janitorThread.start();
            log.info("Cluster revision janitor thread started; first run scheduled at " + janitorNextRun.getTime());
        } else {
            log.info("Cluster revision janitor thread not started");
        }
    }

    public void setJanitorFirstRunHourOfDay(int hourOfDay) {
        janitorNextRun = Calendar.getInstance();
        if (janitorNextRun.get(Calendar.HOUR_OF_DAY) >= hourOfDay) {
            janitorNextRun.add(Calendar.DAY_OF_MONTH, 1);
        }
        janitorNextRun.set(Calendar.HOUR_OF_DAY, hourOfDay);
        janitorNextRun.set(Calendar.MINUTE, 0);
        janitorNextRun.set(Calendar.SECOND, 0);
        janitorNextRun.set(Calendar.MILLISECOND, 0);
    }

    /**
     * Class for maintaining the revision table. This is only useful if all
     * JR information except the search index is in the database (i.e., node types
     * etc). In that case, revision data can safely be thrown away from the JOURNAL table.
     */
    public class RevisionTableJanitor implements Runnable {

        /**
         * {@inheritDoc}
         */
        public void run() {
            while (!Thread.currentThread().isInterrupted()) {
                try {
                    log.info("Next clean-up run scheduled at " + janitorNextRun.getTime());
                    long sleepTime = janitorNextRun.getTimeInMillis() - System.currentTimeMillis();
                    if (sleepTime > 0) {
                        Thread.sleep(sleepTime);
                    }
                    cleanUpOldRevisions();
                    janitorNextRun.add(Calendar.SECOND, janitorSleep);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
            log.info("Interrupted: stopping clean-up task.");
        }

        /**
         * Cleans old revisions from the clustering table.
         */
        protected void cleanUpOldRevisions() {
            ResultSet rs = null;
            try {
                long minRevision = 0;
                rs = conHelper.exec(selectMinLocalRevisionStmtSQL, null, false, 0);
                boolean cleanUp = rs.next();
                if (cleanUp) {
                    minRevision = rs.getLong(1);
                }

                // Clean up if necessary:
                if (cleanUp) {
                    conHelper.exec(cleanRevisionStmtSQL, minRevision);
                    log.info("Cleaned old revisions up to revision " + minRevision + ".");
                }

            } catch (Exception e) {
                log.warn("Failed to clean up old revisions.", e);
            } finally {
                DbUtility.close(rs);
            }
        }
    }
}
