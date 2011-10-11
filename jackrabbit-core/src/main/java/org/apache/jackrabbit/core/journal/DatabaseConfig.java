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

import javax.jcr.RepositoryException;
import javax.sql.DataSource;

import org.apache.jackrabbit.core.util.db.ConnectionFactory;
import org.apache.jackrabbit.core.util.db.ConnectionHelper;
import org.apache.jackrabbit.spi.commons.namespace.NamespaceResolver;
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
public class DatabaseConfig {

    /**
     * Logger.
     */
    static Logger log = LoggerFactory.getLogger(DatabaseConfig.class);

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
     * Schema object prefix, bean property.
     */
    protected String schemaObjectPrefix;

    /**
     * Whether the schema check must be done during initialization, bean property.
     */
    private boolean schemaCheckEnabled = true;

    public DatabaseConfig() {
        databaseType = "default";
        schemaObjectPrefix = "";
    }

    /**
     * Checks whether there's a local revision value in the database for this
     * cluster node. If not, it writes the given default revision to the database.
     *
     * @param revision the default value for the local revision counter
     * @return the local revision
     * @throws DatabaseConfigException on error
     */
    protected synchronized void init(ConnectionFactory connectionFactory) throws DatabaseConfigException {
        initDatabaseParameters(connectionFactory);
    }


    protected DataSource getDataSource(ConnectionFactory connectionFactory) throws Exception {
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
    protected void initDatabaseParameters(ConnectionFactory connectionFactory) throws DatabaseConfigException {
        if (driver == null && dataSourceName == null) {
            String msg = "Driver not specified.";
            throw new DatabaseConfigException(msg);
        }
        if (url == null && dataSourceName == null) {
            String msg = "Connection URL not specified.";
            throw new DatabaseConfigException(msg);
        }
        if (dataSourceName != null) {
            try {
                String configuredDatabaseType = connectionFactory.getDataBaseType(dataSourceName);
                if (DatabaseConfig.class.getResourceAsStream(configuredDatabaseType + ".ddl") != null) {
                    setDatabaseType(configuredDatabaseType);
                }
            } catch (RepositoryException e) {
                throw new DatabaseConfigException("failed to get database type", e);
            }
        }
        if (databaseType == null) {
            try {
                databaseType = getDatabaseTypeFromURL(url);
            } catch (IllegalArgumentException e) {
                String msg = "Unable to derive database type from URL: " + e.getMessage();
                throw new DatabaseConfigException(msg);
            }
        }
    }

    /**
     * Derive a database type from a JDBC connection URL. This simply treats the given URL
     * as delimited by colons and takes the 2nd field.
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


    // ------ Bean getters

    public String getDriver() {
        return driver;
    }

    public String getUrl() {
        return url;
    }

    public String getUser() {
        return user;
    }

    public String getPassword() {
        return password;
    }

    public String getDatabaseType() {
        return databaseType;
    }

    public String getDataSourceName() {
        return dataSourceName;
    }

    public String getSchemaObjectPrefix() {
        return schemaObjectPrefix;
    }

    public final boolean isSchemaCheckEnabled() {
        return schemaCheckEnabled;
    }

    // ------ Bean setters

    public void setDriver(String driver) {
        this.driver = driver;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public void setUser(String user) {
        this.user = user;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public void setDatabaseType(String databaseType) {
        this.databaseType = databaseType;
    }

    public void setDataSourceName(String dataSourceName) {
        this.dataSourceName = dataSourceName;
    }

    public void setSchemaObjectPrefix(String schemaObjectPrefix) {
        this.schemaObjectPrefix = schemaObjectPrefix.toUpperCase();
    }

    public final void setSchemaCheckEnabled(boolean enabled) {
        schemaCheckEnabled = enabled;
    }

}
