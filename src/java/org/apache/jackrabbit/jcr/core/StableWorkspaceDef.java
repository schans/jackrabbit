/*
 * Copyright 2004 The Apache Software Foundation.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.jackrabbit.jcr.core;

import org.apache.jackrabbit.jcr.fs.FileSystem;

import java.util.HashMap;

/**
 * A <code>StableWorkspaceDef</code> defines a stable workspace, i.e.
 * a workspace that physically stores all items that can be accessed
 * through it.
 *
 * @author Stefan Guggisberg
 * @version $Revision: 1.13 $, $Date: 2004/08/02 16:19:41 $
 * @see WorkspaceDef
 * @see DynamicWorkspaceDef
 */
public class StableWorkspaceDef extends WorkspaceDef {

    private DynamicWorkspaceDef[] dynWorkspaces;

    /**
     * Creates a <code>StableWorkspaceDef</code> object, defining a stable
     * workspace.
     *
     * @param name                     name of the stable workspace
     * @param wspStore                 file system where the stable workspace stores its state
     * @param blobStore                file system where the stable workspace stores BLOB data
     * @param persistenceManagerClass  FQN of class implementing the <code>PersistenceManager</code> interface
     * @param persistenceManagerParams parameters for the <code>PersistenceManager</code>
     * @param dynWorkspaces            array of dynamic workspaces that are based on this
     *                                 stable workspace.
     */
    StableWorkspaceDef(String name, FileSystem wspStore, FileSystem blobStore,
		       String persistenceManagerClass, HashMap persistenceManagerParams,
		       DynamicWorkspaceDef[] dynWorkspaces) {
	super(name, wspStore, blobStore, persistenceManagerClass, persistenceManagerParams);
	this.dynWorkspaces = dynWorkspaces;
    }

    /**
     * Returns the dynamic workspaces that are based on this stable workspace.
     *
     * @return array of dynamic workspaces
     */
    public DynamicWorkspaceDef[] getDynWorkspaces() {
	return dynWorkspaces;
    }

    /**
     * Always returns false.
     *
     * @return always false
     */
    public boolean isDynamic() {
	return false;
    }
}
