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
package org.apache.jackrabbit.jcr.core.nodetype;

import org.apache.log4j.Logger;
import org.apache.jackrabbit.jcr.core.NamespaceResolver;
import org.apache.jackrabbit.jcr.core.NoPrefixDeclaredException;
import org.apache.jackrabbit.jcr.core.QName;

import javax.jcr.nodetype.ItemDef;
import javax.jcr.nodetype.NoSuchNodeTypeException;
import javax.jcr.nodetype.NodeType;

/**
 * An <code>ItemDefImpl</code> ...
 *
 * @author Stefan Guggisberg
 * @version $Revision: 1.1 $, $Date: 2004/09/09 15:23:43 $
 */
abstract class ItemDefImpl implements ItemDef {

    private static Logger log = Logger.getLogger(ItemDefImpl.class);

    protected final NodeTypeManagerImpl ntMgr;
    // namespace resolver used to translate qualified names to JCR names
    protected final NamespaceResolver nsResolver;

    private final ChildItemDef itemDef;

    /**
     * Package private constructor
     *
     * @param itemDef    item definition
     * @param ntMgr      node type manager
     * @param nsResolver namespace resolver
     */
    ItemDefImpl(ChildItemDef itemDef, NodeTypeManagerImpl ntMgr, NamespaceResolver nsResolver) {
	this.itemDef = itemDef;
	this.ntMgr = ntMgr;
	this.nsResolver = nsResolver;
    }

    public QName getQName() {
	return itemDef.getName();
    }

    //--------------------------------------------------------------< ItemDef >
    /**
     * @see ItemDef#getDeclaringNodeType
     */
    public NodeType getDeclaringNodeType() {
	try {
	    return ntMgr.getNodeType(itemDef.getDeclaringNodeType());
	} catch (NoSuchNodeTypeException e) {
	    // should never get here
	    log.error("declaring node type does not exist", e);
	    return null;
	}
    }

    /**
     * @see ItemDef#getName
     */
    public String getName() {
	QName name = itemDef.getName();
	if (name == null) {
	    return null;
	}
	try {
	    return name.toJCRName(nsResolver);
	} catch (NoPrefixDeclaredException npde) {
	    // should never get here
	    log.error("encountered unregistered namespace in property name", npde);
	    // not correct, but an acceptable fallback
	    return itemDef.getName().toString();
	}
    }

    /**
     * @see ItemDef#getOnParentVersion()
     */
    public int getOnParentVersion() {
	return itemDef.getOnParentVersion();
    }

    /**
     * @see ItemDef#isAutoCreate
     */
    public boolean isAutoCreate() {
	return itemDef.isAutoCreate();
    }

    /**
     * @see ItemDef#isMandatory
     */
    public boolean isMandatory() {
	return itemDef.isMandatory();
    }

    /**
     * @see ItemDef#isPrimaryItem
     */
    public boolean isPrimaryItem() {
	return itemDef.isPrimaryItem();
    }

    /**
     * @see ItemDef#isProtected
     */
    public boolean isProtected() {
	return itemDef.isProtected();
    }
}

