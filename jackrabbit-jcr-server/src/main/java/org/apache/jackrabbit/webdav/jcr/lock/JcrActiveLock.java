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
package org.apache.jackrabbit.webdav.jcr.lock;

import org.apache.jackrabbit.webdav.DavConstants;
import org.apache.jackrabbit.webdav.jcr.ItemResourceConstants;
import org.apache.jackrabbit.webdav.lock.AbstractActiveLock;
import org.apache.jackrabbit.webdav.lock.ActiveLock;
import org.apache.jackrabbit.webdav.lock.Scope;
import org.apache.jackrabbit.webdav.lock.Type;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jcr.RepositoryException;
import javax.jcr.lock.Lock;

/**
 * <code>JcrActiveLock</code> wraps a {@link Lock JCR lock} object.
 */
public class JcrActiveLock extends AbstractActiveLock implements ActiveLock, DavConstants {

    private static Logger log = LoggerFactory.getLogger(JcrActiveLock.class);

    private final Lock lock;

    /**
     * Create a new <code>ActiveLock</code> object with type '{@link Type#WRITE write}'
     * and scope '{@link Scope#EXCLUSIVE exclusive}'.
     *
     * @param lock
     */
    public JcrActiveLock(Lock lock) {
        if (lock == null) {
            throw new IllegalArgumentException("Can not create a ActiveLock with a 'null' argument.");
        }
        this.lock = lock;
    }

    /**
     * Return true if the given lock token equals the token holding that lock.
     *
     * @param lockToken
     * @return true if the given lock token equals this locks token.
     * @see org.apache.jackrabbit.webdav.lock.ActiveLock#isLockedByToken(String)
     */
    public boolean isLockedByToken(String lockToken) {
        if (lockToken != null && lockToken.equals(getToken())) {
            return true;
        }
        return false;
    }

    /**
     * @see ActiveLock#isExpired()
     */
    public boolean isExpired() {
        try {
            return !lock.isLive();
        } catch (RepositoryException e) {
            log.error("Unexpected error: " + e.getMessage());
            return false;
        }
    }

    /**
     * Return the lock token if the {@link javax.jcr.Session} that obtained the lock
     * is the lock token holder, <code>null</code> otherwise.<br>
     * NOTE: currently the token generated by the underlying JCR repository
     * is not checked for compliance with RFC 2518 ("<cite>OpaqueLockToken-URI = "opaquelocktoken:"
     * UUID [Extension] ; The UUID production is the string representation of a
     * UUID, as defined in [ISO-11578]. Note that white space (LWS) is not allowed
     * between elements of this production.</cite>").
     * <p/>
     * In case of session-scoped JCR 2.0 locks, the token is never exposed even
     * if the current session is lock holder. In order to cope with DAV specific
     * requirements and the fulfill the requirement stated above, the node's
     * identifier is subsequently exposed as DAV-token.
     *
     * @see ActiveLock#getToken()
     */
    public String getToken() {
        String token = lock.getLockToken();
        if (token == null && lock.isSessionScoped()
                && lock.isLockOwningSession()) {
            // special handling for session scoped locks that are owned by the
            // current session but never expose their token with jsr 283.
            try {
                token = lock.getNode().getIdentifier();
            } catch (RepositoryException e) {
                // should never get here
                log.warn("Unexpected error while retrieving node identifier for building a DAV specific lock token.",e.getMessage());
            }
        }
        // default behaviour: just return the token exposed by the lock.
        return token;
    }

    /**
     * @see ActiveLock#getOwner()
     */
    public String getOwner() {
        return lock.getLockOwner();
    }

    /**
     * @see ActiveLock#setOwner(String)
     */
    public void setOwner(String owner) {
        throw new UnsupportedOperationException("setOwner is not implemented");
    }

    /**
     * Since jcr locks do not reveal the time left until they expire, {@link #INFINITE_TIMEOUT}
     * is returned. A missing timeout causes problems with Microsoft clients.
     *
     * @return Always returns {@link #INFINITE_TIMEOUT}
     * @see ActiveLock#getTimeout()
     */
    public long getTimeout() {
        return INFINITE_TIMEOUT;
    }

    /**
     * Throws <code>UnsupportedOperationException</code>
     *
     * @see ActiveLock#setTimeout(long)
     */
    public void setTimeout(long timeout) {
        throw new UnsupportedOperationException("setTimeout is not implemented");
    }

    /**
     * @see ActiveLock#isDeep()
     */
    public boolean isDeep() {
        return lock.isDeep();
    }

    /**
     * @see ActiveLock#setIsDeep(boolean)
     */
    public void setIsDeep(boolean isDeep) {
        throw new UnsupportedOperationException("setIsDeep is not implemented");
    }

    /**
     * Always returns {@link Type#WRITE}.
     *
     * @return {@link Type#WRITE}
     * @see ActiveLock#getType()
     */
    public Type getType() {
        return Type.WRITE;
    }

    /**
     * @return The scope of this lock, which may either by an {@link Scope#EXCLUSIVE exclusive}
     * or {@link ItemResourceConstants#EXCLUSIVE_SESSION exclusive session scoped}
     * lock.
     * @see ActiveLock#getScope()
     */
    public Scope getScope() {
        return (lock.isSessionScoped()) ? ItemResourceConstants.EXCLUSIVE_SESSION : Scope.EXCLUSIVE;
    }
}
