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
package org.apache.jackrabbit.jcr.core.observation;

import javax.jcr.Session;
import javax.jcr.observation.EventListener;
import javax.jcr.observation.EventListenerIterator;
import java.util.Collection;
import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * @author Marcel Reutegger
 * @version $Revision: 1.5 $, $Date: 2004/08/25 16:44:50 $
 */
class EventListenerIteratorImpl implements EventListenerIterator {

    /**
     * This iterator will return {@link EventListener}s registered by this
     * <code>Session</code>.
     */
    private final Session session;

    /**
     * Iterator over {@link EventConsumer} instances
     */
    private final Iterator consumers;

    /**
     * The next <code>EventListener</code> that belongs to the session
     * passed in the constructor of this <code>EventListenerIteratorImpl</code>.
     */
    private EventListener next;

    /**
     * Current position
     */
    private long pos = 0;

    /**
     * Creates a new <code>EventListenerIteratorImpl</code>.
     *
     * @param session
     * @param consumers
     * @throws NullPointerException if <code>session</code> or <code>consumer</code>
     *                              is <code>null</code>.
     */
    EventListenerIteratorImpl(Session session, Collection consumers) {
	if (session == null) {
	    throw new NullPointerException("session");
	}
	if (consumers == null) {
	    throw new NullPointerException("consumers");
	}
	this.session = session;
	this.consumers = consumers.iterator();
	fetchNext();
    }

    /**
     * @see javax.jcr.observation.EventListenerIterator#nextEventListener()
     */
    public EventListener nextEventListener() {
	if (next == null) {
	    throw new NoSuchElementException();
	}
	EventListener l = next;
	fetchNext();
	pos++;
	return l;
    }

    /**
     * @see javax.jcr.RangeIterator#skip(long)
     */
    public void skip(long skipNum) {
	while (skipNum-- > 0) {
	    next();
	}
    }

    /**
     * Always returns <code>-1</code>.
     *
     * @return <code>-1</code>.
     */
    public long getSize() {
	return -1;
    }

    /**
     * @see javax.jcr.RangeIterator#getPos()
     */
    public long getPos() {
	return pos;
    }

    /**
     * Remove is not supported on this Iterator.
     *
     * @throws UnsupportedOperationException
     */
    public void remove() {
	throw new UnsupportedOperationException("EventListenerIterator.remove()");
    }

    /**
     * Returns <tt>true</tt> if the iteration has more elements. (In other
     * words, returns <tt>true</tt> if <tt>next</tt> would return an element
     * rather than throwing an exception.)
     *
     * @return <tt>true</tt> if the consumers has more elements.
     */
    public boolean hasNext() {
	return (next != null);
    }

    /**
     * @see Iterator#next()
     */
    public Object next() {
	return nextEventListener();
    }

    /**
     * Fetches the next {@link javax.jcr.observation.EventListener} associated
     * with the <code>Session</code> passed in the constructor of this
     * <code>EventListenerIteratorImpl</code> from all register
     * <code>EventListener</code>s
     */
    private void fetchNext() {
	EventConsumer consumer;
	next = null;
	while (next == null && consumers.hasNext()) {
	    consumer = (EventConsumer) consumers.next();
	    // only return EventConsumers that belong to our session
	    if (consumer.getSession().equals(session)) {
		next = consumer.getEventListener();
	    }

	}
    }
}
