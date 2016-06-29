package org.wso2.siddhi.core.util.collection.executor;

import org.wso2.siddhi.core.event.state.StateEvent;
import org.wso2.siddhi.core.event.stream.StreamEvent;
import org.wso2.siddhi.core.event.stream.StreamEventCloner;
import org.wso2.siddhi.core.table.holder.IndexedEventHolder;

import java.util.Set;

public interface CollectionExecutor {

    /**
     * Find the Events matching to the condition, used on the primary call
     *
     * @param matchingEvent        matching input event
     * @param indexedEventHolder   indexed EventHolder containing data
     * @param candidateEventCloner candidate event cloner
     * @return matched StreamEvent, null if no events matched. If candidateEventCloner is null it will return the actual event references.
     */
    StreamEvent find(StateEvent matchingEvent, IndexedEventHolder indexedEventHolder, StreamEventCloner candidateEventCloner);

    /**
     * Find the Events matching to the condition, used for consecutive calls from parent CollectionExecutor
     *
     * @param matchingEvent      matching input event
     * @param indexedEventHolder indexed EventHolder containing data
     * @return matched events as Set, null if Exhaustive processing need to be done.
     */
    Set<StreamEvent> findEventSet(StateEvent matchingEvent, IndexedEventHolder indexedEventHolder);

    /**
     * Checks if a matching event exist in indexedEventHolder
     *
     * @param matchingEvent      matching input event
     * @param indexedEventHolder indexed EventHolder containing data
     * @return true if a matching event is available in indexedEventHolder else false
     */
    boolean contains(StateEvent matchingEvent, IndexedEventHolder indexedEventHolder);

    /**
     * Delete matching events exists from indexedEventHolder
     *
     * @param deletingEvent      matching input event
     * @param indexedEventHolder indexed EventHolder containing data
     */
    void delete(StateEvent deletingEvent, IndexedEventHolder indexedEventHolder);
}