package org.neo4j.kernel.impl.api;

import org.neo4j.cursor.Cursor;

import java.util.Collection;
import java.util.Iterator;

/**
 * Created by Sascha Peukert on 20.08.2016.
 */
public class VirtualCursor<RelationshipItem> implements Cursor<RelationshipItem> {
    // TODO: Tests
    private Iterator<RelationshipItem> iterator;
    //private RelationshipIterator relationshipIterator;
    private RelationshipItem current;
    //private OperationsFacade facade;

    public VirtualCursor(Collection<RelationshipItem> collection){//, RelationshipIterator realIt, OperationsFacade facade){
        iterator = collection.iterator();
        current = null;
        //this.relationshipIterator = realIt;
        //this.facade = facade;
    }

    @Override
    public boolean next() {
        if(iterator.hasNext()){
            current = iterator.next();
            return true;
        }
        //if(relationshipIterator.hasNext()){
        //    current = new VirtualRelationshipItem(relationshipIterator.next();
        //}
        current = null;
        return false;
    }

    @Override
    public void close() {

    }

    @Override
    public RelationshipItem get() {
        return current;
    }
}
