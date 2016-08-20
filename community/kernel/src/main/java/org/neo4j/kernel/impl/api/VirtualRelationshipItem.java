package org.neo4j.kernel.impl.api;

import org.apache.commons.lang3.NotImplementedException;
import org.neo4j.collection.primitive.PrimitiveIntIterator;
import org.neo4j.cursor.Cursor;
import org.neo4j.storageengine.api.PropertyItem;
import org.neo4j.storageengine.api.RelationshipItem;

/**
 * Created by Sascha Peukert on 20.08.2016.
 */
public class VirtualRelationshipItem implements RelationshipItem {

    private int type;
    private long startNode;
    private long endNode;
    private long id;

    public VirtualRelationshipItem(long start, long end, int type, long relId){
        this.type = type;
        this.startNode = start;
        this.endNode = end;
        this.id = relId;
    }

    @Override
    public int type() {
        return type;
    }

    @Override
    public long startNode() {
        return startNode;
    }

    @Override
    public long endNode() {
        return endNode;
    }

    @Override
    public long otherNode(long nodeId) {
        if(startNode==nodeId){
            return endNode;
        } else{
            return startNode;
        }
    }

    @Override
    public long id() {
        return id;
    }

    @Override
    public Cursor<PropertyItem> properties() {
        throw new NotImplementedException("This should not be called");
        //return null;
    }

    @Override
    public Cursor<PropertyItem> property(int propertyKeyId) {
        throw new NotImplementedException("This should not be called");
        //return null;
    }

    @Override
    public boolean hasProperty(int propertyKeyId) {
        throw new NotImplementedException("This should not be called");
        //return false;
    }

    @Override
    public Object getProperty(int propertyKeyId) {
        throw new NotImplementedException("This should not be called");
        //return null;
    }

    @Override
    public PrimitiveIntIterator getPropertyKeys() {
        throw new NotImplementedException("This should not be called");
        //return null;
    }
}
