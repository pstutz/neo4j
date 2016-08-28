package org.neo4j.kernel.impl.api;

import org.neo4j.collection.primitive.PrimitiveIntIterator;
import org.neo4j.cursor.Cursor;
import org.neo4j.kernel.api.exceptions.EntityNotFoundException;
import org.neo4j.kernel.impl.util.Cursors;
import org.neo4j.storageengine.api.PropertyItem;
import org.neo4j.storageengine.api.RelationshipItem;

import java.util.ArrayList;

/**
 * Created by Sascha Peukert on 20.08.2016.
 */
public class VirtualRelationshipItem implements RelationshipItem {

    private int type;
    private long startNode;
    private long endNode;
    private long id;
    private VirtualOperationsFacade ops;

    public VirtualRelationshipItem(long start, long end, int type, long relId, VirtualOperationsFacade ops){
        this.type = type;
        this.startNode = start;
        this.endNode = end;
        this.id = relId;
        this.ops = ops;
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
        //TODO: Needs testing
        ArrayList<PropertyItem> array = new ArrayList<PropertyItem>();

        try {
            PrimitiveIntIterator it = ops.relationshipGetPropertyKeys(id);
            while(it.hasNext()){
                int key = it.next();
                Object value = ops.relationshipGetProperty(id,key);
                array.add(new VirtualPropertyItem(key,value));
            }

        } catch (EntityNotFoundException e) {
            e.printStackTrace();
        }

        return Cursors.cursor(array);

    }

    @Override
    public Cursor<PropertyItem> property(int propertyKeyId) {
        ArrayList<PropertyItem> array = new ArrayList<>();
        try {
            Object value = ops.relationshipGetProperty(id,propertyKeyId);
            array.add(new VirtualPropertyItem(propertyKeyId,value));
        } catch (EntityNotFoundException e) {
            e.printStackTrace();
        }
        return Cursors.cursor(array);
    }

    @Override
    public boolean hasProperty(int propertyKeyId) {
        try {
            return ops.relationshipHasProperty(id,propertyKeyId);
        } catch (EntityNotFoundException e) {
            e.printStackTrace();
            return false;
        }
    }

    @Override
    public Object getProperty(int propertyKeyId) {
        try {
            return ops.relationshipGetProperty(id,propertyKeyId);
        } catch (EntityNotFoundException e) {
            e.printStackTrace();
            return null;
        }
    }

    @Override
    public PrimitiveIntIterator getPropertyKeys() {
        try {
            return ops.relationshipGetPropertyKeys(id);
        } catch (EntityNotFoundException e) {
            e.printStackTrace();
            return new MergingPrimitiveIntIterator(null,null);
        }
    }
}
