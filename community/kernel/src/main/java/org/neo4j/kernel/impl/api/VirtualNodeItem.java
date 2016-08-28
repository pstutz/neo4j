package org.neo4j.kernel.impl.api;

import org.neo4j.collection.primitive.PrimitiveIntIterator;
import org.neo4j.cursor.Cursor;
import org.neo4j.kernel.api.exceptions.EntityNotFoundException;
import org.neo4j.kernel.impl.api.store.RelationshipIterator;
import org.neo4j.kernel.impl.util.Cursors;
import org.neo4j.storageengine.api.*;

import java.util.ArrayList;
import java.util.function.IntSupplier;

/**
 * Created by Sascha Peukert on 26.08.2016.
 */
public class VirtualNodeItem implements NodeItem{

    private long id;
    private VirtualOperationsFacade ops;

    public VirtualNodeItem(long id, VirtualOperationsFacade ops){
        this.id = id;
        this.ops = ops;
    }

    @Override
    public Cursor<LabelItem> labels() {
        try {
            ops.nodeGetLabels(id);
            //TODO Sascha finish this
        } catch (EntityNotFoundException e) {
            return null;
        }
        return null;
    }

    @Override
    public Cursor<LabelItem> label(int labelId) {
        // seems wrong, but...
        try {
            if(ops.nodeHasLabel(id,labelId)){
                return Cursors.cursor(new VirtualLabelItem(labelId));
            }
        } catch (EntityNotFoundException e) {
            e.printStackTrace();
        }
        return null;
    }

    @Override
    public Cursor<RelationshipItem> relationships(Direction direction, int... typeIds) {
        ArrayList<RelationshipItem> array = new ArrayList<>();
        try {
            RelationshipIterator it;
            switch (direction) {
                case OUTGOING:
                    it = ops.nodeGetRelationships(id, org.neo4j.graphdb.Direction.OUTGOING, typeIds);
                    break;
                case INCOMING:
                    it = ops.nodeGetRelationships(id, org.neo4j.graphdb.Direction.INCOMING, typeIds);
                    break;
                case BOTH:
                    it = ops.nodeGetRelationships(id, org.neo4j.graphdb.Direction.BOTH, typeIds);
                    break;
                default:
                    throw new IllegalStateException("An unknown relationship direction is provided. How?!");
            }

            while(it.hasNext()){
                long key = it.next();
                Cursor<RelationshipItem> oneCursor = ops.relationshipCursor(key);
                array.add(oneCursor.get());
            }

        } catch (EntityNotFoundException e) {
                e.printStackTrace();
        }
        return Cursors.cursor(array);
    }

    @Override
    public Cursor<RelationshipItem> relationships(Direction direction) {
        ArrayList<RelationshipItem> array = new ArrayList<>();
        try {
            RelationshipIterator it;
            switch (direction) {
                case OUTGOING:
                    it = ops.nodeGetRelationships(id, org.neo4j.graphdb.Direction.OUTGOING);
                    break;
                case INCOMING:
                    it = ops.nodeGetRelationships(id, org.neo4j.graphdb.Direction.INCOMING);
                    break;
                case BOTH:
                    it = ops.nodeGetRelationships(id, org.neo4j.graphdb.Direction.BOTH);
                    break;
                default:
                    throw new IllegalStateException("An unknown relationship direction is provided. How?!");
            }

            while(it.hasNext()){
                long key = it.next();
                Cursor<RelationshipItem> oneCursor = ops.relationshipCursor(key);
                array.add(oneCursor.get());
            }

        } catch (EntityNotFoundException e) {
            e.printStackTrace();
        }
        return Cursors.cursor(array);
    }

    @Override
    public Cursor<IntSupplier> relationshipTypes() {
        return null;
    }

    @Override
    public int degree(Direction direction) {
        return 0;
    }

    @Override
    public int degree(Direction direction, int typeId) {
        return 0;
    }

    @Override
    public boolean isDense() {
        return false;
    }

    @Override
    public Cursor<DegreeItem> degrees() {
        return null;
    }

    @Override
    public boolean hasLabel(int labelId) {
        return false;
    }

    @Override
    public PrimitiveIntIterator getLabels() {
        return null;
    }

    @Override
    public RelationshipIterator getRelationships(Direction direction, int[] typeIds) {
        return null;
    }

    @Override
    public RelationshipIterator getRelationships(Direction direction) {
        return null;
    }

    @Override
    public PrimitiveIntIterator getRelationshipTypes() {
        return null;
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
            PrimitiveIntIterator it = ops.nodeGetPropertyKeys(id);
            while(it.hasNext()){
                int key = it.next();
                Object value = ops.nodeGetProperty(id,key);
                array.add(new VirtualPropertyItem(key,value));
            }

        } catch (EntityNotFoundException e) {
            e.printStackTrace();
        }

        return Cursors.cursor(array);
    }

    @Override
    public Cursor<PropertyItem> property(int propertyKeyId) {
        Object value = null;
        try {
            value = ops.nodeGetProperty(id,propertyKeyId);
            return Cursors.cursor(new VirtualPropertyItem(propertyKeyId,value));
        } catch (EntityNotFoundException e) {
            e.printStackTrace();
            return null;
        }
    }

    @Override
    public boolean hasProperty(int propertyKeyId) {
        try {
            return ops.nodeHasProperty(id,propertyKeyId);
        } catch (EntityNotFoundException e) {
            e.printStackTrace();
            return false;
        }
    }

    @Override
    public Object getProperty(int propertyKeyId) {
        try {
            return ops.nodeGetProperty(id,propertyKeyId);
        } catch (EntityNotFoundException e) {
            e.printStackTrace();
            return null;
        }
    }

    @Override
    public PrimitiveIntIterator getPropertyKeys() {
        try {
            return ops.nodeGetPropertyKeys(id);
        } catch (EntityNotFoundException e) {
            e.printStackTrace();
            return null;
        }
    }
}
