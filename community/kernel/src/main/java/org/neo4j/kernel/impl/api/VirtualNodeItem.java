package org.neo4j.kernel.impl.api;

import org.neo4j.collection.primitive.PrimitiveIntIterator;
import org.neo4j.cursor.Cursor;
import org.neo4j.kernel.api.exceptions.EntityNotFoundException;
import org.neo4j.kernel.impl.api.store.RelationshipIterator;
import org.neo4j.storageengine.api.*;

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
        return null;
    }

    @Override
    public Cursor<RelationshipItem> relationships(Direction direction, int... typeIds) {
        return null;
    }

    @Override
    public Cursor<RelationshipItem> relationships(Direction direction) {
        return null;
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
        return null;
    }

    @Override
    public Cursor<PropertyItem> property(int propertyKeyId) {
        return null;
    }

    @Override
    public boolean hasProperty(int propertyKeyId) {
        return false;
    }

    @Override
    public Object getProperty(int propertyKeyId) {
        return null;
    }

    @Override
    public PrimitiveIntIterator getPropertyKeys() {
        return null;
    }
}
