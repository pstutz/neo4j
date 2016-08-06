package org.neo4j.kernel.impl.factory.virtual;

import org.neo4j.collection.primitive.PrimitiveLongIterator;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.PropertyContainer;
import org.neo4j.graphdb.Relationship;

import java.util.Collection;
import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * Created by Sascha Peukert on 06.08.2016.
 */
public class MergePrimitiveLongIterator implements PrimitiveLongIterator {

    private PrimitiveLongIterator originalIterator;
    private Iterator<PropertyContainer> virtualCollectionIterator;
    private boolean node; // if false relationship!

    public MergePrimitiveLongIterator(PrimitiveLongIterator it, Collection<PropertyContainer> values, boolean node) {
        this.virtualCollectionIterator = values.iterator();
        this.originalIterator = it;
        this.node = node;
    }

    @Override
    public boolean hasNext() {
        if(virtualCollectionIterator.hasNext()){
            return true;
        }
        return originalIterator.hasNext();
    }

    @Override
    public long next() {
        try{
            if(node){
                return ((Node)virtualCollectionIterator.next()).getId();
            }
            return ((Relationship)virtualCollectionIterator.next()).getId();
        } catch (NoSuchElementException e){
            return originalIterator.next();
        }
    }
}
