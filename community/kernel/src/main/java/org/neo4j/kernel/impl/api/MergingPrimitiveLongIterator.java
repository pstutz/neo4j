package org.neo4j.kernel.impl.api;

import org.neo4j.collection.primitive.PrimitiveLongIterator;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.NoSuchElementException;
/**
 * Created by Sascha Peukert on 11.08.2016.
 */
public class MergingPrimitiveLongIterator implements PrimitiveLongIterator {

    private PrimitiveLongIterator originalIterator;
    private Iterator<Long> virtualIdCollectionIterator;

    public MergingPrimitiveLongIterator(PrimitiveLongIterator it, Collection<Long> values) {
        if(values==null){
            // meh.
            values = new ArrayList<Long>();
        }

        this.virtualIdCollectionIterator = values.iterator();
        this.originalIterator = it;
    }

    @Override
    public boolean hasNext() {
        if(virtualIdCollectionIterator.hasNext()){
            return true;
        }
        if(originalIterator!=null) {
            return originalIterator.hasNext();
        }
        return false;
    }

    @Override
    public long next() {
        try{
            return virtualIdCollectionIterator.next();
        } catch (NoSuchElementException e){
            if(originalIterator!=null) {
                return originalIterator.next();
            }
            throw new NoSuchElementException();
        }
    }
}



