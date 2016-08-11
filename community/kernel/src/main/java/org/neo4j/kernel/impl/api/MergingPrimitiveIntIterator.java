package org.neo4j.kernel.impl.api;

import org.neo4j.collection.primitive.PrimitiveIntIterator;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * Created by Sascha Peukert on 11.08.2016.
 */
public class MergingPrimitiveIntIterator implements PrimitiveIntIterator {

    private PrimitiveIntIterator originalIterator;
    private Iterator<Integer> virtualIdCollectionIterator;

    public MergingPrimitiveIntIterator(PrimitiveIntIterator it, Collection<Integer> values) {
        if(values==null){
            // meh.
            values = new ArrayList<Integer>();
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
    public int next() {
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



