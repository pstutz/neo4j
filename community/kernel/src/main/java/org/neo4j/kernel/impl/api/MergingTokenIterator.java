package org.neo4j.kernel.impl.api;

import org.neo4j.storageengine.api.Token;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * Created by Sascha Peukert on 11.08.2016.
 */
public class MergingTokenIterator implements Iterator<Token> {

    private Iterator<Token> originalIterator;
    private Iterator<Token> virtualTokenCollectionIterator;

    public MergingTokenIterator(Iterator<Token> it, Collection<Token> values) {
        if(values==null){
            // meh.
            values = new ArrayList<Token>();
        }

        this.virtualTokenCollectionIterator = values.iterator();
        this.originalIterator = it;
    }

    @Override
    public boolean hasNext() {
        if(virtualTokenCollectionIterator.hasNext()){
            return true;
        }
        if(originalIterator!=null) {
            return originalIterator.hasNext();
        }
        return false;
    }

    @Override
    public Token next() {
        try{
            return virtualTokenCollectionIterator.next();
        } catch (NoSuchElementException e){
            if(originalIterator!=null) {
                return originalIterator.next();
            }
            throw new NoSuchElementException();
        }
    }
}



