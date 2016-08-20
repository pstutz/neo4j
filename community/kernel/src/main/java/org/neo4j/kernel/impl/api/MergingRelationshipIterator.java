package org.neo4j.kernel.impl.api;

import org.neo4j.kernel.impl.api.store.RelationshipIterator;

import java.util.NoSuchElementException;

/**
 * Created by Sascha Peukert on 19.08.2016.
 */
public class MergingRelationshipIterator implements RelationshipIterator {

    private MergingPrimitiveLongIterator it;
    private RelationshipIterator realIterator;

    @Override
    public <EXCEPTION extends Exception> boolean relationshipVisit(long relationshipId, RelationshipVisitor<EXCEPTION> visitor) throws EXCEPTION {
        if(relationshipId<0){
            return true;
        } else {
            return realIterator.relationshipVisit(relationshipId,visitor);
        }
    }

    public MergingRelationshipIterator(RelationshipIterator rIt, MergingPrimitiveLongIterator virtualIterator){
        this.it = virtualIterator;
        if(rIt==null){
            rIt = RelationshipIterator.EMPTY;
        }
        realIterator = rIt;
    }

    @Override
    public boolean hasNext() {
        if(it.hasNext()){
            return true;
        }
        return realIterator.hasNext();

    }

    @Override
    public long next() {
        try{
            return it.next();
        } catch (NoSuchElementException e){
            return realIterator.next();
        }
    }
}
