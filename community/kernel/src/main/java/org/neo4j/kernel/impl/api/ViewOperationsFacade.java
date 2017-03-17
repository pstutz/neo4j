/*
 * Copyright (c) 2002-2016 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.kernel.impl.api;

import org.neo4j.collection.primitive.PrimitiveIntIterator;
import org.neo4j.collection.primitive.PrimitiveLongIterator;
import org.neo4j.cursor.Cursor;
import org.neo4j.graphdb.Direction;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.StatementConstants;
import org.neo4j.kernel.api.exceptions.*;
import org.neo4j.kernel.api.exceptions.index.IndexNotFoundKernelException;
import org.neo4j.kernel.api.exceptions.legacyindex.AutoIndexingKernelException;
import org.neo4j.kernel.api.exceptions.schema.ConstraintValidationKernelException;
import org.neo4j.kernel.api.exceptions.schema.IllegalTokenNameException;
import org.neo4j.kernel.api.exceptions.schema.IndexBrokenKernelException;
import org.neo4j.kernel.api.exceptions.schema.TooManyLabelsException;
import org.neo4j.kernel.api.index.IndexDescriptor;
import org.neo4j.kernel.api.properties.DefinedProperty;
import org.neo4j.kernel.api.properties.Property;
import org.neo4j.kernel.impl.api.store.CursorRelationshipIterator;
import org.neo4j.kernel.impl.api.store.RelationshipIterator;
import org.neo4j.kernel.impl.proc.Procedures;
import org.neo4j.kernel.impl.util.Cursors;
import org.neo4j.register.Register.DoubleLongRegister;
import org.neo4j.storageengine.api.EntityType;
import org.neo4j.storageengine.api.NodeItem;
import org.neo4j.storageengine.api.RelationshipItem;
import org.neo4j.storageengine.api.Token;
import saschapeukert.IdFilter;

import java.util.*;

// TODO Sascha
public class ViewOperationsFacade extends OperationsFacade
{

    class PropertyValueId {

        private long entityId;
        private int propKeyId;

        public PropertyValueId(long entity, int prop){
            this.entityId = entity;
            this.propKeyId = prop;
        }

        public long getEntityId(){
            return entityId;
        }

        public int getPropertyKeyId(){
            return propKeyId;
        }

        @Override
        public boolean equals(Object obj) {
            if(obj.getClass()==this.getClass()){
                PropertyValueId other = (PropertyValueId) obj;
                if((this.entityId==other.entityId) && (this.propKeyId == other.propKeyId)){
                    return true;
                }
                return false;
            } else{
                return false;
            }
        }
    }

    class FilteredPrimitiveLongIterator implements PrimitiveLongIterator {

        private IdFilter filter;
        private PrimitiveLongIterator iterator;
        private boolean hasCached;
        private long cached;

        public FilteredPrimitiveLongIterator(IdFilter f, PrimitiveLongIterator originalIterator) {
            super();
            filter = f;
            iterator = originalIterator;
            cached = -1;
            hasCached = false;
        }

        @Override
        public boolean hasNext() {
            if (hasCached) return true;
            //iterate until you find one and set hasCached and cached
            if (filter.isUnused()) {
                return iterator.hasNext();
            } else {
                // filter in use
                do {
                    if (iterator.hasNext()) {
                        long candidateId = iterator.next();
                        if (filter.idIsInFilter(candidateId)) {
                            hasCached = true;
                            cached = candidateId;
                            return true;
                        }
                    } else {
                        return false;
                    }
                } while (true);
            }
        }

        @Override
        public long next() {
            if (hasCached) {
                hasCached = false;
                return cached;
            }
            //iterate until next matches
            if (filter.isUnused()) {
                return iterator.next();
            } else {
                // filter in use
                if (hasNext()) {
                    hasCached = false;
                    return cached;
                } else {
                    iterator.next(); // this makes booom
                    return -1; // this does not happen!
                }
            }
        }
    }

    //! private Map<Integer,HashMap<Long,Integer>> virtualRelationshipIdToTypeId; // actualData and ref to types
    //! private Map<Integer,HashSet<Long>> virtualNodeIds;  // "actual data"
    //! private Map<Integer,HashMap<Integer,String>> virtualPropertyKeyIdsToName;  // "actual data"
    //! private Map<Integer,HashMap<Integer,String>> virtualLabels; // actual data
    //! private Map<Integer,HashMap<Integer,String>> virtualRelationshipTypeIdToName; // actual data

    //entityId + propKeyId -> value
    //! private Map<PropertyValueId,Object> virtualPropertyIdToValueForNodes;
    //! private Map<PropertyValueId,Object> virtualPropertyIdToValueForRels;

    //! private Map<Integer,Map<Long,Set<Integer>>> virtualNodeIdToPropertyKeyIds;   // Natural ordering 1 2 3 10 12 ...  -> first one is the smallest with negative
    //! private Map<Integer,Map<Long,Set<Long>>> virtualNodeIdToConnectedRelationshipIds;
    //! private Map<Integer,Map<Long,Set<Integer>>> virtualRelationshipIdToPropertyKeyIds;
    //! private Map<Integer,Map<Long,Set<Integer>>> virtualNodeIdToLabelIds;
    //! private Map<Integer,Map<Long,Long[]>> virtualRelationshipIdToVirtualNodeIds; // Node[0] = from, Node[1] = to

    //! private HashSet<Integer> knowntransactionIds;  // why sorted?

    ViewOperationsFacade(KernelTransaction tx, KernelStatement statement,
                         Procedures procedures )
    {
        super(tx,statement,procedures);

        //! virtualRelationshipIdToTypeId = new HashMap<>();
        //! virtualNodeIds = new HashMap<>();
        //! virtualPropertyKeyIdsToName = new HashMap<>();
        //! virtualLabels = new HashMap<>();
        //! virtualRelationshipTypeIdToName = new HashMap<>();

        //! virtualNodeIdToPropertyKeyIds = new HashMap<>();
        //! virtualRelationshipIdToPropertyKeyIds = new HashMap<>();
        //! virtualNodeIdToLabelIds = new HashMap<>();
        //! virtualRelationshipIdToVirtualNodeIds = new HashMap<>();
        //! virtualNodeIdToConnectedRelationshipIds = new HashMap<>();

        //! virtualPropertyIdToValueForNodes = new HashMap<>();
        //! virtualPropertyIdToValueForRels = new HashMap<>();

        //! knowntransactionIds = new HashSet<>(); // previously TreeSet

    }

    public void initialize( StatementOperationParts operationParts )
    {
        super.initialize( operationParts);
    }

    // <DataRead>

    @Override
    public PrimitiveLongIterator nodesGetAll()
    {
        PrimitiveLongIterator allRealNodes = super.nodesGetAll();
        KernelTransactionImplementation tx = txState();
        MergingPrimitiveLongIterator bothNodeIds = new MergingPrimitiveLongIterator(allRealNodes,
                tx.virtualNodeIds);
        IdFilter nodeIdFilter = tx.getNodeIdFilter();
        return new FilteredPrimitiveLongIterator(nodeIdFilter,bothNodeIds);
    }

    @Override
    public PrimitiveLongIterator relationshipsGetAll()
    {
        PrimitiveLongIterator allRealRels = super.relationshipsGetAll();
        MergingPrimitiveLongIterator bothRelIds =
                new MergingPrimitiveLongIterator(allRealRels,virtualRelationshipIds());
        IdFilter relIdFilter = txState().getRelationshipIdFilter();
        return new FilteredPrimitiveLongIterator(relIdFilter,bothRelIds);
    }

    @Override
    public PrimitiveLongIterator nodesGetForLabel(int labelId )
    {
        PrimitiveLongIterator originalIT = super.nodesGetForLabel(labelId);
        KernelTransactionImplementation tx = txState();
        ArrayList<Long> resultList = new ArrayList<>();

        // Get all virtual nodes that belong to this transaction
        Map<Long,Set<Integer>> nodeIdToLabelIds = tx.virtualNodeIdToLabelIds;

        for(Long nodeId: nodeIdToLabelIds.keySet()) {
            Set<Integer> labelIds = nodeIdToLabelIds.get(nodeId);
            if(labelIds.contains(labelId)){
                resultList.add(nodeId);
            }
        }
        IdFilter nodeIdFilter = tx.getNodeIdFilter();

        PrimitiveLongIterator it = new MergingPrimitiveLongIterator(originalIT,resultList);
        return new FilteredPrimitiveLongIterator(nodeIdFilter, it);
    }

    @Override
    public PrimitiveLongIterator nodesGetFromIndexSeek(IndexDescriptor index, Object value )
            throws IndexNotFoundKernelException
    {
        // TODO !
        IdFilter nodeIdFilter = txState().getNodeIdFilter();
        return new FilteredPrimitiveLongIterator(nodeIdFilter, super.nodesGetFromIndexSeek(index,value));
    }

    @Override
    public PrimitiveLongIterator nodesGetFromIndexRangeSeekByNumber(IndexDescriptor index,
                                                                    Number lower,
                                                                    boolean includeLower,
                                                                    Number upper,
                                                                    boolean includeUpper )
            throws IndexNotFoundKernelException
    {
        // TODO !
        IdFilter nodeIdFilter = txState().getNodeIdFilter();
        return new FilteredPrimitiveLongIterator(nodeIdFilter, super.nodesGetFromIndexRangeSeekByNumber(index,lower,includeLower,upper,includeUpper));
    }

    @Override
    public PrimitiveLongIterator nodesGetFromIndexRangeSeekByString(IndexDescriptor index,
                                                                    String lower,
                                                                    boolean includeLower,
                                                                    String upper,
                                                                    boolean includeUpper )
            throws IndexNotFoundKernelException
    {
        // TODO !
        IdFilter nodeIdFilter = txState().getNodeIdFilter();
        return new FilteredPrimitiveLongIterator(nodeIdFilter, super.nodesGetFromIndexRangeSeekByString(index,lower,includeLower,upper,includeUpper));
    }

    @Override
    public PrimitiveLongIterator nodesGetFromIndexRangeSeekByPrefix(IndexDescriptor index, String prefix )
            throws IndexNotFoundKernelException
    {
        // TODO !
        IdFilter nodeIdFilter = txState().getNodeIdFilter();
        return new FilteredPrimitiveLongIterator(nodeIdFilter, super.nodesGetFromIndexRangeSeekByPrefix(index,prefix));
    }

    @Override
    public PrimitiveLongIterator nodesGetFromIndexScan(IndexDescriptor index )
            throws IndexNotFoundKernelException
    {
        // TODO !
        IdFilter nodeIdFilter = txState().getNodeIdFilter();
        return new FilteredPrimitiveLongIterator(nodeIdFilter, super.nodesGetFromIndexScan(index));
    }

    @Override
    public PrimitiveLongIterator nodesGetFromIndexContainsScan(IndexDescriptor index, String term )
            throws IndexNotFoundKernelException
    {
        // TODO !
        IdFilter nodeIdFilter = txState().getNodeIdFilter();
        return new FilteredPrimitiveLongIterator(nodeIdFilter, super.nodesGetFromIndexContainsScan(index,term));
    }

    @Override
    public PrimitiveLongIterator nodesGetFromIndexEndsWithScan(IndexDescriptor index, String suffix )
            throws IndexNotFoundKernelException
    {
        // TODO !
        IdFilter nodeIdFilter = txState().getNodeIdFilter();
        return new FilteredPrimitiveLongIterator(nodeIdFilter, super.nodesGetFromIndexEndsWithScan(index,suffix));
    }

    @Override
    public long nodeGetFromUniqueIndexSeek(IndexDescriptor index, Object value )
            throws IndexNotFoundKernelException, IndexBrokenKernelException
    {
        long candidateId = super.nodeGetFromUniqueIndexSeek(index,value);
        IdFilter nodeIdFilter = txState().getNodeIdFilter();
        //if(candidateId!=StatementConstants.NO_SUCH_NODE){
            if(!nodeIdFilter.isUnused()) {
                // -> is used
                if (!nodeIdFilter.idIsInFilter(candidateId)) {
                    candidateId = StatementConstants.NO_SUCH_NODE;
                }
            }
        //}

        return candidateId;
    }

    @Override
    public boolean nodeExists( long nodeId )
    {
        KernelTransactionImplementation tx = txState();
        IdFilter nodeIdFilter = tx.getNodeIdFilter();
        if(!nodeIdFilter.isUnused()){
            // -> is used
            if(!nodeIdFilter.idIsInFilter(nodeId)){
                // but not in filter, so it dont exists
                return false;
            }
        }

        if(isVirtual(nodeId)){
            return tx.virtualNodeIds.contains(nodeId);
        } else {
            return super.nodeExists(nodeId);
        }
    }

    @Override
    public boolean relationshipExists( long relId )
    {
        IdFilter relIdFilter = txState().getRelationshipIdFilter();
        if(!relIdFilter.isUnused()){
            // -> is used
            if(!relIdFilter.idIsInFilter(relId)){
                // but not in filter, so it dont exists
                return false;
            }
        }

        if(isVirtual(relId)){
            return virtualRelationshipIds().contains(relId);
        } else {
            return super.relationshipExists(relId);
        }
    }

    @Override
    public boolean nodeHasLabel( long nodeId, int labelId ) throws EntityNotFoundException
    {
        KernelTransactionImplementation tx = txState();
        if(isVirtual(nodeId)){
            if(nodeExists(nodeId)){
                Set<Integer> labelIds = tx.virtualNodeIdToLabelIds.get(nodeId);
                if(labelIds==null){
                    return false;
                }
                return labelIds.contains(labelId);
            } else{
                throw new EntityNotFoundException(EntityType.NODE,nodeId);
            }
        } else {
            return super.nodeHasLabel(nodeId, labelId);
        }
    }

    @Override
    public PrimitiveIntIterator nodeGetLabels(long nodeId ) throws EntityNotFoundException
    {
        KernelTransactionImplementation tx = txState();
        if(isVirtual(nodeId)){
            if(nodeExists(nodeId)){
                Set<Integer> labelIds = tx.virtualNodeIdToLabelIds.get(nodeId);
                MergingPrimitiveIntIterator it = new MergingPrimitiveIntIterator(null,labelIds);
                return it;
            } else{
                throw new EntityNotFoundException(EntityType.NODE,nodeId);
            }
        } else {
            return super.nodeGetLabels(nodeId);
        }
    }

    @Override
    public boolean nodeHasProperty( long nodeId, int propertyKeyId ) throws EntityNotFoundException
    {
        KernelTransactionImplementation tx = txState();
        if(isVirtual(nodeId)){
            if(nodeExists(nodeId)){
                Set<Integer> propIds = tx.virtualNodeIdToPropertyKeyIds.get(nodeId);
                if(propIds==null){
                    return false;
                }
                return propIds.contains(propertyKeyId);
            } else{
                throw new EntityNotFoundException(EntityType.NODE,nodeId);
            }
        } else {
            return super.nodeHasProperty(nodeId, propertyKeyId);
        }
    }

    @Override
    public Object nodeGetProperty( long nodeId, int propertyKeyId ) throws EntityNotFoundException
    {
        KernelTransactionImplementation tx = txState();
        if(isVirtual(nodeId)){
            if(nodeExists(nodeId)){
                // assert that the property belongs to this node
                Set<Integer> props = tx.virtualNodeIdToPropertyKeyIds.get(nodeId);
                if(props.contains(propertyKeyId)){
                    return getPropertyValueForNodes(nodeId,propertyKeyId); //TODO: Test this! Might need some improvements
                }
                throw new EntityNotFoundException(EntityType.NODE,nodeId);
            } else{
                throw new EntityNotFoundException(EntityType.NODE,nodeId);
            }

        } else {
            return super.nodeGetProperty(nodeId, propertyKeyId);
        }
    }

    @Override
    public RelationshipIterator nodeGetRelationships(long nodeId, Direction direction, int... relTypes )
            throws EntityNotFoundException
    {
        // TODO SASCHA
        KernelTransactionImplementation tx = txState();
        Set<RelationshipItem> foundItems = new HashSet<>();
        RelationshipIterator realIt = null;
        IdFilter relIdFilter = tx.getRelationshipIdFilter();
        // get the real ones
        if(!isVirtual(nodeId)) {
            realIt = super.nodeGetRelationships(nodeId, direction,relTypes);
            if(relIdFilter.isUnused()&&tx.virtualRelationshipIdToVirtualNodeIds.keySet().size()==0){
                return realIt;  // more of a hack then anything else
            }

            while (realIt.hasNext()) {
                Long rId = realIt.next();

                if(!relIdFilter.isUnused()){
                    // -> is used
                    if(!relIdFilter.idIsInFilter(rId)){
                        // but not in filter, so it 'dont exists' in this scope
                        continue;
                    }
                }
                Cursor<RelationshipItem> c = super.relationshipCursor(rId);
                while(c.next()){
                    RelationshipItem item = c.get();
                    //c.close();
                    if(item.id()==rId){
                        foundItems.add(item);
                        break;

                    }
                }

                //foundItems.add(super.relationshipCursor(rId).get());
                /*Cursor<RelationshipItem> cursor = super.relationshipCursorGetAll();
                while(cursor.next()){
                    RelationshipItem i = cursor.get();
                    if(i.id()==rId){
                        foundItems.add(i);
                        break;
                    }
                } */
            }
        }

        // no actual rel there, thats okay! on to the virtual stuff
        Set<Long> relIds = tx.virtualRelationshipIdToVirtualNodeIds.keySet();
        for (Long relId : relIds) {
            if(!relIdFilter.isUnused()){
                // -> is used
                if(!relIdFilter.idIsInFilter(relId)){
                    // but not in filter, so it 'dont exists' in this scope
                    continue;
                }
            }

            for(int type:relTypes){
                // check if type matches for this relId
                if(type==tx.virtualRelationshipIdToTypeId.get(relId)){
                    Long[] nodeIds = tx.virtualRelationshipIdToVirtualNodeIds.get(relId);
                    switch (direction) {
                        case INCOMING:
                            if (nodeIds[1].equals(nodeId)) {
                                foundItems.add(new VirtualRelationshipItem(nodeIds[0],nodeIds[1],
                                        tx.virtualRelationshipIdToTypeId.get(relId),relId, this));
                            }
                            break;
                        case OUTGOING:
                            if (nodeIds[0].equals(nodeId)) {
                                foundItems.add(new VirtualRelationshipItem(nodeIds[0],nodeIds[1],
                                        tx.virtualRelationshipIdToTypeId.get(relId),relId, this));
                            }
                            break;
                        case BOTH:
                            if (nodeIds[0].equals(nodeId) || nodeIds[1].equals(nodeId)) {
                                foundItems.add(new VirtualRelationshipItem(nodeIds[0],nodeIds[1],
                                        tx.virtualRelationshipIdToTypeId.get(relId),relId, this));
                            }
                            break;
                        default:
                            // What?!
                    }
                    break;
                }
            }
        }
        VirtualCursor<RelationshipItem> cursor = new VirtualCursor<>(foundItems);//, realIt, this);
        return new CursorRelationshipIterator(cursor);
        //return new MergingRelationshipIterator(null, new MergingPrimitiveLongIterator(null, foundIds));
    }

    @Override
    public RelationshipIterator nodeGetRelationships(long nodeId, Direction direction )
            throws EntityNotFoundException
    {
        //TODO: check if that is right
        // build up a collection of rel ids that match
        KernelTransactionImplementation tx = txState();
        Set<RelationshipItem> foundItems = new HashSet<>();
        IdFilter relIdFilter = tx.getRelationshipIdFilter();
        // get the real ones
        if(!isVirtual(nodeId)) {
            RelationshipIterator realIt = super.nodeGetRelationships(nodeId, direction);
            if(relIdFilter.isUnused()&&tx.virtualRelationshipIdToVirtualNodeIds.keySet().size()==0){
                return realIt;  // more of a hack then anything else
            }
            while (realIt.hasNext()) {
                Long rId = realIt.next();

                if(!relIdFilter.isUnused()){
                    // -> is used
                    if(!relIdFilter.idIsInFilter(rId)){
                        // but not in filter, so it 'dont exists' in this scope
                        continue;
                    }
                }
                Cursor<RelationshipItem> c = super.relationshipCursor(rId);
                while(c.next()){
                    RelationshipItem item = c.get();
                    //c.close();
                    if(item.id()==rId){
                        foundItems.add(item);
                        break;

                    }
                }
                /*
                Cursor<RelationshipItem> c = super.relationshipCursor(rId);
                RelationshipItem item;
                while(c.next()){
                    item = c.get();
                    // ensure that this is the right one
                    switch (direction){
                        case OUTGOING:
                            if(item.startNode()!=nodeId) {
                                continue;
                            }
                            break;
                        case INCOMING:
                            if(item.endNode()!=nodeId) {
                                continue;
                            }
                            break;
                        case BOTH:
                            if(item.startNode()!=nodeId&&item.endNode()!=nodeId){
                                continue;
                            }
                            break;
                    }
                    foundItems.add(item);
                    break;
                }*/
            }
        }
        Set<Long> relIds = tx.virtualRelationshipIdToVirtualNodeIds.keySet();

        for (Long relId : relIds) {

            if(!relIdFilter.isUnused()){
                // -> is used
                if(!relIdFilter.idIsInFilter(relId)){
                    // but not in filter, so it 'dont exists' in this scope
                    continue;
                }
            }

            Long[] nodeIds = tx.virtualRelationshipIdToVirtualNodeIds.get(relId);
            switch (direction) {
                case INCOMING:
                    if (nodeIds[1].equals(nodeId)) {
                        int type = tx.virtualRelationshipIdToTypeId.get(relId);
                        foundItems.add(new VirtualRelationshipItem(nodeIds[0],nodeIds[1],
                                type,relId, this));
                    }
                    break;
                case OUTGOING:
                    if (nodeIds[0].equals(nodeId)) {
                        int type = tx.virtualRelationshipIdToTypeId.get(relId);
                        foundItems.add(new VirtualRelationshipItem(nodeIds[0],nodeIds[1],
                                type,relId, this));
                    }
                    break;
                case BOTH:
                    if (nodeIds[0].equals(nodeId) || nodeIds[1].equals(nodeId)) {
                        int type = tx.virtualRelationshipIdToTypeId.get(relId);
                        foundItems.add(new VirtualRelationshipItem(nodeIds[0],nodeIds[1],
                                type,relId, this));
                    }
                    break;
                default:
                    // What?!
            }
        }

        VirtualCursor<RelationshipItem> cursor = new VirtualCursor<>(foundItems);
        return new CursorRelationshipIterator(cursor);
    }

    @Override
    public int nodeGetDegree(long nodeId, Direction direction, int relType ) throws EntityNotFoundException
    {
        int degree = 0;
        RelationshipIterator it = nodeGetRelationships(nodeId,direction,relType);
        while(it.hasNext()){
            it.next();
            degree++;
        }
        return degree;
    }

    @Override
    public int nodeGetDegree( long nodeId, Direction direction ) throws EntityNotFoundException
    {
        int degree = 0;
        RelationshipIterator it = nodeGetRelationships(nodeId,direction);
        while(it.hasNext()){
            it.next();
            degree++;
        }
        return degree;
    }

    @Override
    public boolean nodeIsDense( long nodeId ) throws EntityNotFoundException
    {
        // naive impl.
        if(isVirtual(nodeId)){
            return false;
        } else {
            return super.nodeIsDense(nodeId);
        }
    }

    @Override
    public PrimitiveIntIterator nodeGetRelationshipTypes(long nodeId ) throws EntityNotFoundException
    {
        if(isVirtual(nodeId)) {
            KernelTransactionImplementation tx = txState();
            if(nodeExists(nodeId)){
                Set<Long> relIds = tx.virtualNodeIdToConnectedRelationshipIds.get(nodeId);
                Set<Integer> resultSet = new HashSet<>();
                for(Long relId:relIds){
                    resultSet.add(tx.virtualRelationshipIdToTypeId.get(relId)); // this should not be null! TODO: Tests to ensure that
                }
                return new MergingPrimitiveIntIterator(null,resultSet);
            } else{
                throw new EntityNotFoundException(EntityType.NODE,nodeId);
            }
        } else{
            return super.nodeGetRelationshipTypes(nodeId);
        }
    }

    @Override
    public boolean relationshipHasProperty( long relationshipId, int propertyKeyId ) throws EntityNotFoundException
    {
        if(isVirtual(relationshipId)){
            KernelTransactionImplementation tx = txState();
            if(relationshipExists(relationshipId)){
                Set<Integer> propIds = tx.virtualRelationshipIdToPropertyKeyIds.get(relationshipId);
                if(propIds==null){
                    return false;
                }
                return propIds.contains(propertyKeyId);
            } else{
                throw new EntityNotFoundException(EntityType.RELATIONSHIP,relationshipId);
            }
        } else {
            return super.relationshipHasProperty(relationshipId,propertyKeyId);
        }
    }

    @Override
    public Object relationshipGetProperty( long relationshipId, int propertyKeyId ) throws EntityNotFoundException
    {
        //TODO: Ensure that virtual entities can only got virtual props
        if(isVirtual(relationshipId)){
            KernelTransactionImplementation tx = txState();
            if(relationshipExists(relationshipId)){
                // assert that this rel has got that prop -> already covered by relationshipHasProperty but are they always chained?
                Set<Integer> propIds = tx.virtualRelationshipIdToPropertyKeyIds.get(relationshipId);
                if(propIds==null){
                    return null;
                }
                if(propIds.contains(propertyKeyId)){
                    return getPropertyValueForRels(relationshipId,propertyKeyId);
                }
                throw new EntityNotFoundException(EntityType.RELATIONSHIP,relationshipId); // hm.. not that good
            } else{
                throw new EntityNotFoundException(EntityType.RELATIONSHIP,relationshipId);
            }
        } else {
            return super.relationshipGetProperty(relationshipId,propertyKeyId);
        }

    }

    @Override
    public boolean graphHasProperty( int propertyKeyId )
    {
        if(isVirtual(propertyKeyId)){
            return virtualPropertyKeyIds().contains(propertyKeyId);
        } else {
            return super.graphHasProperty(propertyKeyId);
        }
    }

    @Override
    public Object graphGetProperty( int propertyKeyId )
    {
        // TODO: TEST THIS!!!!!
        if(isVirtual(propertyKeyId)){
            KernelTransactionImplementation tx = txState();
            Iterator<Long> it = tx.virtualNodeIds.iterator();
            while(it.hasNext()){
                long current_nodeId = it.next();
                try{
                    return nodeGetProperty(current_nodeId,propertyKeyId);
                }catch (EntityNotFoundException e){
                }
            }
            it = virtualRelationshipIds().iterator();
            while(it.hasNext()){
                long current_relId = it.next();
                try{
                    return relationshipGetProperty(current_relId,propertyKeyId);
                }catch (EntityNotFoundException e){
                }
            }

            return null;
        } else {
            return super.graphGetProperty(propertyKeyId);
        }
    }

    @Override
    public PrimitiveIntIterator nodeGetPropertyKeys(long nodeId ) throws EntityNotFoundException
    {
        if(isVirtual(nodeId)){
            KernelTransactionImplementation tx = txState();
            return new MergingPrimitiveIntIterator(null, tx.virtualNodeIdToPropertyKeyIds.get(nodeId));
        } else {
            return super.nodeGetPropertyKeys(nodeId);
        }
    }

    @Override
    public PrimitiveIntIterator relationshipGetPropertyKeys(long relationshipId ) throws EntityNotFoundException
    {
        if(isVirtual(relationshipId)){
            KernelTransactionImplementation tx = txState();
            return new MergingPrimitiveIntIterator(null, tx.virtualRelationshipIdToPropertyKeyIds.get(relationshipId));
        } else {
            return super.relationshipGetPropertyKeys(relationshipId);
        }
    }

    @Override
    public PrimitiveIntIterator graphGetPropertyKeys()
    {
        return new MergingPrimitiveIntIterator(super.graphGetPropertyKeys(), virtualPropertyKeyIds());
    }

    @Override
    public <EXCEPTION extends Exception> void relationshipVisit( long relId,
            RelationshipVisitor<EXCEPTION> visitor ) throws EntityNotFoundException, EXCEPTION
    {
        if(isVirtual(relId)){
            KernelTransactionImplementation tx = txState();
            // this might do the job -> TODO test that
            if(relationshipExists(relId)){
                int typeId = tx.virtualRelationshipIdToTypeId.get(relId);
                long startNode = tx.virtualRelationshipIdToVirtualNodeIds.get(relId)[0];
                long endNode   = tx.virtualRelationshipIdToVirtualNodeIds.get(relId)[0];
                visitor.visit(relId,typeId,startNode,endNode);
                return;
            } else{
                throw new EntityNotFoundException(EntityType.RELATIONSHIP,relId);
            }
        } else{
            super.relationshipVisit(relId,visitor);
        }

    }

    @Override
    public long nodesGetCount()
    {
        return super.nodesGetCount() + txState().virtualNodeIds.size();
    }

    @Override
    public long relationshipsGetCount()
    {
        return super.relationshipsGetCount() + virtualRelationshipIds().size();
    }

    // </DataRead>

    // <DataReadCursors>
    @Override
    public Cursor<NodeItem> nodeCursor(long nodeId )
    {
        // TODO: Test this
        if(isVirtual(nodeId)){
            VirtualNodeItem v = new VirtualNodeItem(nodeId, this);
            return Cursors.cursor(v);
        }
        return super.nodeCursor(nodeId);
    }

    @Override
    public Cursor<RelationshipItem> relationshipCursor(long relId )
    {
        //TODO: Test this more
        if(isVirtual(relId)){
            KernelTransactionImplementation tx = txState();
            long startNode = tx.virtualRelationshipIdToVirtualNodeIds.get(relId)[0];
            long endNode = tx.virtualRelationshipIdToVirtualNodeIds.get(relId)[1];
            int type = tx.virtualRelationshipIdToTypeId.get(relId);
            VirtualRelationshipItem v = new VirtualRelationshipItem(startNode,endNode,type,relId, this);

            return Cursors.cursor(v);
        }
        return super.relationshipCursor(relId);
    }

    @Override
    public Cursor<NodeItem> nodeCursorGetAll()
    {
        // TODO: Test this
        // TODO: @Sascha: does this work without filter?
        statement.assertOpen(); // from super
        Cursor<NodeItem> realCursor = super.nodeCursorGetAll();
        ArrayList<NodeItem> itemList = new ArrayList<>();
        while(realCursor.next()){
            NodeItem item = realCursor.get();
            itemList.add(item);
        }

        KernelTransactionImplementation tx = txState();
        // getting virtual ids and making them to VirtualNodeItems
        Set<Long> virtualNodes = tx.virtualNodeIds;
        for(Long l:virtualNodes){
            itemList.add(nodeCursor(l).get());
        }

        //NodeItem[] array = itemList.toArray(new NodeItem[itemList.size()]);
        return Cursors.cursor(itemList);
    }

    @Override
    public Cursor<RelationshipItem> relationshipCursorGetAll()
    {
        // TODO: Test this
        // TODO: @Sascha: does this work without filter?
        statement.assertOpen(); // from super
        Cursor<RelationshipItem> realCursor = super.relationshipCursorGetAll();
        ArrayList<RelationshipItem> itemList = new ArrayList<>();
        while(realCursor.next()){
            RelationshipItem item = realCursor.get();
            itemList.add(item);
        }

        // getting virtual ids and making them to VirtualNodeItems
        Set<Long> virtualRels = virtualRelationshipIds();
        for(Long l:virtualRels){
            RelationshipItem item = relationshipCursor(l).get();
            itemList.add(item);
        }

        return Cursors.cursor(itemList);
    }

    @Override
    public Cursor<NodeItem> nodeCursorGetForLabel(int labelId )
    {
        // getting the real ones
        Cursor<NodeItem> realCursor = super.nodeCursorGetForLabel(labelId);
        ArrayList<NodeItem> itemList = new ArrayList<>();
        IdFilter nodeIdFilter = txState().getNodeIdFilter();
        while(realCursor.next()){
            NodeItem item = realCursor.get();
            // filter real ids
            if(!nodeIdFilter.isUnused()){
                if(!nodeIdFilter.idIsInFilter(item.id())){
                    continue;
                }
            }
            itemList.add(item);
        }
        KernelTransactionImplementation tx = txState();
        // adding the virtual ones to the mix
        Set<Long> virtualNodes = tx.virtualNodeIds;
        Map<Long,Set<Integer>> idToLabel = tx.virtualNodeIdToLabelIds;
        for(Long l:virtualNodes){
            // filter virtual ids
            if(!nodeIdFilter.isUnused()){
                if(!nodeIdFilter.idIsInFilter(l)){
                    continue;
                }
            }
            //  --
            if(idToLabel.get(l).contains(labelId)) {
                itemList.add(nodeCursor(l).get());
            }
        }

        return Cursors.cursor(itemList);
    }

    @Override
    public Cursor<NodeItem> nodeCursorGetFromIndexSeek(IndexDescriptor index,
                                                       Object value ) throws IndexNotFoundKernelException
    {
        // TODO !
        return super.nodeCursorGetFromIndexSeek(index,value);
    }

    @Override
    public Cursor<NodeItem> nodeCursorGetFromIndexScan(IndexDescriptor index ) throws IndexNotFoundKernelException
    {
        // TODO !
        return super.nodeCursorGetFromIndexScan(index);
    }

    @Override
    public Cursor<NodeItem> nodeCursorGetFromIndexRangeSeekByNumber(IndexDescriptor index,
                                                                    Number lower, boolean includeLower,
                                                                    Number upper, boolean includeUpper )
            throws IndexNotFoundKernelException
    {
        // TODO !
        return super.nodeCursorGetFromIndexRangeSeekByNumber(index,lower,includeLower,upper,includeUpper);
    }

    @Override
    public Cursor<NodeItem> nodeCursorGetFromIndexRangeSeekByString(IndexDescriptor index,
                                                                    String lower, boolean includeLower,
                                                                    String upper, boolean includeUpper )
            throws IndexNotFoundKernelException
    {
        // TODO !
        return super.nodeCursorGetFromIndexRangeSeekByString(index,lower,includeLower,upper,includeUpper);
    }

    @Override
    public Cursor<NodeItem> nodeCursorGetFromIndexRangeSeekByPrefix(IndexDescriptor index, String prefix )
            throws IndexNotFoundKernelException
    {
        // TODO !
        return super.nodeCursorGetFromIndexRangeSeekByPrefix(index,prefix);
    }

    @Override
    public Cursor<NodeItem> nodeCursorGetFromUniqueIndexSeek(IndexDescriptor index, Object value )
            throws IndexNotFoundKernelException, IndexBrokenKernelException
    {
        // TODO !
        return super.nodeCursorGetFromUniqueIndexSeek(index,value);
    }

    @Override
    public long nodesCountIndexed(IndexDescriptor index, long nodeId, Object value )
            throws IndexNotFoundKernelException, IndexBrokenKernelException
    {
        // TODO !
        return super.nodesCountIndexed(index,nodeId,value);
    }

    // </DataReadCursors>

    // <SchemaRead>
    // TODO: Are they needed?
    /*
    @Override
    public IndexDescriptor indexGetForLabelAndPropertyKey( int labelId, int propertyKeyId )
            throws SchemaRuleNotFoundException
    {
        statement.assertOpen();
        IndexDescriptor descriptor = schemaRead().indexGetForLabelAndPropertyKey( statement, labelId, propertyKeyId );
        if ( descriptor == null )
        {
            throw new IndexSchemaRuleNotFoundException( labelId, propertyKeyId );
        }
        return descriptor;
    }

    @Override
    public Iterator<IndexDescriptor> indexesGetForLabel( int labelId )
    {
        statement.assertOpen();
        return schemaRead().indexesGetForLabel( statement, labelId );
    }

    @Override
    public Iterator<IndexDescriptor> indexesGetAll()
    {
        statement.assertOpen();
        return schemaRead().indexesGetAll( statement );
    }

    @Override
    public IndexDescriptor uniqueIndexGetForLabelAndPropertyKey( int labelId, int propertyKeyId )
            throws SchemaRuleNotFoundException, DuplicateIndexSchemaRuleException

    {
        IndexDescriptor result = null;
        Iterator<IndexDescriptor> indexes = uniqueIndexesGetForLabel( labelId );
        while ( indexes.hasNext() )
        {
            IndexDescriptor index = indexes.next();
            if ( index.getPropertyKeyId() == propertyKeyId )
            {
                if ( null == result )
                {
                    result = index;
                }
                else
                {
                    throw new DuplicateIndexSchemaRuleException( labelId, propertyKeyId, true );
                }
            }
        }

        if ( null == result )
        {
            throw new IndexSchemaRuleNotFoundException( labelId, propertyKeyId, true );
        }

        return result;
    }

    @Override
    public Iterator<IndexDescriptor> uniqueIndexesGetForLabel( int labelId )
    {
        statement.assertOpen();
        return schemaRead().uniqueIndexesGetForLabel( statement, labelId );
    }

    @Override
    public Long indexGetOwningUniquenessConstraintId( IndexDescriptor index ) throws SchemaRuleNotFoundException
    {
        statement.assertOpen();
        return schemaRead().indexGetOwningUniquenessConstraintId( statement, index );
    }

    @Override
    public Iterator<IndexDescriptor> uniqueIndexesGetAll()
    {
        statement.assertOpen();
        return schemaRead().uniqueIndexesGetAll( statement );
    }

    @Override
    public InternalIndexState indexGetState(IndexDescriptor descriptor ) throws IndexNotFoundKernelException
    {
        statement.assertOpen();
        return schemaRead().indexGetState( statement, descriptor );
    }

    @Override
    public PopulationProgress indexGetPopulationProgress( IndexDescriptor descriptor ) throws IndexNotFoundKernelException
    {
        statement.assertOpen();
        return schemaRead().indexGetPopulationProgress( statement, descriptor );
    }

    @Override
    public long indexSize( IndexDescriptor descriptor ) throws IndexNotFoundKernelException
    {
        statement.assertOpen();
        return schemaRead().indexSize( statement, descriptor );
    }

    @Override
    public double indexUniqueValuesSelectivity( IndexDescriptor descriptor ) throws IndexNotFoundKernelException
    {
        statement.assertOpen();
        return schemaRead().indexUniqueValuesPercentage( statement, descriptor );
    }

    @Override
    public String indexGetFailure( IndexDescriptor descriptor ) throws IndexNotFoundKernelException
    {
        statement.assertOpen();
        return schemaRead().indexGetFailure( statement, descriptor );
    }

    @Override
    public Iterator<NodePropertyConstraint> constraintsGetForLabelAndPropertyKey( int labelId, int propertyKeyId )
    {
        statement.assertOpen();
        return schemaRead().constraintsGetForLabelAndPropertyKey( statement, labelId, propertyKeyId );
    }

    @Override
    public Iterator<NodePropertyConstraint> constraintsGetForLabel( int labelId )
    {
        statement.assertOpen();
        return schemaRead().constraintsGetForLabel( statement, labelId );
    }

    @Override
    public Iterator<RelationshipPropertyConstraint> constraintsGetForRelationshipType( int typeId )
    {
        statement.assertOpen();
        return schemaRead().constraintsGetForRelationshipType( statement, typeId );
    }

    @Override
    public Iterator<RelationshipPropertyConstraint> constraintsGetForRelationshipTypeAndPropertyKey( int typeId,
            int propertyKeyId )
    {
        statement.assertOpen();
        return schemaRead().constraintsGetForRelationshipTypeAndPropertyKey( statement, typeId, propertyKeyId );
    }

    @Override
    public Iterator<PropertyConstraint> constraintsGetAll()
    {
        statement.assertOpen();
        return schemaRead().constraintsGetAll( statement );
    }
    */

    // </SchemaRead>


    // <TokenRead>
    @Override
    public int labelGetForName( String labelName )
    {
        //TODO: Might be faster with contains?
        KernelTransactionImplementation tx = txState();
        Iterator<Integer> it = tx.virtualLabels.keySet().iterator();
        while(it.hasNext()){
            int key = it.next();
            if(tx.virtualLabels.get(key).equals(labelName)){
                return key;
            }
        }

        return super.labelGetForName(labelName);
    }

    @Override
    public String labelGetName( int labelId ) throws LabelNotFoundKernelException
    {
        if(isVirtual(labelId)){
            KernelTransactionImplementation tx = txState();
            if(tx.virtualLabels.containsKey(labelId)){
                return tx.virtualLabels.get(labelId);
            }
            throw new LabelNotFoundKernelException("No virtual Label found for id: "+labelId,new Exception());
        }

        return super.labelGetName(labelId);
    }

    @Override
    public int propertyKeyGetForName( String propertyKeyName )
    {
        KernelTransactionImplementation tx = txState();
        Iterator<Integer> it = virtualPropertyKeyIds().iterator();
        while(it.hasNext()){
            int key = it.next();
            if(tx.virtualPropertyKeyIdsToName.get(key).equals(propertyKeyName)){
                return key;
            }
        }
        return super.propertyKeyGetForName(propertyKeyName);
    }

    @Override
    public String propertyKeyGetName( int propertyKeyId ) throws PropertyKeyIdNotFoundKernelException
    {
        if(isVirtual(propertyKeyId)){
            KernelTransactionImplementation tx = txState();
            if(virtualPropertyKeyIds().contains(propertyKeyId)){
                return tx.virtualPropertyKeyIdsToName.get(propertyKeyId);
            }
            throw new PropertyKeyIdNotFoundKernelException(propertyKeyId,new Exception());
        }

        return super.propertyKeyGetName(propertyKeyId);
    }

    @Override
    public Iterator<Token> propertyKeyGetAllTokens()
    {
        Iterator<Token> realOnes = super.propertyKeyGetAllTokens();
        ArrayList<Token> virtualOnes = new ArrayList<>();
        KernelTransactionImplementation tx = txState();
        for(int key: virtualPropertyKeyIds()){
            String name = tx.virtualPropertyKeyIdsToName.get(key);
            virtualOnes.add(new Token(name,key)); // TODO: Definitely need to test this
        }
        return new MergingTokenIterator(realOnes,virtualOnes);
    }

    @Override
    public Iterator<Token> labelsGetAllTokens()
    {
        Iterator<Token> realOnes = super.labelsGetAllTokens();
        KernelTransactionImplementation tx = txState();
        ArrayList<Token> virtualOnes = new ArrayList<>();
        for(int key:virtualLabels()){
            String name = tx.virtualLabels.get(key);
            virtualOnes.add(new Token(name,key));
        }
        return new MergingTokenIterator(realOnes,virtualOnes);
    }

    @Override
    public Iterator<Token> relationshipTypesGetAllTokens()
    {
        Iterator<Token> realOnes = super.relationshipTypesGetAllTokens();
        ArrayList<Token> virtualOnes = new ArrayList<>();
        KernelTransactionImplementation tx = txState();
        for(int key: tx.virtualRelationshipTypeIdToName.keySet()){
            String name = tx.virtualRelationshipTypeIdToName.get(key);
            virtualOnes.add(new Token(name,key));
        }
        return new MergingTokenIterator(realOnes,virtualOnes);
    }

    @Override
    public int relationshipTypeGetForName( String relationshipTypeName )
    {
        // TODO: Improvements with contains?
        KernelTransactionImplementation tx = txState();
        Iterator<Integer> it = tx.virtualRelationshipTypeIdToName.keySet().iterator();
        while(it.hasNext()){
            int key = it.next();
            if(tx.virtualRelationshipTypeIdToName.get(key).equals(relationshipTypeName)){
                return key;
            }
        }

        return super.relationshipTypeGetForName(relationshipTypeName);
    }

    @Override
    public String relationshipTypeGetName( int relationshipTypeId ) throws RelationshipTypeIdNotFoundKernelException
    {
        if(isVirtual(relationshipTypeId)){
            KernelTransactionImplementation tx = txState();
            if(tx.virtualRelationshipTypeIdToName.keySet().contains(relationshipTypeId)){
                return tx.virtualRelationshipTypeIdToName.get(relationshipTypeId);
            }
            throw new RelationshipTypeIdNotFoundKernelException(relationshipTypeId,new Exception());
        }

        return super.relationshipTypeGetName(relationshipTypeId);
    }

    @Override
    public int labelCount()
    {
        // TODO: Solution without counting same type twice if in both collections
        return super.labelCount() + virtualLabels().size();
    }

    @Override
    public int propertyKeyCount()
    {
        // TODO: Solution without counting same type twice if in both collections
        return super.propertyKeyCount() + virtualPropertyKeyIds().size();
    }

    @Override
    public int relationshipTypeCount()
    {
        // TODO: Solution without counting same type twice if in both collections
        return super.relationshipTypeCount() + txState().virtualRelationshipTypeIdToName.keySet().size();
    }

    // </TokenRead>

    // <TokenWrite>
    @Override
    public int virtualLabelGetOrCreateForName( String labelName ) throws IllegalTokenNameException,
            TooManyLabelsException, NoSuchMethodException
    {
        // Try getting the labelId
        // TODO: might be faster with contains?
        KernelTransactionImplementation tx = txState();
        Iterator<Integer> it = tx.virtualLabels.keySet().iterator();
        while(it.hasNext()){
            int key = it.next();
            if(tx.virtualLabels.get(key).equals(labelName)){
                return key;
            }
        }

        // not found, need to create
        int newId = tx.getNextVirtualLabelId();
        virtualLabelCreateForName(labelName,newId);
        return newId;
    }

    @Override
    public int virtualPropertyKeyGetOrCreateForName( String propertyKeyName ) throws IllegalTokenNameException
    {
        // Try getting the proplId
        // TODO: might be faster with contains?
        KernelTransactionImplementation tx = txState();
        Iterator<Integer> it = virtualPropertyKeyIds().iterator();
        while(it.hasNext()){
            int key = it.next();
            if(tx.virtualPropertyKeyIdsToName.get(key).equals(propertyKeyName)){
                return key;
            }
        }

        // not found, need to create
        int newId = tx.getNextVirtualPropertyId();
        virtualPropertyKeyCreateForName(propertyKeyName,newId);
        return newId;
    }

    @Override
    public int virtualRelationshipTypeGetOrCreateForName( String relationshipTypeName ) throws IllegalTokenNameException
    {
        // Try getting the relId
        // TODO: might be faster with contains?
        KernelTransactionImplementation tx = txState();
        Iterator<Integer> it = tx.virtualRelationshipTypeIdToName.keySet().iterator();
        while(it.hasNext()){
            int key = it.next();
            if(tx.virtualRelationshipTypeIdToName.get(key).equals(relationshipTypeName)){
                return key;
            }
        }

        // not found, need to create
        int newId = tx.getNextVirtualRelationshipTypeId();
        virtualRelationshipTypeCreateForName(relationshipTypeName,newId);
        return newId;
    }

    @Override
    public void virtualLabelCreateForName( String labelName, int id ) throws
            IllegalTokenNameException, TooManyLabelsException
    {
        //TODO: Token name checking missing
        txState().virtualLabels.put(id,labelName);
    }

    @Override
    public void virtualPropertyKeyCreateForName( String propertyKeyName,
            int id ) throws
            IllegalTokenNameException
    {
        //TODO: Token name checking missing
        txState().virtualPropertyKeyIdsToName.put(id,propertyKeyName);
    }

    @Override
    public void virtualRelationshipTypeCreateForName( String relationshipTypeName,
            int id ) throws
            IllegalTokenNameException
    {
        //TODO: Token name checking missing
        txState().virtualRelationshipTypeIdToName.put(id,relationshipTypeName);
    }

    // </TokenWrite>

    // <DataWrite>
    @Override
    public long virtualNodeCreate()
    {
        statement.assertOpen();
        KernelTransactionImplementation tx = txState();
        long new_id = tx.getNextVirtualNodeId();

        tx.virtualNodeIds.add(new_id);
        tx.virtualNodeIdToPropertyKeyIds.put(new_id,new LinkedHashSet<>());
        tx.virtualNodeIdToLabelIds.put(new_id,new LinkedHashSet<>());
        tx.virtualNodeIdToConnectedRelationshipIds.put(new_id,new LinkedHashSet<>());

        return new_id;
    }

    @Override
    public void nodeDelete( long nodeId )
            throws EntityNotFoundException, InvalidTransactionTypeKernelException, AutoIndexingKernelException
    {
        if(isVirtual(nodeId)) {
            try {
                virtualNodeDelete(nodeId);
            } catch (NoSuchMethodException  e) {
            }
        } else{
            super.nodeDelete(nodeId);
        }
    }

    @Override
    public int nodeDetachDelete( long nodeId ) throws KernelException
    {
        statement.assertOpen();
        if(isVirtual(nodeId)){
            //TODO: THIS SHOULD BE DOWN PROPERLY!
            nodeDelete(nodeId);

        } else{
            super.nodeDetachDelete(nodeId);
        }
        return 0;
    }

    @Override
    public long relationshipCreate(int relationshipTypeId, long startNodeId, long endNodeId) throws RelationshipTypeIdNotFoundKernelException, EntityNotFoundException {
        //if(isVirtual(startNodeId)||isVirtual(endNodeId)){
        //    // real rel between vNode and ?
        //}

        if(isVirtual(startNodeId)){
            throw new EntityNotFoundException(EntityType.NODE,startNodeId);
        }
        if(isVirtual(endNodeId)){
            throw new EntityNotFoundException(EntityType.NODE,endNodeId);
        }

        return super.relationshipCreate(relationshipTypeId, startNodeId, endNodeId);
    }

    @Override
    public long virtualRelationshipCreate( int relationshipTypeId, long startNodeId, long endNodeId )
            throws RelationshipTypeIdNotFoundKernelException, EntityNotFoundException
    {
        //TODO: Test it with all possible inputs
        //if(isVirtual(startNodeId)||isVirtual(endNodeId)){
        KernelTransactionImplementation tx = txState();

        // create a new relId
            long newId = tx.getNextVirtualRelationshipId();
            tx.virtualRelationshipIdToTypeId.put(newId,relationshipTypeId);

            Long[] nodes = new Long[2];
            nodes[0] = startNodeId;
            nodes[1] = endNodeId;

            tx.virtualRelationshipIdToVirtualNodeIds.put(newId,nodes);
            tx.virtualRelationshipIdToPropertyKeyIds.put(newId,new LinkedHashSet<Integer>());

            return newId;
        //} else {
        //    return super.relationshipCreate(relationshipTypeId, startNodeId, endNodeId);
        //}
    }

    @Override
    public void virtualNodeDelete(long nodeId) throws NoSuchMethodException, EntityNotFoundException {
        KernelTransactionImplementation tx = txState();
        if (tx.virtualNodeIds.contains(nodeId)) {
            //TODO: needs more checks!

            tx.virtualNodeIds.remove(nodeId);

            tx.virtualNodeIdToLabelIds.remove(nodeId);
            tx.virtualNodeIdToPropertyKeyIds.remove(nodeId);

            // TODO: Remove refs that are returned from those calls

            // AND rel prop

            tx.virtualNodeIdToConnectedRelationshipIds.remove(nodeId);
            // virtualRelationshipIdToVirtualNodeIds SHOULD BE CLEARED BEFORE!
        } else {
            throw new EntityNotFoundException(EntityType.NODE, nodeId);
        }
    }

    @Override
    public void relationshipDelete( long relationshipId )
            throws EntityNotFoundException, InvalidTransactionTypeKernelException, AutoIndexingKernelException
    {
        if(isVirtual(relationshipId)){
            KernelTransactionImplementation tx = txState();
            if(relationshipExists(relationshipId)){
                tx.virtualRelationshipIdToTypeId.remove(relationshipId);
                tx.virtualRelationshipIdToPropertyKeyIds.remove(relationshipId);
                tx.virtualRelationshipIdToVirtualNodeIds.remove(relationshipId);
            } else{
                throw new EntityNotFoundException(EntityType.RELATIONSHIP,relationshipId);
            }

        } else {
            super.relationshipDelete(relationshipId);
        }
    }

    @Override
    public boolean nodeAddLabel( long nodeId, int labelId )
            throws EntityNotFoundException, ConstraintValidationKernelException
    {
        if(isVirtual(nodeId)){
            if(nodeExists(nodeId)){
                // Todo: check if this labelId exist?

                txState().virtualNodeIdToLabelIds.get(nodeId).add(labelId);
                return true;
            } else{
                throw new EntityNotFoundException(EntityType.NODE,nodeId);
            }
        }
        return super.nodeAddLabel(nodeId,labelId);
    }

    @Override
    public boolean nodeRemoveLabel( long nodeId, int labelId ) throws EntityNotFoundException
    {
        if(isVirtual(nodeId)){
            if(nodeExists(nodeId)){
                // Todo: check if this labelId exist?

                txState().virtualNodeIdToLabelIds.get(nodeId).remove(labelId);
                return true;
            } else{
                throw new EntityNotFoundException(EntityType.NODE,nodeId);
            }
        } else {
            return super.nodeRemoveLabel(nodeId, labelId);
        }
    }

    @Override
    public Property nodeSetProperty(long nodeId, DefinedProperty property )
            throws EntityNotFoundException, ConstraintValidationKernelException, AutoIndexingKernelException, InvalidTransactionTypeKernelException
    {
        if(isVirtual(nodeId)){
            KernelTransactionImplementation tx = txState();
            if(nodeExists(nodeId)){
                // Todo: check if this propId exist?

                PropertyValueId key=null;
                Object oldValue = null;
                Iterator<PropertyValueId> it = tx.virtualPropertyIdToValueForNodes.keySet().iterator();
                while(it.hasNext()){
                    PropertyValueId pId = it.next();
                    if(pId.getEntityId()==nodeId && pId.getPropertyKeyId()==property.propertyKeyId()){
                        // found it
                        key = pId;
                        oldValue = tx.virtualPropertyIdToValueForNodes.get(pId);
                        break;
                    }
                }
                if(key==null){
                    // not already set
                    key = new PropertyValueId(nodeId,property.propertyKeyId());
                }

                tx.virtualPropertyIdToValueForNodes.put(key,property.value());
                tx.virtualNodeIdToPropertyKeyIds.get(nodeId).add(property.propertyKeyId());
                if(oldValue!=null){
                    return Property.property(key.getPropertyKeyId(),oldValue);
                } else{
                    return null; // TODO: Test this
                }
            } else{
                throw new EntityNotFoundException(EntityType.NODE,nodeId);
            }
        } else {
            return super.nodeSetProperty(nodeId, property);
        }
    }

    @Override
    public Property relationshipSetProperty(long relationshipId, DefinedProperty property )
            throws EntityNotFoundException, AutoIndexingKernelException, InvalidTransactionTypeKernelException
    {
        if(isVirtual(relationshipId)){
            if(relationshipExists(relationshipId)){
                // Todo: check if this propId exist?
                KernelTransactionImplementation tx = txState();
                PropertyValueId key=null;
                Object oldValue=null;
                Iterator<PropertyValueId> it = tx.virtualPropertyIdToValueForRels.keySet().iterator();
                while(it.hasNext()){
                    PropertyValueId pId = it.next();
                    if(pId.getEntityId()==relationshipId && pId.getPropertyKeyId()==property.propertyKeyId()){
                        // found it
                        key = pId;
                        oldValue = tx.virtualPropertyIdToValueForRels.get(pId);
                        break;
                    }
                }
                if(key==null){
                    // not already set
                    key = new PropertyValueId(relationshipId,property.propertyKeyId());
                }

                tx.virtualPropertyIdToValueForRels.put(key,property.value());

                Set<Integer> set = tx.virtualRelationshipIdToPropertyKeyIds.get(relationshipId);
                if(set==null){
                    set = new TreeSet<>();
                }
                set.add(property.propertyKeyId());
                tx.virtualRelationshipIdToPropertyKeyIds.put(relationshipId,set);

                if(oldValue!=null){
                    return Property.property(key.getPropertyKeyId(),oldValue);
                } else{
                    return null; // TODO: Test this
                }
            } else{
                throw new EntityNotFoundException(EntityType.RELATIONSHIP,relationshipId);
            }
        } else {
            return super.relationshipSetProperty(relationshipId,property);
        }
    }

    @Override
    public Property graphSetProperty(DefinedProperty property )
    {
        // TODO !
        /*

        if(isVirtual(property.propertyKeyId())){

            // set it on ALL of the entities

        } else{
            return super.graphSetProperty(property);
        }

        */
        return super.graphSetProperty(property);
    }

    @Override
    public Property nodeRemoveProperty(long nodeId, int propertyKeyId )
            throws EntityNotFoundException, AutoIndexingKernelException, InvalidTransactionTypeKernelException
    {
        if(isVirtual(nodeId)){
            KernelTransactionImplementation tx = txState();
            if(nodeExists(nodeId)){
                Object value = getPropertyValueForNodes(nodeId,propertyKeyId);
                Property returnProp;
                if(value==null){
                    returnProp = Property.noNodeProperty(nodeId,propertyKeyId);
                    return returnProp;
                }
                returnProp = Property.property(propertyKeyId,value);
                tx.virtualNodeIdToPropertyKeyIds.get(nodeId).remove(propertyKeyId);

                PropertyValueId p = new PropertyValueId(nodeId,propertyKeyId);
                tx.virtualPropertyIdToValueForNodes.remove(p); // TODO: Test this

                return returnProp;

            } else{
                throw new EntityNotFoundException(EntityType.NODE,nodeId);
            }
        } else {
            return super.nodeRemoveProperty(nodeId, propertyKeyId);
        }
    }

    @Override
    public Property relationshipRemoveProperty(long relationshipId, int propertyKeyId )
            throws EntityNotFoundException, AutoIndexingKernelException, InvalidTransactionTypeKernelException
    {
        if(isVirtual(relationshipId)){
            KernelTransactionImplementation tx = txState();
            if(nodeExists(relationshipId)){
                Object value = getPropertyValueForRels(relationshipId,propertyKeyId);
                Property returnProp;
                if(value==null){
                    returnProp = Property.noRelationshipProperty(relationshipId,propertyKeyId);
                    return returnProp;
                }
                returnProp = Property.property(propertyKeyId,value);
                tx.virtualRelationshipIdToPropertyKeyIds.get(relationshipId).remove(propertyKeyId);

                PropertyValueId p = new PropertyValueId(relationshipId,propertyKeyId);
                tx.virtualPropertyIdToValueForRels.remove(p); // TODO: Test this

                return returnProp;

            } else{
                throw new EntityNotFoundException(EntityType.RELATIONSHIP,relationshipId);
            }
        } else {
            return super.relationshipRemoveProperty(relationshipId, propertyKeyId);
        }
    }

    @Override
    public Property graphRemoveProperty(int propertyKeyId )
    {
        // TODO !!!!!
        return super.graphRemoveProperty(propertyKeyId);
    }

    //@Override
    //public RawIterator<Object[], ProcedureException> procedureCallWrite(ProcedureName name, Object[] input ) throws ProcedureException
    //{
    //    // TODO ?
    //    return super.procedureCallWrite(name,input);
    //}

    // </DataWrite>

    // <SchemaWrite>
    // TODO: Are they needed?
    /*
    @Override
    public IndexDescriptor indexCreate( int labelId, int propertyKeyId )
            throws AlreadyIndexedException, AlreadyConstrainedException
    {
        statement.assertOpen();
        return schemaWrite().indexCreate( statement, labelId, propertyKeyId );
    }

    @Override
    public void indexDrop( IndexDescriptor descriptor ) throws DropIndexFailureException
    {
        statement.assertOpen();
        schemaWrite().indexDrop( statement, descriptor );
    }

    @Override
    public UniquenessConstraint uniquePropertyConstraintCreate( int labelId, int propertyKeyId )
            throws CreateConstraintFailureException, AlreadyConstrainedException, AlreadyIndexedException
    {
        statement.assertOpen();
        return schemaWrite().uniquePropertyConstraintCreate( statement, labelId, propertyKeyId );
    }

    @Override
    public NodePropertyExistenceConstraint nodePropertyExistenceConstraintCreate( int labelId, int propertyKeyId )
            throws CreateConstraintFailureException, AlreadyConstrainedException
    {
        statement.assertOpen();
        return schemaWrite().nodePropertyExistenceConstraintCreate( statement, labelId, propertyKeyId );
    }

    @Override
    public RelationshipPropertyExistenceConstraint relationshipPropertyExistenceConstraintCreate(
            int relTypeId, int propertyKeyId )
            throws CreateConstraintFailureException, AlreadyConstrainedException
    {
        statement.assertOpen();
        return schemaWrite().relationshipPropertyExistenceConstraintCreate( statement, relTypeId, propertyKeyId );
    }

    @Override
    public void constraintDrop( NodePropertyConstraint constraint ) throws DropConstraintFailureException
    {
        statement.assertOpen();
        schemaWrite().constraintDrop( statement, constraint );
    }

    @Override
    public void constraintDrop( RelationshipPropertyConstraint constraint ) throws DropConstraintFailureException
    {
        statement.assertOpen();
        schemaWrite().constraintDrop( statement, constraint );
    }

    @Override
    public void uniqueIndexDrop( IndexDescriptor descriptor ) throws DropIndexFailureException
    {
        statement.assertOpen();
        schemaWrite().uniqueIndexDrop( statement, descriptor );
    }
    */

    // <Counts>

    @Override
    public long countsForNode( int labelId )
    {
        // TODO: Sascha: Apply id filter on real nodes
        return countVirtualNodes(labelId) + super.countsForNode(labelId);
    }

    @Override
    public long countsForNodeWithoutTxState( int labelId )
    {
        // TODO: Sascha: Apply id filter on real nodes
        return countVirtualNodes(labelId)+ super.countsForNodeWithoutTxState(labelId);
    }

    @Override
    public long countsForRelationship( int startLabelId, int typeId, int endLabelId )
    {
        // TODO: Sascha: Apply id filter on real rels
        return countVirtualRelationships(startLabelId,typeId,endLabelId) +
                super.countsForRelationship(startLabelId,typeId,endLabelId);
    }

    @Override
    public long countsForRelationshipWithoutTxState( int startLabelId, int typeId, int endLabelId )
    {
        // TODO: Sascha: Apply id filter on real rels
        return countVirtualRelationships(startLabelId,typeId,endLabelId) +
                super.countsForRelationshipWithoutTxState(startLabelId,typeId,endLabelId);
    }

    @Override
    public DoubleLongRegister indexUpdatesAndSize(IndexDescriptor index, DoubleLongRegister target )
            throws IndexNotFoundKernelException
    {
        // TODO ?
        return super.indexUpdatesAndSize(index,target);
    }

    @Override
    public DoubleLongRegister indexSample(IndexDescriptor index, DoubleLongRegister target )
            throws IndexNotFoundKernelException
    {
        // TODO ?
        return super.indexSample(index,target);
    }

    // </Counts>


    // HELPER FUNCTIONS

    private boolean isVirtual(long entityId){
        return entityId<-1;
    }

    private Set<Long> virtualRelationshipIds(){
        return txState().virtualRelationshipIdToTypeId.keySet();
    }

    private Set<Integer> virtualPropertyKeyIds(){
        return txState().virtualPropertyKeyIdsToName.keySet();
    }

    private Set<Integer> virtualLabels(){
        return txState().virtualLabels.keySet();
    }

    private Object getPropertyValueForNodes(long nodeId, int propertykey){
        KernelTransactionImplementation tx = txState();
        Iterator<PropertyValueId> it = tx.virtualPropertyIdToValueForNodes.keySet().iterator();
        while(it.hasNext()){
            PropertyValueId pId = it.next();
            if(pId.getEntityId()==nodeId && pId.getPropertyKeyId()==propertykey){
                // success!
                return tx.virtualPropertyIdToValueForNodes.get(pId);
            }
        }
        return null;
    }

    private Object getPropertyValueForRels(long relId, int propertykey){
        KernelTransactionImplementation tx = txState();
        Iterator<PropertyValueId> it =tx.virtualPropertyIdToValueForRels.keySet().iterator();
        while(it.hasNext()){
            PropertyValueId pId = it.next();
            if(pId.getEntityId()==relId && pId.getPropertyKeyId()==propertykey){
                // success!
                return tx.virtualPropertyIdToValueForRels.get(pId);
            }
        }
        return null;
    }

    private int countVirtualNodes(int labelId){
        int count = 0;
        KernelTransactionImplementation tx = txState();
        IdFilter nodeIdFilter = tx.getNodeIdFilter();
        Iterator<Map.Entry<Long, Set<Integer>>> it = tx.virtualNodeIdToLabelIds.entrySet().iterator();
        while(it.hasNext()){
            Map.Entry<Long,Set<Integer>> entry = it.next();
            Long key = entry.getKey();
            // Filter!
            if(!nodeIdFilter.isUnused()){
                if(!nodeIdFilter.idIsInFilter(key)){
                    continue;
                }
            }

            Set<Integer> set = entry.getValue();
            if(set.contains(labelId)){
                count++;
            }
        }
        return count;
    }

    private int countVirtualRelationships( int startLabelId, int typeId, int endLabelId ){
        int count = 0;
        int ANY_LABEL = -1;
        int ANY_REL = -1;
        KernelTransactionImplementation tx = txState();
        IdFilter relIdFilter = tx.getRelationshipIdFilter();

        Set<Long> possibleStartNodes;
        Set<Long> possibleEndNodes;

        // prepare possibleStartNodes
        if(startLabelId==ANY_LABEL){
            possibleStartNodes = tx.virtualNodeIds;
        } else{
            // specific startLabelId
            possibleStartNodes = getVirtualNodesForLabel(startLabelId);
        }

        // prepare possibleEndNodes
        if(endLabelId==ANY_LABEL){
            possibleEndNodes = tx.virtualNodeIds;
        } else{
            // specific endLabelId
            possibleEndNodes = getVirtualNodesForLabel(endLabelId);
        }

        if((possibleStartNodes.size()==0)||(possibleEndNodes.size()==0)){
            return 0;
        }

        // do the actual counting
        Iterator<Long> relIdIterator = tx.virtualRelationshipIdToVirtualNodeIds.keySet().iterator();
        while(relIdIterator.hasNext()){
            Long relId = relIdIterator.next();
            // filter!
            if(!relIdFilter.isUnused()){
                if(!relIdFilter.idIsInFilter(relId)){
                    continue;
                }
            }

            Long[] nodes = tx.virtualRelationshipIdToVirtualNodeIds.get(relId);
            if((possibleStartNodes.contains(nodes[0]))&&
                    (possibleEndNodes.contains(nodes[1]))){
                if(typeId== ANY_REL){
                    // ALL
                    count++;
                } else{
                    // only typeId
                    if(tx.virtualRelationshipIdToTypeId.get(relId)==typeId){
                        count++;
                    }
                }
            }
        }

        return count;
    }

    private Set<Long> getVirtualNodesForLabel(int labelId){
        Set<Long> returnSet = new TreeSet<>();
        KernelTransactionImplementation tx = txState();
        Iterator<Long> nodeIdIterator = tx.virtualNodeIdToLabelIds.keySet().iterator();
        IdFilter nodeIdFilter = tx.getNodeIdFilter();
        while(nodeIdIterator.hasNext()){
            Long id = nodeIdIterator.next();
            // filter
            if(!nodeIdFilter.isUnused()){
                if(!nodeIdFilter.idIsInFilter(id)){
                    continue;
                }
            }

            if(tx.virtualNodeIdToLabelIds.get(id).contains(labelId)){
                returnSet.add(id);
            }
        }
        return returnSet;
    }

    /*private int getTransactionId(){
        /*String txString = tx.toString();

        //"KernelTransaction[" + this.locks.getLockSessionId() + "]";
        int posStart = txString.indexOf("[");
        int posEnd = txString.indexOf("]");
        txString = txString.substring(posStart+1,posEnd);

        return Integer.parseInt(txString);

        return txState().getLockSessionId();
    }*/

    /*private int authenticate(){

        int taId = getTransactionId();
        if(knowntransactionIds.contains(taId)){
            // all is fine

        } else{
            // new id
            virtualRelationshipIdToTypeId.put(taId,new HashMap<>());
            virtualNodeIds.put(taId,new HashSet<>());
            virtualPropertyKeyIdsToName.put(taId, new HashMap<>());
            virtualLabels.put(taId, new HashMap<>());
            virtualRelationshipTypeIdToName.put(taId, new HashMap<>());

            virtualNodeIdToPropertyKeyIds.put(taId, new HashMap<>());
            virtualRelationshipIdToPropertyKeyIds.put(taId, new HashMap<>());
            virtualNodeIdToLabelIds.put(taId, new HashMap<>());
            virtualRelationshipIdToVirtualNodeIds.put(taId, new HashMap<>());
            virtualNodeIdToConnectedRelationshipIds.put(taId, new HashMap<>());

            virtualPropertyIdToValueForNodes = new HashMap<>();
            virtualPropertyIdToValueForRels = new HashMap<>();

            knowntransactionIds.add(taId);

            // TODO: might do clean up of old ids here, only if they are done
        }

        return taId;
    } */

    @Override
    public Property nodeSetVirtualProperty(long nodeId, DefinedProperty property )
            throws EntityNotFoundException, ConstraintValidationKernelException, AutoIndexingKernelException, InvalidTransactionTypeKernelException
    {
        statement.assertOpen();

        if(isVirtual(nodeId)&& nodeExists(nodeId)){
            KernelTransactionImplementation tx = txState();
            if(isVirtual(property.propertyKeyId())){
                tx.virtualNodeIdToPropertyKeyIds.get(nodeId).add(property.propertyKeyId());

                PropertyValueId key = new PropertyValueId(nodeId,property.propertyKeyId());
                tx.virtualPropertyIdToValueForNodes.put(key,property.value());

                return property;

            } else{
                throw new InvalidTransactionTypeKernelException("the property on a virtual node should be virtual too");
            }

        } else{
            throw new EntityNotFoundException(EntityType.NODE,nodeId);
        }
    }


    @Override
    public Property relationshipSetVirtualProperty(long relId, DefinedProperty property )
            throws EntityNotFoundException, ConstraintValidationKernelException, AutoIndexingKernelException, InvalidTransactionTypeKernelException
    {
        statement.assertOpen();

        if(isVirtual(relId) && relationshipExists(relId)){
            KernelTransactionImplementation tx = txState();
            if(isVirtual(property.propertyKeyId())){
                tx.virtualRelationshipIdToPropertyKeyIds.get(relId).add(property.propertyKeyId());

                PropertyValueId key = new PropertyValueId(relId,property.propertyKeyId());
                tx.virtualPropertyIdToValueForRels.put(key,property.value());

                return property;

            } else{
                throw new InvalidTransactionTypeKernelException("the property on a virtual relationship should be virtual too");
            }

        } else{
            throw new EntityNotFoundException(EntityType.RELATIONSHIP,relId);
        }
    }

    public void cacheView(String name, List<Collection<Long>> sets){
        txState().addViewToCache(name,sets);
    }

    public List<Collection<Long>> getCachedView(String name){
        return txState().getCachedView(name);
    }

    public void enableViews(String[] cachedViewnames){
        txState().enableViews(cachedViewnames);
    }

    public void clearNodeIdFilter(){
        txState().clearNodeIdFilter();
    }

    public void clearRelationshipIdFilter(){
        txState().clearRelationshipIdFilter();
    }

    private KernelTransactionImplementation txState(){
        return ((KernelTransactionImplementation) tx);
    }
}
