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
import org.neo4j.collection.primitive.PrimitiveLongCollections;
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

import java.util.*;

// TODO Sascha
public class VirtualOperationsFacade extends OperationsFacade
{

    class PropertyValueId {

        private long entityId;
        private int propKeyId;
        private int transactionId;

        public PropertyValueId(long entity, int prop, int ta_id){
            this.entityId = entity;
            this.propKeyId = prop;
            this.transactionId = ta_id;
        }

        public long getEntityId(){
            return entityId;
        }

        public int getPropertyKeyId(){
            return propKeyId;
        }

        public int getTransactionId(){ return transactionId;};

        @Override
        public boolean equals(Object obj) {
            if(obj.getClass()==this.getClass()){
                PropertyValueId other = (PropertyValueId) obj;
                if((this.entityId==other.entityId) && (this.propKeyId == other.propKeyId) &&
                        (this.transactionId==other.transactionId)){
                    return true;
                }
                return false;
            } else{
                return false;
            }
        }
    }


    public class IdFilter{

        private Set<Long> ids;
        private boolean unused; // previously: empty

        public IdFilter(){
            ids = new HashSet<>();
            unused = true;
        }

        public void addAll(Collection<Long> set){
            ids.addAll(set);
            unused = false;
            //if(unused){
            //   if(set.size()>0){
            //       unused=false;
            //   }
            //}
        }

        //TODO: Write test that checks view with no actual elements does not have access to anything!
        public void clear(){
            ids.clear();
            unused = true;
        }

        public boolean isUnused(){
            return unused;
        }

        public boolean idIsInFilter(long id){
            return ids.contains(id);
        }
    }

    class MyFilteredPrimitiveLongIterator implements PrimitiveLongIterator{

        private IdFilter filter;
        private PrimitiveLongIterator iterator;

        public MyFilteredPrimitiveLongIterator(IdFilter f, PrimitiveLongIterator originalIterator){
            super();
            filter = f;

            if(filter.isUnused()){
                iterator = originalIterator;
            } else {
                // Apply filter!
                Set<Long> resultSet = new HashSet<>();
                while(originalIterator.hasNext()){
                    long id = originalIterator.next();
                    if(filter.idIsInFilter(id)){
                        resultSet.add(id);
                    }
                }

                iterator = PrimitiveLongCollections.toPrimitiveIterator(resultSet.iterator());
            }
        }

        @Override
        public boolean hasNext() {
            return iterator.hasNext();
        }

        @Override
        public long next() {
            return iterator.next();
        }
    }

    private Map<Integer,TreeMap<Long,Integer>> virtualRelationshipIdToTypeId; // actualData and ref to types
    private Map<Integer,SortedSet<Long>> virtualNodeIds;  // "actual data"
    private Map<Integer,TreeMap<Integer,String>> virtualPropertyKeyIdsToName;  // "actual data"
    private Map<Integer,TreeMap<Integer,String>> virtualLabels; // actual data
    private Map<Integer,TreeMap<Integer,String>> virtualRelationshipTypeIdToName; // actual data

    //entityId + propKeyId -> value
    private Map<PropertyValueId,Object> virtualPropertyIdToValueForNodes;
    private Map<PropertyValueId,Object> virtualPropertyIdToValueForRels;

    //private Map<Integer,Object> virtualPropertiyIdsToObjectForNodes; // actual data
    //private Map<Integer,Object> virtualPropertiyIdsToObjectForRels; // actual data

    private Map<Integer,Map<Long,Set<Integer>>> virtualNodeIdToPropertyKeyIds;   // Natural ordering 1 2 3 10 12 ...  -> first one is the smallest with negative
    private Map<Integer,Map<Long,Set<Long>>> virtualNodeIdToConnectedRelationshipIds;
    private Map<Integer,Map<Long,Set<Integer>>> virtualRelationshipIdToPropertyKeyIds;
    private Map<Integer,Map<Long,Set<Integer>>> virtualNodeIdToLabelIds;
    private Map<Integer,Map<Long,Long[]>> virtualRelationshipIdToVirtualNodeIds; // Node[0] = from, Node[1] = to

    private SortedSet<Integer> knowntransactionIds;

    public IdFilter nodeIdFilter; //TODO change to private...
    public IdFilter relIdFilter;  //TODO: Change to private

    VirtualOperationsFacade(KernelTransaction tx, KernelStatement statement,
                            Procedures procedures )
    {
        super(tx,statement,procedures);

        //initialize(operations);

        nodeIdFilter = new IdFilter();
        relIdFilter = new IdFilter();

        virtualRelationshipIdToTypeId = new HashMap<>();
        virtualNodeIds = new HashMap<>();
        virtualPropertyKeyIdsToName = new HashMap<>();
        virtualLabels = new HashMap<>();
        virtualRelationshipTypeIdToName = new HashMap<>();

        virtualNodeIdToPropertyKeyIds = new HashMap<>();
        virtualRelationshipIdToPropertyKeyIds = new HashMap<>();
        virtualNodeIdToLabelIds = new HashMap<>();
        virtualRelationshipIdToVirtualNodeIds = new HashMap<>();
        virtualNodeIdToConnectedRelationshipIds = new HashMap<>();

        virtualPropertyIdToValueForNodes = new HashMap<>();
        virtualPropertyIdToValueForRels = new HashMap<>();

        knowntransactionIds = new TreeSet<>();

        //virtualPropertiyKeyIdsToObjectForNodes = new HashMap<>();
        //virtualPropertiyKeyIdsToObjectForRels = new HashMap<>();
    }

    public void initialize( StatementOperationParts operationParts )
    {
        super.initialize( operationParts);
    }

    //TODO: Sascha: Apply filter to every LongIterator :-/

    // <DataRead>

    @Override
    public PrimitiveLongIterator nodesGetAll()
    {
        PrimitiveLongIterator allRealNodes = super.nodesGetAll();
        MergingPrimitiveLongIterator bothNodeIds = new MergingPrimitiveLongIterator(allRealNodes,
                virtualNodeIds.get(authenticate()));
        return new MyFilteredPrimitiveLongIterator(nodeIdFilter,bothNodeIds);
    }

    @Override
    public PrimitiveLongIterator relationshipsGetAll()
    {
        PrimitiveLongIterator allRealRels = super.relationshipsGetAll();
        MergingPrimitiveLongIterator bothRelIds =
                new MergingPrimitiveLongIterator(allRealRels,virtualRelationshipIds());
        return new MyFilteredPrimitiveLongIterator(relIdFilter,bothRelIds);
    }

    @Override
    public PrimitiveLongIterator nodesGetForLabel(int labelId )
    {

        PrimitiveLongIterator originalIT = super.nodesGetForLabel(labelId);

        ArrayList<Long> resultList = new ArrayList<>();

        // TODO: Improvements possible
        for(Long nodeId : virtualNodeIdToLabelIds.get(authenticate()).keySet()) {
            Set<Integer> labelIds = virtualNodeIdToLabelIds.get(authenticate()).get(nodeId);
            if(labelIds.contains(labelId)){
                resultList.add(nodeId);
            }
        }
        return new MyFilteredPrimitiveLongIterator(nodeIdFilter, new MergingPrimitiveLongIterator(originalIT,resultList));
    }

    @Override
    public PrimitiveLongIterator nodesGetFromIndexSeek(IndexDescriptor index, Object value )
            throws IndexNotFoundKernelException
    {
        // TODO !
        return new MyFilteredPrimitiveLongIterator(nodeIdFilter, super.nodesGetFromIndexSeek(index,value));
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
        return new MyFilteredPrimitiveLongIterator(nodeIdFilter, super.nodesGetFromIndexRangeSeekByNumber(index,lower,includeLower,upper,includeUpper));
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
        return new MyFilteredPrimitiveLongIterator(nodeIdFilter, super.nodesGetFromIndexRangeSeekByString(index,lower,includeLower,upper,includeUpper));
    }

    @Override
    public PrimitiveLongIterator nodesGetFromIndexRangeSeekByPrefix(IndexDescriptor index, String prefix )
            throws IndexNotFoundKernelException
    {
        // TODO !
        return new MyFilteredPrimitiveLongIterator(nodeIdFilter, super.nodesGetFromIndexRangeSeekByPrefix(index,prefix));
    }

    @Override
    public PrimitiveLongIterator nodesGetFromIndexScan(IndexDescriptor index )
            throws IndexNotFoundKernelException
    {
        // TODO !
        return new MyFilteredPrimitiveLongIterator(nodeIdFilter, super.nodesGetFromIndexScan(index));
    }

    @Override
    public PrimitiveLongIterator nodesGetFromIndexContainsScan(IndexDescriptor index, String term )
            throws IndexNotFoundKernelException
    {
        // TODO !
        return new MyFilteredPrimitiveLongIterator(nodeIdFilter, super.nodesGetFromIndexContainsScan(index,term));
    }

    @Override
    public PrimitiveLongIterator nodesGetFromIndexEndsWithScan(IndexDescriptor index, String suffix )
            throws IndexNotFoundKernelException
    {
        // TODO !
        return new MyFilteredPrimitiveLongIterator(nodeIdFilter, super.nodesGetFromIndexEndsWithScan(index,suffix));
    }

    @Override
    public long nodeGetFromUniqueIndexSeek(IndexDescriptor index, Object value )
            throws IndexNotFoundKernelException, IndexBrokenKernelException
    {
        long candidateId = super.nodeGetFromUniqueIndexSeek(index,value);
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
        if(!nodeIdFilter.isUnused()){
            // -> is used
            if(!nodeIdFilter.idIsInFilter(nodeId)){
                // but not in filter, so it dont exists
                return false;
            }
        }

        if(isVirtual(nodeId)){
            return virtualNodeIds.get(authenticate()).contains(nodeId);
        } else {
            return super.nodeExists(nodeId);
        }
    }

    @Override
    public boolean relationshipExists( long relId )
    {
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
        if(isVirtual(nodeId)){
            if(nodeExists(nodeId)){
                Set<Integer> labelIds = virtualNodeIdToLabelIds.get(authenticate()).get(nodeId);
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
        if(isVirtual(nodeId)){
            if(nodeExists(nodeId)){
                Set<Integer> labelIds = virtualNodeIdToLabelIds.get(authenticate()).get(nodeId);
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
        if(isVirtual(nodeId)){
            if(nodeExists(nodeId)){
                Set<Integer> propIds = virtualNodeIdToPropertyKeyIds.get(authenticate()).get(nodeId);
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
        if(isVirtual(nodeId)){
            if(nodeExists(nodeId)){
                // assert that the property belongs to this node
                Set<Integer> props = virtualNodeIdToPropertyKeyIds.get(authenticate()).get(nodeId);
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
        Set<RelationshipItem> foundItems = new HashSet<>();
        RelationshipIterator realIt = null;
        // get the real ones
        if(!isVirtual(nodeId)) {
            realIt = super.nodeGetRelationships(nodeId, direction,relTypes);

            while (realIt.hasNext()) {
                Long rId = realIt.next();

                if(!relIdFilter.isUnused()){
                    // -> is used
                    if(!relIdFilter.idIsInFilter(rId)){
                        // but not in filter, so it 'dont exists' in this scope
                        continue;
                    }
                }
                //if(nodeId==1) {
                //    //System.Message.println(relationshipCursor(rId).get().id());
                //    System.Message.println(super.relationshipCursor(rId).get().id());
                //    System.Message.println(super.relationshipCursor(rId).get().id());
                //}
                //super.relationshipCursor(rId).get().id(); // because...
                Cursor<RelationshipItem> cursor = super.relationshipCursorGetAll();
                while(cursor.next()){
                    RelationshipItem i = cursor.get();
                    if(i.id()==rId){
                        foundItems.add(i);
                        break;
                    }
                }

                //foundItems.add(super.relationshipCursor(rId).get());
            }
        }

        // no actual rel there, thats okay! on to the virtual stuff
        Set<Long> relIds = virtualRelationshipIdToVirtualNodeIds.get(authenticate()).keySet();
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
                if(type==virtualRelationshipIdToTypeId.get(authenticate()).get(relId)){
                    Long[] nodeIds = virtualRelationshipIdToVirtualNodeIds.get(authenticate()).get(relId);
                    switch (direction) {
                        case INCOMING:
                            if (nodeIds[1].equals(nodeId)) {
                                foundItems.add(new VirtualRelationshipItem(nodeIds[0],nodeIds[1],
                                        virtualRelationshipIdToTypeId.get(authenticate()).get(relId),relId, this));
                            }
                            break;
                        case OUTGOING:
                            if (nodeIds[0].equals(nodeId)) {
                                foundItems.add(new VirtualRelationshipItem(nodeIds[0],nodeIds[1],
                                        virtualRelationshipIdToTypeId.get(authenticate()).get(relId),relId, this));
                            }
                            break;
                        case BOTH:
                            if (nodeIds[0].equals(nodeId) || nodeIds[1].equals(nodeId)) {
                                foundItems.add(new VirtualRelationshipItem(nodeIds[0],nodeIds[1],
                                        virtualRelationshipIdToTypeId.get(authenticate()).get(relId),relId, this));
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

        Set<RelationshipItem> foundItems = new HashSet<>();

        // get the real ones
        if(!isVirtual(nodeId)) {
            RelationshipIterator realIt = super.nodeGetRelationships(nodeId, direction);
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
                }
            }
        }
        Set<Long> relIds = virtualRelationshipIdToVirtualNodeIds.get(authenticate()).keySet();

        for (Long relId : relIds) {

            if(!relIdFilter.isUnused()){
                // -> is used
                if(!relIdFilter.idIsInFilter(relId)){
                    // but not in filter, so it 'dont exists' in this scope
                    continue;
                }
            }

            Long[] nodeIds = virtualRelationshipIdToVirtualNodeIds.get(authenticate()).get(relId);
            switch (direction) {
                case INCOMING:
                    if (nodeIds[1].equals(nodeId)) {
                        int type = virtualRelationshipIdToTypeId.get(authenticate()).get(relId);
                        foundItems.add(new VirtualRelationshipItem(nodeIds[0],nodeIds[1],
                                type,relId, this));
                    }
                    break;
                case OUTGOING:
                    if (nodeIds[0].equals(nodeId)) {
                        int type = virtualRelationshipIdToTypeId.get(authenticate()).get(relId);
                        foundItems.add(new VirtualRelationshipItem(nodeIds[0],nodeIds[1],
                                type,relId, this));
                    }
                    break;
                case BOTH:
                    if (nodeIds[0].equals(nodeId) || nodeIds[1].equals(nodeId)) {
                        int type = virtualRelationshipIdToTypeId.get(authenticate()).get(relId);
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
            //return c;
            //return new MergingRelationshipIterator(null, new MergingPrimitiveLongIterator(null, foundItems));
        //} else{
        //    return super.nodeGetRelationships(nodeId, direction);
        //}
    }

    @Override
    public int nodeGetDegree(long nodeId, Direction direction, int relType ) throws EntityNotFoundException
    {
        int degree = 0;
        /*if(!isVirtual(nodeId)){
            degree+= super.nodeGetDegree(nodeId,direction,relType);
        }*/
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
        /*if(!isVirtual(nodeId)){
            degree+= super.nodeGetDegree(nodeId,direction,relType);
        }*/
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
            if(nodeExists(nodeId)){
                Set<Long> relIds = virtualNodeIdToConnectedRelationshipIds.get(authenticate()).get(nodeId);
                Set<Integer> resultSet = new HashSet<>();
                for(Long relId:relIds){
                    resultSet.add(virtualRelationshipIdToTypeId.get(authenticate()).get(relId)); // this should not be null! TODO: Tests to ensure that
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
            if(relationshipExists(relationshipId)){
                Set<Integer> propIds = virtualRelationshipIdToPropertyKeyIds.get(authenticate()).get(relationshipId);
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
            if(relationshipExists(relationshipId)){
                // assert that this rel has got that prop -> already covered by relationshipHasProperty but are they always chained?
                Set<Integer> propIds = virtualRelationshipIdToPropertyKeyIds.get(authenticate()).get(relationshipId);
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
            Iterator<Long> it = virtualNodeIds.get(authenticate()).iterator();
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
            return new MergingPrimitiveIntIterator(null, virtualNodeIdToPropertyKeyIds.get(authenticate()).get(nodeId));
        } else {
            return super.nodeGetPropertyKeys(nodeId);
        }
    }

    @Override
    public PrimitiveIntIterator relationshipGetPropertyKeys(long relationshipId ) throws EntityNotFoundException
    {
        if(isVirtual(relationshipId)){
            return new MergingPrimitiveIntIterator(null, virtualRelationshipIdToPropertyKeyIds.get(authenticate()).get(relationshipId));
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
            // this might do the job -> TODO test that
            if(relationshipExists(relId)){
                int typeId = virtualRelationshipIdToTypeId.get(authenticate()).get(relId);
                long startNode = virtualRelationshipIdToVirtualNodeIds.get(authenticate()).get(relId)[0];
                long endNode   = virtualRelationshipIdToVirtualNodeIds.get(authenticate()).get(relId)[0];
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
        return super.nodesGetCount() + virtualNodeIds.size();
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
            long startNode = virtualRelationshipIdToVirtualNodeIds.get(authenticate()).get(relId)[0];
            long endNode = virtualRelationshipIdToVirtualNodeIds.get(authenticate()).get(relId)[1];
            int type = virtualRelationshipIdToTypeId.get(authenticate()).get(relId);
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

        // getting virtual ids and making them to VirtualNodeItems
        Set<Long> virtualNodes = virtualNodeIds.get(authenticate());
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

        // adding the virtual ones to the mix
        Set<Long> virtualNodes = virtualNodeIds.get(authenticate());
        Map<Long,Set<Integer>> idToLabel = virtualNodeIdToLabelIds.get(authenticate());
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
        Iterator<Integer> it = virtualLabels.get(authenticate()).keySet().iterator();
        while(it.hasNext()){
            int key = it.next();
            if(virtualLabels.get(authenticate()).get(key).equals(labelName)){
                return key;
            }
        }

        return super.labelGetForName(labelName);
    }

    @Override
    public String labelGetName( int labelId ) throws LabelNotFoundKernelException
    {
        if(isVirtual(labelId)){
            if(virtualLabels.get(authenticate()).containsKey(labelId)){
                return virtualLabels.get(authenticate()).get(labelId);
            }
            throw new LabelNotFoundKernelException("No virtual Label found for id: "+labelId,new Exception());
        }

        return super.labelGetName(labelId);
    }

    @Override
    public int propertyKeyGetForName( String propertyKeyName )
    {
        Iterator<Integer> it = virtualPropertyKeyIds().iterator();
        while(it.hasNext()){
            int key = it.next();
            if(virtualPropertyKeyIdsToName.get(authenticate()).get(key).equals(propertyKeyName)){
                return key;
            }
        }
        return super.propertyKeyGetForName(propertyKeyName);
    }

    @Override
    public String propertyKeyGetName( int propertyKeyId ) throws PropertyKeyIdNotFoundKernelException
    {
        if(isVirtual(propertyKeyId)){
            if(virtualPropertyKeyIds().contains(propertyKeyId)){
                return virtualPropertyKeyIdsToName.get(authenticate()).get(propertyKeyId);
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

        for(int key: virtualPropertyKeyIds()){
            String name = virtualPropertyKeyIdsToName.get(authenticate()).get(key);
            virtualOnes.add(new Token(name,key)); // TODO: Definitely need to test this
        }
        return new MergingTokenIterator(realOnes,virtualOnes);
    }

    @Override
    public Iterator<Token> labelsGetAllTokens()
    {
        Iterator<Token> realOnes = super.labelsGetAllTokens();

        ArrayList<Token> virtualOnes = new ArrayList<>();
        for(int key:virtualLabels()){
            String name = virtualLabels.get(authenticate()).get(key);
            virtualOnes.add(new Token(name,key));
        }
        return new MergingTokenIterator(realOnes,virtualOnes);
    }

    @Override
    public Iterator<Token> relationshipTypesGetAllTokens()
    {
        Iterator<Token> realOnes = super.relationshipTypesGetAllTokens();
        ArrayList<Token> virtualOnes = new ArrayList<>();
        for(int key: virtualRelationshipTypeIdToName.get(authenticate()).keySet()){
            String name = virtualRelationshipTypeIdToName.get(authenticate()).get(key);
            virtualOnes.add(new Token(name,key));
        }
        return new MergingTokenIterator(realOnes,virtualOnes);
    }

    @Override
    public int relationshipTypeGetForName( String relationshipTypeName )
    {
        // TODO: Improvements with contains?
        Iterator<Integer> it = virtualRelationshipTypeIdToName.get(authenticate()).keySet().iterator();
        while(it.hasNext()){
            int key = it.next();
            if(virtualRelationshipTypeIdToName.get(authenticate()).get(key).equals(relationshipTypeName)){
                return key;
            }
        }

        return super.relationshipTypeGetForName(relationshipTypeName);
    }

    @Override
    public String relationshipTypeGetName( int relationshipTypeId ) throws RelationshipTypeIdNotFoundKernelException
    {
        if(isVirtual(relationshipTypeId)){
            if(virtualRelationshipTypeIdToName.get(authenticate()).keySet().contains(relationshipTypeId)){
                return virtualRelationshipTypeIdToName.get(authenticate()).get(relationshipTypeId);
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
        return super.relationshipTypeCount() + virtualRelationshipTypeIdToName.keySet().size();
    }

    // </TokenRead>

    // <TokenWrite>
    @Override
    public int virtualLabelGetOrCreateForName( String labelName ) throws IllegalTokenNameException,
            TooManyLabelsException, NoSuchMethodException
    {
        // Try getting the labelId
        // TODO: might be faster with contains?
        Iterator<Integer> it = virtualLabels.get(authenticate()).keySet().iterator();
        while(it.hasNext()){
            int key = it.next();
            if(virtualLabels.get(authenticate()).get(key).equals(labelName)){
                return key;
            }
        }

        // not found, need to create
        int newId;
        if(virtualLabels().size()==0){
            newId = -2;
        } else{
            newId = virtualLabels.get(authenticate()).firstKey()-1;
        }
        virtualLabelCreateForName(labelName,newId);
        return newId;
    }

    @Override
    public int virtualPropertyKeyGetOrCreateForName( String propertyKeyName ) throws IllegalTokenNameException
    {
        // Try getting the proplId
        // TODO: might be faster with contains?
        Iterator<Integer> it = virtualPropertyKeyIds().iterator();
        while(it.hasNext()){
            int key = it.next();
            if(virtualPropertyKeyIdsToName.get(authenticate()).get(key).equals(propertyKeyName)){
                return key;
            }
        }

        // not found, need to create
        int newId;
        if(virtualPropertyKeyIds().size()==0){
            newId = -2;
        } else{
            newId = virtualPropertyKeyIdsToName.get(authenticate()).firstKey()-1;
        }
        virtualPropertyKeyCreateForName(propertyKeyName,newId);
        return newId;
    }

    @Override
    public int virtualRelationshipTypeGetOrCreateForName( String relationshipTypeName ) throws IllegalTokenNameException
    {
        // Try getting the relId
        // TODO: might be faster with contains?
        Iterator<Integer> it = virtualRelationshipTypeIdToName.get(authenticate()).keySet().iterator();
        while(it.hasNext()){
            int key = it.next();
            if(virtualRelationshipTypeIdToName.get(authenticate()).get(key).equals(relationshipTypeName)){
                return key;
            }
        }

        // not found, need to create
        int newId;
        if(virtualRelationshipTypeIdToName.get(authenticate()).size()==0){
            newId = -2;
        } else{
            newId = virtualRelationshipTypeIdToName.get(authenticate()).firstKey()-1;
        }
        virtualRelationshipTypeCreateForName(relationshipTypeName,newId);
        return newId;
    }

    @Override
    public void virtualLabelCreateForName( String labelName, int id ) throws
            IllegalTokenNameException, TooManyLabelsException
    {
        //TODO: Token name checking missing
        virtualLabels.get(authenticate()).put(id,labelName);
    }

    @Override
    public void virtualPropertyKeyCreateForName( String propertyKeyName,
            int id ) throws
            IllegalTokenNameException
    {
        //TODO: Token name checking missing
        virtualPropertyKeyIdsToName.get(authenticate()).put(id,propertyKeyName);
    }

    @Override
    public void virtualRelationshipTypeCreateForName( String relationshipTypeName,
            int id ) throws
            IllegalTokenNameException
    {
        //TODO: Token name checking missing
        virtualRelationshipTypeIdToName.get(authenticate()).put(id,relationshipTypeName);
    }

    // </TokenWrite>

    // <DataWrite>
    @Override
    public long virtualNodeCreate()
    {
        statement.assertOpen();
        long new_id;
        if(virtualNodeIds.get(authenticate()).size()==0){
            new_id = -2;
        } else {
            long smallest = virtualNodeIds.get(authenticate()).first();
            new_id = smallest - 1;
        }
        virtualNodeIds.get(authenticate()).add(new_id);
        virtualNodeIdToPropertyKeyIds.get(authenticate()).put(new_id,new LinkedHashSet<>());
        virtualNodeIdToLabelIds.get(authenticate()).put(new_id,new LinkedHashSet<>());
        virtualNodeIdToConnectedRelationshipIds.get(authenticate()).put(new_id,new LinkedHashSet<>());

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

            // create a new relId
            long newId;
            if(virtualRelationshipIdToTypeId.get(authenticate()).size()==0) {
                newId =-2;
            } else {
                newId = virtualRelationshipIdToTypeId.get(authenticate()).firstKey() - 1;
            }
            virtualRelationshipIdToTypeId.get(authenticate()).put(newId,relationshipTypeId);

            Long[] nodes = new Long[2];
            nodes[0] = startNodeId;
            nodes[1] = endNodeId;

            virtualRelationshipIdToVirtualNodeIds.get(authenticate()).put(newId,nodes);
            virtualRelationshipIdToPropertyKeyIds.get(authenticate()).put(newId,new LinkedHashSet<Integer>());

            return newId;
        //} else {
        //    return super.relationshipCreate(relationshipTypeId, startNodeId, endNodeId);
        //}
    }

    @Override
    public void virtualNodeDelete(long nodeId) throws NoSuchMethodException, EntityNotFoundException {
        if (virtualNodeIds.get(authenticate()).contains(nodeId)) {
            //TODO: needs more checks!

            virtualNodeIds.get(authenticate()).remove(nodeId);

            virtualNodeIdToLabelIds.get(authenticate()).remove(nodeId);
            virtualNodeIdToPropertyKeyIds.get(authenticate()).remove(nodeId);

            // TODO: Remove refs that are returned from those calls

            // AND rel prop

            virtualNodeIdToConnectedRelationshipIds.get(authenticate()).remove(nodeId);
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

            if(relationshipExists(relationshipId)){
                virtualRelationshipIdToTypeId.get(authenticate()).remove(relationshipId);
                virtualRelationshipIdToPropertyKeyIds.get(authenticate()).remove(relationshipId);
                virtualRelationshipIdToVirtualNodeIds.get(authenticate()).remove(relationshipId);
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

                virtualNodeIdToLabelIds.get(authenticate()).get(nodeId).add(labelId);
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

                virtualNodeIdToLabelIds.get(authenticate()).get(nodeId).remove(labelId);
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
            if(nodeExists(nodeId)){
                // Todo: check if this propId exist?

                PropertyValueId key=null;
                Object oldValue = null;
                Iterator<PropertyValueId> it = virtualPropertyIdToValueForNodes.keySet().iterator();
                while(it.hasNext()){
                    PropertyValueId pId = it.next();
                    if(pId.getEntityId()==nodeId && pId.getPropertyKeyId()==property.propertyKeyId() &&
                            (pId.getTransactionId()==getTransactionId())){
                        // found it
                        key = pId;
                        oldValue = virtualPropertyIdToValueForNodes.get(pId);
                        break;
                    }
                }
                if(key==null){
                    // not already set
                    key = new PropertyValueId(nodeId,property.propertyKeyId(),getTransactionId());
                }

                virtualPropertyIdToValueForNodes.put(key,property.value());
                virtualNodeIdToPropertyKeyIds.get(authenticate()).get(nodeId).add(property.propertyKeyId());
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

                PropertyValueId key=null;
                Object oldValue=null;
                Iterator<PropertyValueId> it = virtualPropertyIdToValueForRels.keySet().iterator();
                while(it.hasNext()){
                    PropertyValueId pId = it.next();
                    if(pId.getEntityId()==relationshipId && pId.getPropertyKeyId()==property.propertyKeyId()&&
                            (pId.getTransactionId()==getTransactionId())){
                        // found it
                        key = pId;
                        oldValue = virtualPropertyIdToValueForRels.get(pId);
                        break;
                    }
                }
                if(key==null){
                    // not already set
                    key = new PropertyValueId(relationshipId,property.propertyKeyId(),getTransactionId());
                }

                virtualPropertyIdToValueForRels.put(key,property.value());

                Set<Integer> set = virtualRelationshipIdToPropertyKeyIds.get(authenticate()).get(relationshipId);
                if(set==null){
                    set = new TreeSet<>();
                }
                set.add(property.propertyKeyId());
                virtualRelationshipIdToPropertyKeyIds.get(authenticate()).put(relationshipId,set);

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
            if(nodeExists(nodeId)){
                Object value = getPropertyValueForNodes(nodeId,propertyKeyId);
                Property returnProp;
                if(value==null){
                    returnProp = Property.noNodeProperty(nodeId,propertyKeyId);
                    return returnProp;
                }
                returnProp = Property.property(propertyKeyId,value);
                virtualNodeIdToPropertyKeyIds.get(authenticate()).get(nodeId).remove(propertyKeyId);

                PropertyValueId p = new PropertyValueId(nodeId,propertyKeyId,getTransactionId());
                virtualPropertyIdToValueForNodes.remove(p); // TODO: Test this

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
            if(nodeExists(relationshipId)){
                Object value = getPropertyValueForRels(relationshipId,propertyKeyId);
                Property returnProp;
                if(value==null){
                    returnProp = Property.noRelationshipProperty(relationshipId,propertyKeyId);
                    return returnProp;
                }
                returnProp = Property.property(propertyKeyId,value);
                virtualRelationshipIdToPropertyKeyIds.get(authenticate()).get(relationshipId).remove(propertyKeyId);

                PropertyValueId p = new PropertyValueId(relationshipId,propertyKeyId,getTransactionId());
                virtualPropertyIdToValueForRels.remove(p); // TODO: Test this

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

    // </SchemaWrite>

    // There is no need for locking here

    // <Legacy index>

    // Not needed in proof of concept
    /*
    @Override
    public LegacyIndexHits nodeLegacyIndexGet( String indexName, String key, Object value )
            throws LegacyIndexNotFoundKernelException
    {
        return super.nodeLegacyIndexGet(indexName,key,value);
    }

    @Override
    public LegacyIndexHits nodeLegacyIndexQuery( String indexName, String key, Object queryOrQueryObject )
            throws LegacyIndexNotFoundKernelException
    {
        return super.nodeLegacyIndexQuery(indexName,key,queryOrQueryObject);
    }

    @Override
    public LegacyIndexHits nodeLegacyIndexQuery( String indexName, Object queryOrQueryObject )
            throws LegacyIndexNotFoundKernelException
    {
        return super.nodeLegacyIndexQuery(indexName,queryOrQueryObject);
    }

    @Override
    public LegacyIndexHits relationshipLegacyIndexGet( String indexName, String key, Object value,
            long startNode, long endNode ) throws LegacyIndexNotFoundKernelException
    {
        return super.relationshipLegacyIndexGet(indexName,key,value,startNode,endNode);
    }

    @Override
    public LegacyIndexHits relationshipLegacyIndexQuery( String indexName, String key, Object queryOrQueryObject,
            long startNode, long endNode ) throws LegacyIndexNotFoundKernelException
    {
        return super.relationshipLegacyIndexQuery(indexName,key,queryOrQueryObject,startNode,endNode);
    }

    @Override
    public LegacyIndexHits relationshipLegacyIndexQuery( String indexName, Object queryOrQueryObject,
            long startNode, long endNode ) throws LegacyIndexNotFoundKernelException
    {
        return super.relationshipLegacyIndexQuery(indexName,queryOrQueryObject,startNode,endNode);
    }

    @Override
    public void nodeLegacyIndexCreateLazily( String indexName, Map<String,String> customConfig )
    {
        super.nodeLegacyIndexCreateLazily(indexName,customConfig);
    }

    @Override
    public void nodeLegacyIndexCreate( String indexName, Map<String,String> customConfig )
    {
        super.nodeLegacyIndexCreate(indexName,customConfig);
    }

    @Override
    public void relationshipLegacyIndexCreateLazily( String indexName, Map<String,String> customConfig )
    {
        super.relationshipLegacyIndexCreateLazily(indexName,customConfig);
    }

    @Override
    public void relationshipLegacyIndexCreate( String indexName, Map<String,String> customConfig )

        super.relationshipLegacyIndexCreate(indexName,customConfig);
    }

    @Override
    public void nodeAddToLegacyIndex( String indexName, long node, String key, Object value )
            throws EntityNotFoundException, LegacyIndexNotFoundKernelException
    {
        super.nodeAddToLegacyIndex(indexName,node,key,value);
    }

    @Override
    public void nodeRemoveFromLegacyIndex( String indexName, long node, String key, Object value )
            throws LegacyIndexNotFoundKernelException
    {
        super.nodeRemoveFromLegacyIndex(indexName,node,key,value);
    }

    @Override
    public void nodeRemoveFromLegacyIndex( String indexName, long node, String key )
            throws LegacyIndexNotFoundKernelException
    {
        super.nodeRemoveFromLegacyIndex(indexName,node,key);
    }

    @Override
    public void nodeRemoveFromLegacyIndex( String indexName, long node ) throws LegacyIndexNotFoundKernelException
    {
        super.nodeRemoveFromLegacyIndex(indexName,node);
    }

    @Override
    public void relationshipAddToLegacyIndex( String indexName, long relationship, String key, Object value )
            throws EntityNotFoundException, LegacyIndexNotFoundKernelException
    {
        super.relationshipAddToLegacyIndex(indexName,relationship,key,value);
    }

    @Override
    public void relationshipRemoveFromLegacyIndex( String indexName, long relationship, String key, Object value )
            throws EntityNotFoundException, LegacyIndexNotFoundKernelException
    {
        super.relationshipRemoveFromLegacyIndex(indexName,relationship,key,value);
    }

    @Override
    public void relationshipRemoveFromLegacyIndex( String indexName, long relationship, String key )
            throws LegacyIndexNotFoundKernelException, EntityNotFoundException
    {
        super.relationshipRemoveFromLegacyIndex(indexName,relationship,key);
    }

    @Override
    public void relationshipRemoveFromLegacyIndex( String indexName, long relationship )
            throws LegacyIndexNotFoundKernelException, EntityNotFoundException
    {
        super.relationshipRemoveFromLegacyIndex(indexName,relationship);
    }

    @Override
    public void nodeLegacyIndexDrop( String indexName ) throws LegacyIndexNotFoundKernelException
    {
        super.nodeLegacyIndexDrop(indexName);
    }

    @Override
    public void relationshipLegacyIndexDrop( String indexName ) throws LegacyIndexNotFoundKernelException
    {
        super.relationshipLegacyIndexDrop(indexName);
    }

    @Override
    public Map<String,String> nodeLegacyIndexGetConfiguration( String indexName )
            throws LegacyIndexNotFoundKernelException
    {
        return super.nodeLegacyIndexGetConfiguration(indexName);
    }

    @Override
    public Map<String,String> relationshipLegacyIndexGetConfiguration( String indexName )
            throws LegacyIndexNotFoundKernelException
    {
        return super.relationshipLegacyIndexGetConfiguration(indexName);
    }

    @Override
    public String nodeLegacyIndexSetConfiguration( String indexName, String key, String value )
            throws LegacyIndexNotFoundKernelException
    {
        return super.nodeLegacyIndexSetConfiguration(indexName,key,value);
    }

    @Override
    public String relationshipLegacyIndexSetConfiguration( String indexName, String key, String value )
            throws LegacyIndexNotFoundKernelException
    {
        return super.relationshipLegacyIndexSetConfiguration(indexName,key,value);
    }

    @Override
    public String nodeLegacyIndexRemoveConfiguration( String indexName, String key )
            throws LegacyIndexNotFoundKernelException
    {
        return super.nodeLegacyIndexRemoveConfiguration(indexName,key);
    }

    @Override
    public String relationshipLegacyIndexRemoveConfiguration( String indexName, String key )
            throws LegacyIndexNotFoundKernelException
    {
        return super.relationshipLegacyIndexRemoveConfiguration(indexName,key);
    }

    @Override
    public String[] nodeLegacyIndexesGetAll()
    {
        return super.nodeLegacyIndexesGetAll();
    }

    @Override
    public String[] relationshipLegacyIndexesGetAll()
    {
        return super.relationshipLegacyIndexesGetAll();
    }
    */
    // </Legacy index>

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
        return virtualRelationshipIdToTypeId.get(authenticate()).keySet();
    }

    private Set<Integer> virtualPropertyKeyIds(){
        return virtualPropertyKeyIdsToName.get(authenticate()).keySet();
    }

    private Set<Integer> virtualLabels(){
        return virtualLabels.get(authenticate()).keySet();
    }

    private Object getPropertyValueForNodes(long nodeId, int propertykey){
        Iterator<PropertyValueId> it = virtualPropertyIdToValueForNodes.keySet().iterator();
        while(it.hasNext()){
            PropertyValueId pId = it.next();
            if(pId.getEntityId()==nodeId && pId.getPropertyKeyId()==propertykey){
                // success!
                return virtualPropertyIdToValueForNodes.get(pId);
            }
        }
        return null;
    }

    private Object getPropertyValueForRels(long relId, int propertykey){
        Iterator<PropertyValueId> it =virtualPropertyIdToValueForRels.keySet().iterator();
        while(it.hasNext()){
            PropertyValueId pId = it.next();
            if(pId.getEntityId()==relId && pId.getPropertyKeyId()==propertykey){
                // success!
                return virtualPropertyIdToValueForRels.get(pId);
            }
        }
        return null;
    }

    private int countVirtualNodes(int labelId){
        int count = 0;
        Iterator<Map.Entry<Long, Set<Integer>>> it = virtualNodeIdToLabelIds.get(authenticate()).entrySet().iterator();
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

        Set<Long> possibleStartNodes;
        Set<Long> possibleEndNodes;

        // prepare possibleStartNodes
        if(startLabelId==ANY_LABEL){
            possibleStartNodes = virtualNodeIds.get(authenticate());
        } else{
            // specific startLabelId
            possibleStartNodes = getVirtualNodesForLabel(startLabelId);
        }

        // prepare possibleEndNodes
        if(endLabelId==ANY_LABEL){
            possibleEndNodes = virtualNodeIds.get(authenticate());
        } else{
            // specific endLabelId
            possibleEndNodes = getVirtualNodesForLabel(endLabelId);
        }

        if((possibleStartNodes.size()==0)||(possibleEndNodes.size()==0)){
            return 0;
        }

        // do the actual counting
        Iterator<Long> relIdIterator = virtualRelationshipIdToVirtualNodeIds.get(authenticate()).keySet().iterator();
        while(relIdIterator.hasNext()){
            Long relId = relIdIterator.next();
            // filter!
            if(!relIdFilter.isUnused()){
                if(!relIdFilter.idIsInFilter(relId)){
                    continue;
                }
            }

            Long[] nodes = virtualRelationshipIdToVirtualNodeIds.get(authenticate()).get(relId);
            if((possibleStartNodes.contains(nodes[0]))&&
                    (possibleEndNodes.contains(nodes[1]))){
                if(typeId== ANY_REL){
                    // ALL
                    count++;
                } else{
                    // only typeId
                    if(virtualRelationshipIdToTypeId.get(authenticate()).get(relId)==typeId){
                        count++;
                    }
                }
            }
        }

        return count;
    }

    private Set<Long> getVirtualNodesForLabel(int labelId){
        Set<Long> returnSet = new TreeSet<>();
        Iterator<Long> nodeIdIterator = virtualNodeIdToLabelIds.get(authenticate()).keySet().iterator();
        while(nodeIdIterator.hasNext()){
            Long id = nodeIdIterator.next();
            // filter
            if(!nodeIdFilter.isUnused()){
                if(!nodeIdFilter.idIsInFilter(id)){
                    continue;
                }
            }

            if(virtualNodeIdToLabelIds.get(authenticate()).get(id).contains(labelId)){
                returnSet.add(id);
            }
        }
        return returnSet;
    }

    private int getTransactionId(){
        String txString = tx.toString();

        //"KernelTransaction[" + this.locks.getLockSessionId() + "]";
        int posStart = txString.indexOf("[");
        int posEnd = txString.indexOf("]");
        txString = txString.substring(posStart+1,posEnd);

        return Integer.parseInt(txString);
    }

    private int authenticate(){

        int taId = getTransactionId();
        if(knowntransactionIds.contains(taId)){
            // all is fine

        } else{
            // new id
            virtualRelationshipIdToTypeId.put(taId,new TreeMap<>());
            virtualNodeIds.put(taId,new TreeSet<>());
            virtualPropertyKeyIdsToName.put(taId, new TreeMap<>());
            virtualLabels.put(taId, new TreeMap<>());
            virtualRelationshipTypeIdToName.put(taId, new TreeMap<>());

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
    }

    @Override
    public Property nodeSetVirtualProperty(long nodeId, DefinedProperty property )
            throws EntityNotFoundException, ConstraintValidationKernelException, AutoIndexingKernelException, InvalidTransactionTypeKernelException
    {
        statement.assertOpen();

        if(isVirtual(nodeId)&& nodeExists(nodeId)){
            if(isVirtual(property.propertyKeyId())){
                virtualNodeIdToPropertyKeyIds.get(authenticate()).get(nodeId).add(property.propertyKeyId());

                PropertyValueId key = new PropertyValueId(nodeId,property.propertyKeyId(),getTransactionId());
                virtualPropertyIdToValueForNodes.put(key,property.value());

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
            if(isVirtual(property.propertyKeyId())){
                virtualRelationshipIdToPropertyKeyIds.get(authenticate()).get(relId).add(property.propertyKeyId());

                PropertyValueId key = new PropertyValueId(relId,property.propertyKeyId(),getTransactionId());
                virtualPropertyIdToValueForRels.put(key,property.value());

                return property;

            } else{
                throw new InvalidTransactionTypeKernelException("the property on a virtual relationship should be virtual too");
            }

        } else{
            throw new EntityNotFoundException(EntityType.RELATIONSHIP,relId);
        }
    }
}
