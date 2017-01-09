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
package org.neo4j.cypher;

import org.hamcrest.Matchers;
import org.junit.Test;
import org.neo4j.graphdb.*;
import org.neo4j.graphdb.spatial.CRS;
import org.neo4j.graphdb.spatial.Coordinate;
import org.neo4j.graphdb.spatial.Geometry;
import org.neo4j.graphdb.spatial.Point;
import org.neo4j.helpers.collection.Iterables;
import org.neo4j.kernel.impl.core.NodeProxy;
import org.neo4j.kernel.impl.core.RelationshipProxy;
import org.neo4j.kernel.impl.proc.Procedures;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;
import org.neo4j.test.TestGraphDatabaseFactory;
import saschapeukert.CONST;
import saschapeukert.ViewController;
import saschapeukert.ViewDefinition;

import java.util.*;
import java.util.stream.Stream;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.*;
import static org.neo4j.helpers.collection.MapUtil.map;

public class GraphDatabaseServiceExecuteTest
{
    @Test
    public void shouldExecuteCypher() throws Exception
    {
        // given
        GraphDatabaseService graphDb = new TestGraphDatabaseFactory().newImpermanentDatabase();
        long before, after;
        try ( Transaction tx = graphDb.beginTx() )
        {
            before = Iterables.count( graphDb.getAllNodes() );
            tx.success();
        }

        // when
        try ( Transaction tx = graphDb.beginTx() )
        {
            Result r = graphDb.execute( "CREATE (n:Foo{bar:\"baz\"}) RETURN n.bar, id(n)" );
            assertEquals("{id(n)=0, n.bar=baz}",r.next().toString());

            r = graphDb.execute("MATCH (n:Foo) RETURN n.bar, COUNT(n)");
            assertEquals("The created node should return if matched","{n.bar=baz, COUNT(n)=1}",
                    r.next().toString());
            after = Iterables.count( graphDb.getAllNodes() );

            tx.success();
        }

        // then
        try ( Transaction tx = graphDb.beginTx() )
        {
            after = Iterables.count( graphDb.getAllNodes() );
            tx.success();
        }
        assertEquals( before + 1, after );
    }

    @Test
    public void callCreateViewShouldWork() throws Exception{
        GraphDatabaseService graphDb = new TestGraphDatabaseFactory().newImpermanentDatabase();

        // when
        try ( Transaction tx = graphDb.beginTx() )
        {
            // Create
            Result r = graphDb.execute( "CALL db.createView('Test','MATCH (n:Person)',['n'],[])");

            assertEquals("{Message=Created view 'Test'}",r.next().toString());
            assertFalse(r.hasNext());

            // Replace
            r = graphDb.execute( "CALL db.createView('Test','MATCH (n:Person)',['n'],[])");
            assertEquals("{Message=Replaced old view definition for 'Test': (query: 'MATCH (n:Person)' , savedNodes: [n], savedRelationships: [] )}",r.next().toString());
            assertFalse(r.hasNext());

            // cleanup
            ViewController.getInstance().clearViews();

            tx.success();
        }
    }

    @Test
    public void callGetAllViewDefinitionsShouldWork() throws Exception{
        GraphDatabaseService graphDb = new TestGraphDatabaseFactory().newImpermanentDatabase();

        // when
        try ( Transaction tx = graphDb.beginTx() )
        {
            // Create
            Result r = graphDb.execute( "CALL db.getAllViewDefinitions()");

            assertFalse(r.hasNext());

            // Replace
            graphDb.execute( "CALL db.createView('Test','MATCH (n:Person)',['n'],[])");
            graphDb.execute( "CALL db.createView('Test2','MATCH (m:Person)',['m'],[])");
            r = graphDb.execute( "CALL db.getAllViewDefinitions()");

            assertEquals("{name=Test, savedRelationships=[], savedNodes=[n], query=MATCH (n:Person)}",r.next().toString());
            assertEquals("{name=Test2, savedRelationships=[], savedNodes=[m], query=MATCH (m:Person)}",r.next().toString());
            assertFalse(r.hasNext());

            // cleanup
            ViewController.getInstance().clearViews();

            tx.success();
        }
    }

    @Test
    public void callRemoveViewShouldWork() throws Exception{
        GraphDatabaseService graphDb = new TestGraphDatabaseFactory().newImpermanentDatabase();

        // when
        try ( Transaction tx = graphDb.beginTx() )
        {
            // Create
            Result r = graphDb.execute( "CALL db.getAllViewDefinitions()");
            assertFalse(r.hasNext());

            r = graphDb.execute( "CALL db.removeView('Test')");
            assertEquals("{Message=No views with name 'Test' registered.}",r.next().toString());
            assertFalse(r.hasNext());

            // Replace
            graphDb.execute( "CALL db.createView('Test','MATCH (n:Person)',['n'],[])");
            r = graphDb.execute( "CALL db.removeView('Test')");
            assertEquals("{Message=Removed view defintion for 'Test': (query: 'MATCH (n:Person)' , savedNodes: " +
                    "[n], savedRelationships: [] ).}",r.next().toString());
            assertFalse(r.hasNext());

            r = graphDb.execute( "CALL db.getAllViewDefinitions()");
            assertFalse(r.hasNext());

            // cleanup
            ViewController.getInstance().clearViews();
            tx.success();
        }
    }

    @Test
    public void viewIdQueryShouldWorkProperly() throws Exception
    {
        GraphDatabaseService graphDb = new TestGraphDatabaseFactory().newImpermanentDatabase();

        // when
        try ( Transaction tx = graphDb.beginTx() )
        {
            ViewDefinition result = new ViewDefinition();
            result.name = "Test";
            result.setQuery("MATCH (n    :Foo)-[:TEST]->(m:Foo) MATCH (n)-[a:Yo]->(m)");
            ArrayList<String> arr = new ArrayList<>();
            arr.add("n");
            arr.add("m");
            result.savedNodes = arr;
            ArrayList<String> arrRel = new ArrayList<>();
            arrRel.add("a");
            result.savedRelationships = arrRel;

            assertEquals("[Foo]",result.getLabels().toString());
            assertEquals("[Yo]",result.getRelTypes().toString());

            Result r = graphDb.execute( "CREATE (n:Foo{bar:\"baz\"}) RETURN n.bar, id(n)" );
            assertEquals("{id(n)=0, n.bar=baz}",r.next().toString());


            assertEquals("MATCH (n :Foo)-[:TEST]->(m:Foo) MATCH (n)-[a:Yo]->(m) " +
                    "RETURN collect(id(n))+ collect(id(m)) AS nodeIds , collect(id(a)) AS relIds",
                    result.getIdQuery());

            result.setQuery("MATCH (n    :Foo),  (m:Foo)");
            arrRel.clear(); // Need to do this!
            Result test = graphDb.execute(result.getIdQuery());
            Map<String,Object> map =test.next();

            assertEquals("{nodeIds=[0, 0]}",map.toString());
            Object o = map.values().iterator().next();
            List<Long> list = (List<Long>) o;
            Set<Long> set = new HashSet<>(list);

            assertEquals("[0]",set.toString());
            tx.success();
        }
    }

    @Test
    public void useViewShouldWorkProperly() throws Exception
    {
        GraphDatabaseService graphDb = new TestGraphDatabaseFactory().newImpermanentDatabase();

        // when
        try ( Transaction tx = graphDb.beginTx() )
        {
            graphDb.execute( "CREATE (n:Foo{text:'hallo'})-[:REL]->(m:Bar{text:'welt'})" );
            graphDb.execute("CALL db.createView('Test','MATCH (n:Foo)',['n'],[])");

            Result r = graphDb.execute("CALL db.useView(['Test']) MATCH (n) RETURN n");
            NodeProxy n = (NodeProxy)r.next().get("n");
            assertEquals(0,n.getId());
            assertEquals("hallo",(String)n.getProperty("text"));

            tx.success();
        }
        try ( Transaction tx = graphDb.beginTx() )
        {
            graphDb.execute("CALL db.createView('Test','MATCH (m:Bar)',['m'],[])");

            Result r = graphDb.execute("CALL db.useView(['Test']) MATCH (n) RETURN n");
            NodeProxy n = (NodeProxy)r.next().get("n");
            assertEquals(1,n.getId());
            assertEquals("welt",(String)n.getProperty("text"));

            tx.success();
        }

        try ( Transaction tx = graphDb.beginTx() )
        {
            graphDb.execute("CALL db.createView('Test','MATCH (n:Foo)-[r]->(m:Bar)',['n','m'],['r'])");

            Result r = graphDb.execute("CALL db.useView(['Test']) MATCH ()-[r]->() RETURN r");
            RelationshipProxy n = (RelationshipProxy)r.next().get("r");
            assertEquals(0,n.getId());
            assertEquals(0,n.getAllProperties().size());

            // cleanup
            ViewController.getInstance().clearViews();

            tx.success();
        }
    }

    @Test
    public void runOnViewWithNestingShouldWorkProperly() throws Exception
    {
        GraphDatabaseService graphDb = new TestGraphDatabaseFactory().newImpermanentDatabase();

        // when
        try ( Transaction tx = graphDb.beginTx() )
        {
            graphDb.execute("CREATE (n:Foo{text:'hallo'})-[:REL]->(m:Bar{text:'welt'})" );
            graphDb.execute("CALL db.createView('First','MATCH (n:Foo)-->(m:Bar)',['n','m'],[])");
            graphDb.execute("CALL db.createView('Second',\"CALL db.useView(['First']) MATCH (n:Foo) \",[\'n\'],[] )");


            Result r = graphDb.execute("CALL db.useView(['Second']) MATCH (n) RETURN n");

            NodeProxy n = (NodeProxy)r.next().get("n");
            assertEquals(0,n.getId());
            assertEquals("hallo",(String)n.getProperty("text"));

            // clean up
            ViewController.getInstance().clearViews();

            tx.success();
        }
    }


    @Test
    public void shouldExecuteCypherWithOnlyVirtualNode() throws Exception
    {
        // given
        GraphDatabaseService graphDb = new TestGraphDatabaseFactory().newImpermanentDatabase();
        final long before, after;
        try ( Transaction tx = graphDb.beginTx() )
        {
            before = Iterables.count( graphDb.getAllNodes() );
            tx.success();
        }

        // when


        try ( Transaction tx = graphDb.beginTx() )
        {
            Result r = graphDb.execute( "CREATE (n:Foo{"+ CONST.PROPERTYKEY+":\"baz\", bar:\"baz\"}) RETURN id(n)" );
            assertEquals("{id(n)=-2}",r.next().toString());

            r = graphDb.execute("MATCH (n:Foo) RETURN n.bar, COUNT(n)");
            assertEquals("The created node should return if matched","{n.bar=baz, COUNT(n)=1}",
                    r.next().toString());
            tx.success();
        }

        // then
        try ( Transaction tx = graphDb.beginTx() )
        {
            after = Iterables.count( graphDb.getAllNodes() );
            tx.success();
        }
        assertEquals( before , after );
    }

    @Test
    public void shouldExecuteCypherWithRealRelationship() throws Exception
    {
        // given
        GraphDatabaseService graphDb = new TestGraphDatabaseFactory().newImpermanentDatabase();
        long before, after;
        try ( Transaction tx = graphDb.beginTx() )
        {
            before = Iterables.count( graphDb.getAllRelationships() );
            tx.success();
        }

        // when

        try ( Transaction tx = graphDb.beginTx() )
        {
            Result r = graphDb.execute( "CREATE (n:Foo{bar:true})-[t:TEST{bar:true}]->" +
                    "(m:Bar{bar:true}) RETURN t.bar" );

            assertEquals("{t.bar=true}",r.next().toString());

            after = Iterables.count( graphDb.getAllRelationships());
            assertEquals("There should be one Relationship present",before+1,after);
            r = graphDb.execute("MATCH (:Foo)-[t]->(:Bar) RETURN COUNT(t)");
            assertEquals("The query should return 1 matching relationship","{COUNT(t)=1}",r.next().toString());
            tx.success();
        }

        // then
        try ( Transaction tx = graphDb.beginTx() )
        {
            after = Iterables.count( graphDb.getAllRelationships() );
            tx.success();
        }
        assertEquals("There should still be one relationship present after the TA endet it was created in",
                before+1 , after );
    }

    @Test
    public void shouldExecuteCypherWithOnlyVirtualRelationship() throws Exception
    {
        // given
        GraphDatabaseService graphDb = new TestGraphDatabaseFactory().newImpermanentDatabase();
        long before, after;
        try ( Transaction tx = graphDb.beginTx() )
        {
            before = Iterables.count( graphDb.getAllRelationships() );
            tx.success();
        }

        // when

        try ( Transaction tx = graphDb.beginTx() )
        {
            Result r = graphDb.execute( "CREATE (n:Foo{"+CONST.PROPERTYKEY+":\"baz\"})-[t:TEST{"+CONST.PROPERTYKEY+":\"baz\"}]->" +
                    "(m:Bar{"+CONST.PROPERTYKEY+":\"baz\"}) RETURN id(t), id(n), id(m)" );

            assertEquals("{id(n)=-2, id(m)=-3, id(t)=-2}",r.next().toString());

            after = Iterables.count( graphDb.getAllRelationships());

            assertEquals("There should be one (virtual) Relationship present",before+1,after);
            r = graphDb.execute("MATCH (:Foo)-[t:TEST]->(:Bar) RETURN COUNT(t)");
            assertEquals("The query should return 1 matching relationship","{COUNT(t)=1}",r.next().toString());
            tx.success();
        }

        // then
        try ( Transaction tx = graphDb.beginTx() )
        {
            after = Iterables.count( graphDb.getAllRelationships() );
            tx.success();
        }
        assertEquals( before , after );
    }

    @Test
    public void shouldExecuteCypherWithMixedRelationship() throws Exception
    {
        // given
        GraphDatabaseService graphDb = new TestGraphDatabaseFactory().newImpermanentDatabase();

        // when
        try ( Transaction tx = graphDb.beginTx() )
        {

            Result r = graphDb.execute( "CREATE (n:Foo{bar:\"baz\"})-[t:TEST{"+CONST.PROPERTYKEY+":\"baz\"}]->" +
                    "(m:Bar{"+CONST.PROPERTYKEY+":\"baz\"}) RETURN id(t)" );

            assertEquals("{id(t)=-2}",r.next().toString());

            r = graphDb.execute("MATCH (:Foo)-[t]->(:Bar) RETURN COUNT(t)");
            assertEquals("The query should return one matching (virtual) relationship","{COUNT(t)=1}",r.next().toString());

            r = graphDb.execute( "CREATE (n:Foo{"+CONST.PROPERTYKEY+":\"baz\"})<-[t:TEST{"+CONST.PROPERTYKEY+":\"baz\"}]-" +
                    "(m:Bar{bar:\"baz\"}) RETURN id(t)" );
            assertEquals("{id(t)=-3}",r.next().toString());

            r = graphDb.execute("MATCH (:Foo)<-[t:TEST]-(:Bar) RETURN COUNT(t)");
            assertEquals("The query should return one matching (virtual) relationship","{COUNT(t)=1}",r.next().toString());

            r = graphDb.execute("MATCH (:Foo)-[t:TEST]-(:Bar) RETURN COUNT(t)");
            assertEquals("The query should return two matching (virtual) relationship","{COUNT(t)=2}",r.next().toString());

            r = graphDb.execute( "CREATE (n:A{bar:\"baz\"})<-[t:TEST{"+CONST.PROPERTYKEY+":\"baz\"}]-" +
                    "(m:B{bar:\"baz\"}) RETURN id(t)" );
            assertEquals("{id(t)=-4}",r.next().toString());
            r = graphDb.execute("MATCH (:A)<-[t:TEST]-(:B) RETURN COUNT(t)");
            assertEquals("The query should return one matching (virtual) relationship","{COUNT(t)=1}",r.next().toString());

            tx.success();
        }

        // then
        try ( Transaction tx = graphDb.beginTx() )
        {
            assertEquals("There should be no more relationship after the previous Transaction ended",
                    0,Iterables.count( graphDb.getAllRelationships() ));
            assertEquals("There should be only four nodes after the previous Transaction ended",
                    4,Iterables.count( graphDb.getAllNodes() ));
            // should check if they are the right ones

            tx.success();
        }
    }

    @Test
    public void shouldNotPersistSpecialVirtualProperty() throws Exception
    {
        GraphDatabaseService graphDb = new TestGraphDatabaseFactory().newImpermanentDatabase();
        try ( Transaction tx = graphDb.beginTx() )
        {

            Result r = graphDb.execute( "CREATE (n:Foo{"+ CONST.PROPERTYKEY+":\"baz\"})-[t:TEST{"+ CONST.PROPERTYKEY+":\"baz\"}]->" +
                    "(m:Bar{"+ CONST.PROPERTYKEY+":\"baz\"}) RETURN n."+ CONST.PROPERTYKEY+", id(n), t."+ CONST.PROPERTYKEY+", id(t), m."+ CONST.PROPERTYKEY+", id(m)" );

            assertEquals("{id(n)=-2, id(m)=-3, id(t)=-2, m."+ CONST.PROPERTYKEY+"=null, n."+ CONST.PROPERTYKEY+"=null, " +
                            "t."+ CONST.PROPERTYKEY+"=null}",
                    r.next().toString());

            tx.success();
        }
    }

    @Test
    public void shouldNotBeAbleToCreateRelationshipFromOrToVirtualNode() throws Exception
    {
        GraphDatabaseService graphDb = new TestGraphDatabaseFactory().newImpermanentDatabase();
        try ( Transaction tx = graphDb.beginTx() )
        {
            try {
                // the following execute should produce an error
                graphDb.execute("CREATE (n:Foo{"+CONST.PROPERTYKEY+":\"baz\"})-[t:TEST]->" +
                        "(m:Bar{"+CONST.PROPERTYKEY+":\"baz\"}) RETURN id(n), id(t), id(m)");
                fail();
            } catch (QueryExecutionException e){
                // success!?
                assertEquals("Unable to load NODE with id -2.",e.getMessage());
                graphDb.execute("MATCH (n) DETACH DELETE n");

                //Result q = graphDb.execute("MATCH (n) RETURN n");
                //System.Message.println(q.resultAsString());

            }
            try {
                // the following execute should produce an error
                graphDb.execute("CREATE (n:Foo{bar:\"baz\"})-[t:TEST]->" +
                        "(m:Bar{"+CONST.PROPERTYKEY+":\"baz\"}) RETURN id(n), id(t), id(m)");
                fail();
            } catch (QueryExecutionException e){
                // success!?
                assertEquals("Unable to load NODE with id -2.",e.getMessage());
                graphDb.execute("MATCH (n) DETACH DELETE n");
            }
            try {
                // the following execute should produce an error
                graphDb.execute("CREATE (n:Foo{"+CONST.PROPERTYKEY+":\"baz\"})-[t:TEST]->" +
                        "(m:Bar{bar:\"baz\"}) RETURN id(n), id(t), id(m)");
                fail();
            } catch (QueryExecutionException e){
                // success!?
                assertEquals("Unable to load NODE with id -2.",e.getMessage());
                graphDb.execute("MATCH (n) DETACH DELETE n");
            }

            tx.failure();
        }
    }


    @Test
    public void realisingVirtualRelShouldOnlyWorkIfItIsOnlyConnectedToRealNodes(){
        GraphDatabaseService graphDb = new TestGraphDatabaseFactory().newImpermanentDatabase();
        try ( Transaction tx = graphDb.beginTx() )
        {
            graphDb.execute("CREATE (n:Foo{"+CONST.PROPERTYKEY+":\"baz\", test:true})");
            graphDb.execute("MATCH (n:Foo) CREATE (n)-[t:TEST{"+CONST.PROPERTYKEY+":\"baz\", test:true}]->(n)");

            // trying to realise a virtual relationship connected to any virtual node should fail
            try{
                graphDb.execute("MATCH (n:Foo)-[t:TEST]->(n) SET t."+CONST.PROPERTYKEY+" = false");
                fail();
            }catch (NotFoundException e){
                // it did fail! Good :-)
                assertEquals("Node[-2] is deleted or virtual and cannot be used to create a real relationship to or from it",
                        e.getMessage());
            }
            assertEquals(1,Iterables.count(graphDb.getAllNodes()));

            // change the virtual node to a real one
            Result r = graphDb.execute("MATCH (n:Foo)-[t:TEST]->(n) SET n."+CONST.PROPERTYKEY+" = false RETURN id(n), id(t)");
            assertEquals(1,Iterables.count(graphDb.getAllNodes()));
            assertEquals("{id(n)=0, id(t)=-2}",r.next().toString());

            // trying to realise a virtual relationship connected to only real nodes should  work
            r = graphDb.execute("MATCH (n:Foo)-[t:TEST]->(n) SET t."+CONST.PROPERTYKEY+" = false RETURN id(n), id(t)");
            assertEquals("{id(n)=0, id(t)=0}",r.next().toString());

            tx.failure();
        }
    }

    @Test
    public void allNodesScanShouldWorkAsIntended(){
        GraphDatabaseService graphDb = new TestGraphDatabaseFactory().newImpermanentDatabase();
        try ( Transaction tx = graphDb.beginTx() )
        {
            graphDb.execute("CREATE (n:Foo{"+CONST.PROPERTYKEY+":\"baz\", test:true})");
            graphDb.execute("MATCH (n:Foo) CREATE (n)-[t:TEST{"+CONST.PROPERTYKEY+":\"baz\", test:true}]->(n)");

            Result r = graphDb.execute("MATCH (n)-[t]-(m) RETURN t.test, id(t), id(n), id(m)");
            assertEquals("{id(n)=-2, id(m)=-2, id(t)=-2, t.test=true}",r.next().toString());

            r = graphDb.execute("MATCH (n)-[t]-(m) WHERE exists(t.test) RETURN t.test, id(t), id(n), id(m)");
            assertEquals("{id(n)=-2, id(m)=-2, id(t)=-2, t.test=true}",r.next().toString());
        }
    }

    @Test
    public void mergeShouldWorkAsIntended() throws Exception
    {
        GraphDatabaseService graphDb = new TestGraphDatabaseFactory().newImpermanentDatabase();
        try ( Transaction tx = graphDb.beginTx() )
        {
            // the following execute should produce an error
            Result r =graphDb.execute("CREATE (n:Foo{"+CONST.PROPERTYKEY+":\"baz\", test:true}) RETURN labels(n), id(n), n.test");
            assertEquals("{id(n)=-2, labels(n)=[Foo], n.test=true}",r.next().toString());

            r = graphDb.execute("MERGE (n:Foo) ON MATCH SET n."+CONST.PROPERTYKEY+"=false RETURN labels(n), id(n), n.test");
            assertEquals("{id(n)=0, labels(n)=[Foo], n.test=true}",r.next().toString());

            assertEquals(1,Iterables.count(graphDb.getAllNodes()));

            r= graphDb.execute("MATCH (n:Foo) CREATE (n)-[t:TEST{"+CONST.PROPERTYKEY+":\"baz\", test:true}]->(n) RETURN id(n), id(t)");
            assertEquals("{id(n)=0, id(t)=-2}",r.next().toString());
            r = graphDb.execute("MERGE (n:Foo)-[t:TEST]->(n) ON MATCH SET t."+CONST.PROPERTYKEY+"=false RETURN type(t), id(t), t.test");
            assertEquals("{id(t)=0, type(t)=TEST, t.test=true}",r.next().toString());

            //TODO check SET

            tx.success();
        }
    }

    @Test
    public void complexExample(){

        GraphDatabaseService graphDb = new TestGraphDatabaseFactory().newImpermanentDatabase();
        try ( Transaction tx = graphDb.beginTx() ) {
            // setup
            Result r = graphDb.execute("CREATE ( : Person {name:'Tim', sex : 'm' }) –[a:married{since:'01.01.1970'}]–> " +
                    "( : Person {name:'Tina', sex : 'f' })" +
                    " CREATE ( : Person {name:'Bob', sex : 'm' }) –[b:married{since:'01.02.2016'}]–> " +
                    "( : Person {name:'Heidi', sex : 'f' })" +
                    " CREATE ( : Person {name:'Peter', sex : 'm' }) –[c:married{since:'10.10.2011'}]–> " +
                    "( : Person {name:'Franzi', sex : 'f' }) RETURN id(a), id(b), id(c)");

            assertEquals("{id(a)=0, id(c)=2, id(b)=1}",r.next().toString());

            assertEquals(6,Iterables.count(graphDb.getAllNodes()));

            r = graphDb.execute("MATCH (h : Person { sex : 'm' }) –[ma : married]– (w : Person { sex : 'f' }) " +
                    "WITH h, w, ma.since AS d " +
                    "CREATE (h) –[hu : husband{"+CONST.PROPERTYKEY+":\"baz\"}]-> (m : Marriage {"+CONST.PROPERTYKEY+":\"baz\", since : d } ) " +
                    "<-[wi : wife{"+CONST.PROPERTYKEY+":\"baz\"}]- (w) " +
                    "RETURN h, hu, m, wi, w");
            assertEquals("{wi=(1)-[wife,-3]->(-2), m=Node[-2], h=Node[0], w=Node[1], hu=(0)-[husband,-2]->(-2)}",
                    r.next().toString());
            assertEquals("{wi=(3)-[wife,-5]->(-3), m=Node[-3], h=Node[2], w=Node[3], hu=(2)-[husband,-4]->(-3)}",
                    r.next().toString());
            assertEquals("{wi=(5)-[wife,-7]->(-4), m=Node[-4], h=Node[4], w=Node[5], hu=(4)-[husband,-6]->(-4)}",
                    r.next().toString());
            assertFalse(r.hasNext());
            tx.success();
        }
    }

    @Test
    public void complexExample2(){

        GraphDatabaseService graphDb = new TestGraphDatabaseFactory().newImpermanentDatabase();
        try ( Transaction tx = graphDb.beginTx() ) {
            // setup
            Result r = graphDb.execute("CREATE ( : Person {name:'Tim', sex : 'm' }) –[a:married{since:'01.01.1970'}]–> " +
                    "( : Person {name:'Tina', sex : 'f' })" +
                    " CREATE ( : Person {name:'Bob', sex : 'm' }) –[b:married{since:'01.02.2016'}]–> " +
                    "( : Person {name:'Heidi', sex : 'f' })" +
                    " CREATE ( : Person {name:'Peter', sex : 'm' }) –[c:married{since:'10.10.2011'}]–> " +
                    "( : Person {name:'Franzi', sex : 'f' }) RETURN id(a), id(b), id(c)");

            assertEquals("{id(a)=0, id(c)=2, id(b)=1}",r.next().toString());

            assertEquals(6,Iterables.count(graphDb.getAllNodes()));

            r = graphDb.execute("MATCH (h : Person { sex : 'm' }) –[ma : married]– (w : Person { sex : 'f' }) " +
                    "WITH h, w, ma.since AS d " +
                    "CREATE VIRTUAL (h) –[hu : husband]-> (m : Marriage { since : d } ) " +
                    "<-[wi : wife]- (w) " +
                    "RETURN h, hu, m, wi, w");
            assertEquals("{wi=(1)-[wife,-3]->(-2), m=Node[-2], h=Node[0], w=Node[1], hu=(0)-[husband,-2]->(-2)}",
                    r.next().toString());
            assertEquals("{wi=(3)-[wife,-5]->(-3), m=Node[-3], h=Node[2], w=Node[3], hu=(2)-[husband,-4]->(-3)}",
                    r.next().toString());
            assertEquals("{wi=(5)-[wife,-7]->(-4), m=Node[-4], h=Node[4], w=Node[5], hu=(4)-[husband,-6]->(-4)}",
                    r.next().toString());
            assertFalse(r.hasNext());
            tx.success();
        }
    }

    @Test
    public void matchEveryConnectedNodeShouldWork(){
        GraphDatabaseService graphDb = new TestGraphDatabaseFactory().newImpermanentDatabase();
        try ( Transaction tx = graphDb.beginTx() ) {
            // setup
            Result r = graphDb.execute("CREATE ( : Person {name:'Tim', sex : 'm' }) –[a:married{since:'01.01.1970'}]–> " +
                    "( : Person {name:'Tina', sex : 'f' })" +
                    " CREATE ( : Person {name:'Bob', sex : 'm' }) –[b:married{since:'01.02.2016'}]–> " +
                    "( : Person {name:'Heidi', sex : 'f' })" +
                    " CREATE ( : Person {name:'Peter', sex : 'm' }) –[c:married{since:'10.10.2011'}]–> " +
                    "( : Person {name:'Franzi', sex : 'f' }) RETURN id(a), id(b), id(c)");

            assertEquals("{id(a)=0, id(c)=2, id(b)=1}", r.next().toString());

            r = graphDb.execute("MATCH (n)-[]-() RETURN n");
            assertEquals("{n=Node[0]}",r.next().toString());
            assertEquals("{n=Node[1]}",r.next().toString());
            assertEquals("{n=Node[2]}",r.next().toString());
            assertEquals("{n=Node[3]}",r.next().toString());
            assertEquals("{n=Node[4]}",r.next().toString());
            assertEquals("{n=Node[5]}",r.next().toString());
            assertFalse(r.hasNext());
            tx.success();
        }

    }

    @Test
    public void showcaseTest(){
        GraphDatabaseService graphDb = new TestGraphDatabaseFactory().newImpermanentDatabase();
        try ( Transaction tx = graphDb.beginTx() ) {
            // setup
            graphDb.execute("MATCH (p:Person)-[:TWEETED]->(:Tweet{content:\"Hi!\"}) " +
                    "RETURN p");

            tx.success();
        }
    }

    @Test
    public void shouldNotReturnInternalGeographicPointType() throws Exception
    {
        // given
        GraphDatabaseService graphDb = new TestGraphDatabaseFactory().newImpermanentDatabase();

        // when
        Result execute = graphDb.execute( "RETURN point({longitude: 144.317718, latitude: -37.031738}) AS p" );

        // then
        Object obj = execute.next().get( "p" );
        assertThat( obj, Matchers.instanceOf(Point.class));

        Point point = (Point) obj;
        assertThat( point.getCoordinate(), equalTo(new Coordinate( 144.317718, -37.031738 )));

        CRS crs = point.getCRS();
        assertThat( crs.getCode(), equalTo(4326));
        assertThat( crs.getType(), equalTo("WGS-84"));
        assertThat( crs.getHref(), equalTo("http://spatialreference.org/ref/epsg/4326/"));
    }

    @Test
    public void shouldNotReturnInternalCartesianPointType() throws Exception
    {
        // given
        GraphDatabaseService graphDb = new TestGraphDatabaseFactory().newImpermanentDatabase();

        // when
        Result execute = graphDb.execute( "RETURN point({x: 13.37, y: 13.37, crs:'cartesian'}) AS p" );

        // then
        Object obj = execute.next().get( "p" );
        assertThat( obj, Matchers.instanceOf(Point.class));

        Point point = (Point) obj;
        assertThat( point.getCoordinate(), equalTo(new Coordinate( 13.37, 13.37 )));

        CRS crs = point.getCRS();
        assertThat( crs.getCode(), equalTo(7203));
        assertThat( crs.getType(), equalTo("cartesian"));
        assertThat( crs.getHref(), equalTo("http://spatialreference.org/ref/sr-org/7203/"));
    }

    @SuppressWarnings( "unchecked" )
    @Test
    public void shouldNotReturnInternalPointWhenInArray() throws Exception
    {
        // given
        GraphDatabaseService graphDb = new TestGraphDatabaseFactory().newImpermanentDatabase();

        // when
        Result execute = graphDb.execute( "RETURN [point({longitude: 144.317718, latitude: -37.031738})] AS ps" );

        // then
        List<Point> points = (List<Point>)execute.next().get( "ps" );
        assertThat( points.get(0), Matchers.instanceOf(Point.class));
    }

    @SuppressWarnings( "unchecked" )
    @Test
    public void shouldNotReturnInternalPointWhenInMap() throws Exception
    {
        // given
        GraphDatabaseService graphDb = new TestGraphDatabaseFactory().newImpermanentDatabase();

        // when
        Result execute = graphDb.execute( "RETURN {p: point({longitude: 144.317718, latitude: -37.031738})} AS m" );

        // then
        Map<String,Object> points = (Map<String, Object>)execute.next().get( "m" );
        assertThat( points.get("p"), Matchers.instanceOf(Point.class));
    }

    @Test
    public void shouldBeAbleToUseResultingPointFromOneQueryAsParameterToNext() throws Exception
    {
        // given a point create by one cypher query
        GraphDatabaseService graphDb = new TestGraphDatabaseFactory().newImpermanentDatabase();
        Result execute = graphDb.execute( "RETURN point({longitude: 144.317718, latitude: -37.031738}) AS p" );
        Point point = (Point) execute.next().get( "p" );

        // when passing as params to a distance function
        Result result = graphDb.execute(
                "RETURN distance(point({longitude: 144.317718, latitude: -37.031738}),{previous}) AS dist",
                map( "previous", point ) );

        // then
        Double dist = (Double) result.next().get( "dist" );
        assertThat( dist, equalTo( 0.0 ) );
    }

    @Test
    public void shouldBeAbleToUseExternalPointAsParameterToQuery() throws Exception
    {
        // given a point created from public interface
        GraphDatabaseService graphDb = new TestGraphDatabaseFactory().newImpermanentDatabase();
        Point point = makeFakePoint( 144.317718, -37.031738, makeWGS84() );

        // when passing as params to a distance function
        Result result = graphDb.execute(
                "RETURN distance(point({longitude: 144.317718, latitude: -37.031738}),{previous}) AS dist",
                map( "previous", point ) );

        // then
        Double dist = (Double) result.next().get( "dist" );
        assertThat( dist, equalTo( 0.0 ) );
    }

    @Test
    public void shouldBeAbleToUseExternalGeometryAsParameterToQuery() throws Exception
    {
        // given a point created from public interface
        GraphDatabaseService graphDb = new TestGraphDatabaseFactory().newImpermanentDatabase();
        Geometry geometry = makeFakePointAsGeometry( 144.317718, -37.031738, makeWGS84() );

        // when passing as params to a distance function
        Result result = graphDb.execute(
                "RETURN distance(point({longitude: 144.317718, latitude: -37.031738}),{previous}) AS dist",
                map( "previous", geometry ) );

        // then
        Double dist = (Double) result.next().get( "dist" );
        assertThat( dist, equalTo( 0.0 ) );
    }

    @Test
    public void shouldBeAbleToUseExternalPointArrayAsParameterToQuery() throws Exception
    {
        // given a point created from public interface
        GraphDatabaseService graphDb = new TestGraphDatabaseFactory().newImpermanentDatabase();
        Point point = makeFakePoint( 144.317718, -37.031738, makeWGS84() );
        Point[] points = new Point[]{point, point};

        // when passing as params to a distance function
        Result result = graphDb.execute(
                "RETURN distance({points}[0],{points}[1]) AS dist",
                map( "points", points ) );

        // then
        Double dist = (Double) result.next().get( "dist" );
        assertThat( dist, equalTo( 0.0 ) );
    }

    @Test
    public void shouldBeAbleToUseResultsOfPointProcedureAsInputToDistanceFunction() throws Exception
    {
        // given procedure that produces a point
        GraphDatabaseService graphDb = new TestGraphDatabaseFactory().newImpermanentDatabase();
        Procedures procedures =
                ((GraphDatabaseAPI) graphDb).getDependencyResolver().resolveDependency( Procedures.class );
        procedures.registerProcedure( PointProcs.class );

        // when calling procedure that produces a point
        Result result = graphDb.execute(
                "CALL spatial.point(144.317718, -37.031738) YIELD point " +
                "RETURN distance(point({longitude: 144.317718, latitude: -37.031738}), point) AS dist" );

        // then
        Double dist = (Double) result.next().get( "dist" );
        assertThat( dist, equalTo( 0.0 ) );

    }

    @Test
    public void shouldBeAbleToUseResultsOfPointGeometryProcedureAsInputToDistanceFunction() throws Exception
    {
        // given procedure that produces a point
        GraphDatabaseService graphDb = new TestGraphDatabaseFactory().newImpermanentDatabase();
        Procedures procedures =
                ((GraphDatabaseAPI) graphDb).getDependencyResolver().resolveDependency( Procedures.class );
        procedures.registerProcedure( PointProcs.class );

        // when calling procedure that produces a point
        Result result = graphDb.execute(
                "CALL spatial.pointGeometry(144.317718, -37.031738) YIELD geometry " +
                "RETURN distance(point({longitude: 144.317718, latitude: -37.031738}), geometry) AS dist" );

        // then
        Double dist = (Double) result.next().get( "dist" );
        assertThat( dist, equalTo( 0.0 ) );

    }

    private static Point makeFakePoint(double x, double y, final CRS crs)
    {
        final Coordinate coord = new Coordinate( x, y );
        return new Point() {

            @Override
            public String getGeometryType()
            {
                return "Point";
            }

            @Override
            public List<Coordinate> getCoordinates()
            {
                return Arrays.asList( new Coordinate[]{coord} );
            }

            @Override
            public CRS getCRS()
            {
                return crs;
            }
        };
    }

    private static Geometry makeFakePointAsGeometry(double x, double y, final CRS crs)
    {
        final Coordinate coord = new Coordinate( x, y );
        return new Geometry() {

            @Override
            public String getGeometryType()
            {
                return "Point";
            }

            @Override
            public List<Coordinate> getCoordinates()
            {
                return Arrays.asList( new Coordinate[]{coord} );
            }

            @Override
            public CRS getCRS()
            {
                return crs;
            }
        };
    }

    private static CRS makeWGS84()
    {
        // "WGS-84", 4326, "http://spatialreference.org/ref/epsg/4326/"
        return new CRS() {
            @Override
            public int getCode()
            {
                return 4326;
            }

            @Override
            public String getType()
            {
                return "WGS-84";
            }

            @Override
            public String getHref()
            {
                return "http://spatialreference.org/ref/epsg/4326/";
            }
        };
    }

    public static class PointProcs
    {
        @Procedure( "spatial.point" )
        public Stream<PointResult> spatialPoint( @Name( "longitude" ) double longitude, @Name( "latitude" ) double latitude )
        {
            Point point = makeFakePoint( longitude, latitude, makeWGS84() );
            return Stream.of( new PointResult(point) );
        }
        @Procedure( "spatial.pointGeometry" )
        public Stream<GeometryResult> spatialPointGeometry( @Name( "longitude" ) double longitude, @Name( "latitude" ) double latitude )
        {
            Geometry geometry = makeFakePointAsGeometry( longitude, latitude, makeWGS84() );
            return Stream.of( new GeometryResult(geometry) );
        }
    }

    public static class PointResult
    {
        public Point point;

        public PointResult( Point point )
        {
            this.point = point;
        }
    }

    public static class GeometryResult
    {
        public Geometry geometry;

        public GeometryResult( Geometry geometry )
        {
            this.geometry = geometry;
        }
    }
}
