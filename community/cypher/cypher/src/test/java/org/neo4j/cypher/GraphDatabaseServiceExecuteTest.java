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

import org.junit.Test;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
import org.neo4j.helpers.collection.Iterables;
import org.neo4j.test.TestGraphDatabaseFactory;

import static org.junit.Assert.assertEquals;

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
            Result r = graphDb.execute( "CREATE (n:Foo{virtual:\"baz\"}) RETURN n.virtual" );
            assertEquals("{n.virtual=baz}",r.next().toString());

            r = graphDb.execute("MATCH (n:Foo) RETURN n.virtual, COUNT(n)");
            assertEquals("The created node should return if matched","{n.virtual=baz, COUNT(n)=1}",
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
            Result r = graphDb.execute( "CREATE (n:Foo{virtual:\"baz\"})-[t:TEST{virtual:\"baz\"}]->" +
                    "(m:Bar{virtual:\"baz\"}) RETURN t.virtual" );

            assertEquals("{t.virtual=baz}",r.next().toString());

            after = Iterables.count( graphDb.getAllRelationships());

            assertEquals("There should be one (virtual) Relationship present",before+1,after);
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

            Result r = graphDb.execute( "CREATE (n:Foo{bar:\"baz\"})-[t:TEST{virtual:\"baz\"}]->" +
                    "(m:Bar{virtual:\"baz\"}) RETURN t.virtual, id(t)" );

            assertEquals("{id(t)=-1, t.virtual=baz}",r.next().toString());

            r = graphDb.execute("MATCH (:Foo)-[t]->(:Bar) RETURN COUNT(t)");
            assertEquals("The query should return one matching (virtual) relationship","{COUNT(t)=1}",r.next().toString());

            r = graphDb.execute( "CREATE (n:Foo{virtual:\"baz\"})<-[t:TEST{virtual:\"baz\"}]-" +
                    "(m:Bar{bar:\"baz\"}) RETURN t.virtual, id(t)" );
            assertEquals("{id(t)=-2, t.virtual=baz}",r.next().toString());

            r = graphDb.execute("MATCH (:Foo)<-[t:TEST]-(:Bar) RETURN COUNT(t)");
            assertEquals("The query should return one matching (virtual) relationship","{COUNT(t)=1}",r.next().toString());

            r = graphDb.execute("MATCH (:Foo)-[t:TEST]-(:Bar) RETURN COUNT(t)");
            assertEquals("The query should return two matching (virtual) relationship","{COUNT(t)=2}",r.next().toString());

            //TODO: Remove virtual prop

            r = graphDb.execute( "CREATE (n:A{bar:\"baz\"})<-[t:TEST{virtual:\"baz\"}]-" +
                    "(m:B{bar:\"baz\"}) RETURN t.virtual, id(t)" );
            assertEquals("{id(t)=-3, t.virtual=baz}",r.next().toString());
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
}
