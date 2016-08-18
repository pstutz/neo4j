package org.neo4j.kernel.impl.api;

import org.junit.Before;
import org.junit.Test;
import org.neo4j.graphdb.*;
import org.neo4j.helpers.collection.Iterables;
import org.neo4j.test.TestGraphDatabaseFactory;

import java.util.Iterator;

import static junit.framework.TestCase.assertFalse;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * Created by Sascha Peukert on 18.08.2016.
 */
public class VirtualOperationsFacadeTest {
    // okay, we do only test the virtual functionality here

    private GraphDatabaseService graphDb;

    @Before
    public void setUp(){
        graphDb = new TestGraphDatabaseFactory().newImpermanentDatabase();
    }


    @Test
    public void shouldCreateVirtualNodesThatOnlyExistForThisTransaction() throws Exception
    {
        long before, after;
        try ( Transaction tx = graphDb.beginTx() )
        {
            before = Iterables.count( graphDb.getAllNodes() );
            tx.success();
        }

        try ( Transaction tx = graphDb.beginTx() )
        {
            Node n = graphDb.createVirtualNode();
            assertTrue("Id of an virtual node should be negative",n.getId()<0);
            after = Iterables.count( graphDb.getAllNodes() );
            tx.success();
        }
        assertEquals("Virtual node should appear in transaction", before+1, after );

        try ( Transaction tx = graphDb.beginTx() )
        {
            after = Iterables.count( graphDb.getAllNodes() );
            tx.success();
        }assertEquals("Virtual node should only appear in transaction it was created in", before, after );
    }

    @Test
    public void virtualPropertiesOnNodesShouldBeWorking() throws Exception
    {
        GraphDatabaseService graphDb = new TestGraphDatabaseFactory().newImpermanentDatabase();

        try ( Transaction tx = graphDb.beginTx() )
        {
            Node n = graphDb.createVirtualNode();  // HERE is a problem!
            assertTrue("Id of an virtual node should be negative",n.getId()<0);
            n.setProperty("Test",true);

            assertTrue("A set property should be obtainable",(Boolean)n.getProperty("Test"));
            ResourceIterator<String> it = graphDb.getAllPropertyKeys().iterator();
            assertEquals("Test",it.next()); // because it only should be this one
            assertFalse("There should only be one property key here",it.hasNext());

            // TODO?

            tx.success();
        }
    }

    @Test
    public void virtualLabelsShouldBeWorking() throws Exception
    {
        GraphDatabaseService graphDb = new TestGraphDatabaseFactory().newImpermanentDatabase();

        try ( Transaction tx = graphDb.beginTx() )
        {
            Node n = graphDb.createVirtualNode();  // HERE is a problem!
            assertTrue("Id of an virtual node should be negative",n.getId()<0);
            n.addLabel(Label.label("Test"));


            Iterator<Label> it =n.getLabels().iterator();
            Label l = it.next();
            assertNotNull("A set label should be obtainable",l);
            assertEquals("The set label name should match the returned one","Test",l.name());

            assertFalse("There should only be one label here",it.hasNext());

            tx.success();
        }

    }
}
