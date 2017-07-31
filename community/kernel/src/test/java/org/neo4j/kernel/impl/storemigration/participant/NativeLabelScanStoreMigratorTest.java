/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.kernel.impl.storemigration.participant;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Paths;

import org.neo4j.collection.primitive.PrimitiveLongCollections;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Transaction;
import org.neo4j.index.internal.gbptree.RecoveryCleanupWorkCollector;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.fs.StoreChannel;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.impl.api.scan.FullStoreChangeStream;
import org.neo4j.kernel.impl.index.labelscan.NativeLabelScanStore;
import org.neo4j.kernel.impl.store.MetaDataStore;
import org.neo4j.kernel.impl.store.format.standard.StandardV2_3;
import org.neo4j.kernel.impl.store.format.standard.StandardV3_2;
import org.neo4j.kernel.impl.storemigration.monitoring.MigrationProgressMonitor;
import org.neo4j.kernel.lifecycle.Lifespan;
import org.neo4j.kernel.monitoring.Monitors;
import org.neo4j.test.TestGraphDatabaseFactory;
import org.neo4j.test.rule.PageCacheRule;
import org.neo4j.test.rule.TestDirectory;
import org.neo4j.test.rule.fs.DefaultFileSystemRule;

import static java.lang.String.format;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class NativeLabelScanStoreMigratorTest
{
    private final TestDirectory testDirectory = TestDirectory.testDirectory();
    private final DefaultFileSystemRule fileSystemRule = new DefaultFileSystemRule();
    private final PageCacheRule pageCacheRule = new PageCacheRule();

    @Rule
    public RuleChain ruleChain = RuleChain.outerRule( testDirectory ).around( fileSystemRule ).around( pageCacheRule );

    private File storeDir;
    private File nativeLabelIndex;
    private File migrationDir;
    private File luceneLabelScanStore;

    private final MigrationProgressMonitor.Section progressMonitor = mock( MigrationProgressMonitor.Section.class );

    private FileSystemAbstraction fileSystem;
    private PageCache pageCache;
    private NativeLabelScanStoreMigrator indexMigrator;

    @Before
    public void setUp() throws Exception
    {
        storeDir = testDirectory.directory( "store" );
        nativeLabelIndex = new File( storeDir, NativeLabelScanStore.FILE_NAME );
        migrationDir = testDirectory.directory( "migrationDir" );
        luceneLabelScanStore = storeDir.toPath().resolve( Paths.get( "schema", "label", "lucene" ) ).toFile();

        fileSystem = fileSystemRule.get();
        pageCache = pageCacheRule.getPageCache( fileSystemRule );
        indexMigrator = new NativeLabelScanStoreMigrator( fileSystem, pageCache );
        fileSystem.mkdirs( luceneLabelScanStore );
    }

    @Test
    public void skipMigrationIfNativeIndexExist() throws Exception
    {
        ByteBuffer sourceBuffer = writeNativeIndexFile( nativeLabelIndex, new byte[]{1, 2, 3} );

        indexMigrator.migrate( storeDir, migrationDir, progressMonitor, StandardV3_2.STORE_VERSION, StandardV3_2.STORE_VERSION );
        indexMigrator.moveMigratedFiles( migrationDir, storeDir, StandardV3_2.STORE_VERSION, StandardV3_2.STORE_VERSION );

        ByteBuffer resultBuffer = readFileContent( nativeLabelIndex, 3 );
        assertEquals( sourceBuffer, resultBuffer );
        assertTrue( fileSystem.fileExists( luceneLabelScanStore ) );
    }

    @Test
    public void luceneLabelIndexRemovedAfterSuccessfulMigration() throws IOException
    {
        prepareEmpty23Database();

        indexMigrator.migrate( storeDir, migrationDir, progressMonitor, StandardV2_3.STORE_VERSION, StandardV3_2.STORE_VERSION );
        indexMigrator.moveMigratedFiles( migrationDir, storeDir, StandardV2_3.STORE_VERSION, StandardV3_2.STORE_VERSION );

        assertFalse( fileSystem.fileExists( luceneLabelScanStore ) );
    }

    @Test
    public void moveCreatedNativeLabelIndexBackToStoreDirectory() throws IOException
    {
        prepareEmpty23Database();
        indexMigrator.migrate( storeDir, migrationDir, progressMonitor, StandardV2_3.STORE_VERSION, StandardV3_2.STORE_VERSION );
        File migrationNativeIndex = new File( migrationDir, NativeLabelScanStore.FILE_NAME );
        ByteBuffer migratedFileContent = writeNativeIndexFile( migrationNativeIndex, new byte[]{5, 4, 3, 2, 1} );

        indexMigrator.moveMigratedFiles( migrationDir, storeDir, StandardV2_3.STORE_VERSION, StandardV3_2.STORE_VERSION );

        ByteBuffer movedNativeIndex = readFileContent( nativeLabelIndex, 5 );
        assertEquals( migratedFileContent, movedNativeIndex );
    }

    @Test
    public void populateNativeLabelScanIndexDuringMigration() throws IOException
    {
        prepare32DatabaseWithNodes();
        indexMigrator.migrate( storeDir, migrationDir, progressMonitor, StandardV3_2.STORE_VERSION, StandardV3_2.STORE_VERSION );
        indexMigrator.moveMigratedFiles( migrationDir, storeDir, StandardV2_3.STORE_VERSION, StandardV3_2.STORE_VERSION );

        try ( Lifespan lifespan = new Lifespan() )
        {
            NativeLabelScanStore labelScanStore =
                    new NativeLabelScanStore( pageCache, storeDir, FullStoreChangeStream.EMPTY, true, new Monitors(),
                            RecoveryCleanupWorkCollector.NULL );
            lifespan.add( labelScanStore );
            for ( int labelId = 0; labelId < 10; labelId++ )
            {
                int nodeCount = PrimitiveLongCollections.count( labelScanStore.newReader().nodesWithLabel( labelId ) );
                assertEquals( format( "Expected to see only one node for label %d but was %d.", labelId, nodeCount ),
                        1, nodeCount );
            }
        }
    }

    @Test
    public void reportProgressOnNativeIndexPopulation() throws IOException
    {
        prepare32DatabaseWithNodes();
        indexMigrator.migrate( storeDir, migrationDir, progressMonitor, StandardV3_2.STORE_VERSION, StandardV3_2.STORE_VERSION );
        indexMigrator.moveMigratedFiles( migrationDir, storeDir, StandardV2_3.STORE_VERSION, StandardV3_2.STORE_VERSION );

        verify( progressMonitor ).start( 10 );
        verify( progressMonitor, times( 10 ) ).progress( 1 );
    }

    private ByteBuffer writeNativeIndexFile( File file, byte[] content ) throws IOException
    {
        ByteBuffer sourceBuffer = ByteBuffer.wrap( content );
        storeFileContent( file, sourceBuffer );
        sourceBuffer.flip();
        return sourceBuffer;
    }

    private void prepare32DatabaseWithNodes()
    {
        GraphDatabaseService embeddedDatabase = new TestGraphDatabaseFactory().newEmbeddedDatabase( storeDir );
        try
        {
            try ( Transaction transaction = embeddedDatabase.beginTx() )
            {
                for ( int i = 0; i < 10; i++ )
                {
                    embeddedDatabase.createNode( Label.label( "label" + i ) );
                }
                transaction.success();
            }
        }
        finally
        {
            embeddedDatabase.shutdown();
        }
        fileSystem.deleteFile( nativeLabelIndex );
    }

    private void prepareEmpty23Database() throws IOException
    {
        new TestGraphDatabaseFactory().newEmbeddedDatabase( storeDir ).shutdown();
        fileSystem.deleteFile( nativeLabelIndex );
        MetaDataStore.setRecord( pageCache, new File( storeDir, MetaDataStore.DEFAULT_NAME ), MetaDataStore.Position
                .STORE_VERSION, MetaDataStore.versionStringToLong( StandardV2_3.STORE_VERSION ) );
    }

    private ByteBuffer readFileContent( File nativeLabelIndex, int length ) throws IOException
    {
        try ( StoreChannel storeChannel = fileSystem.open( nativeLabelIndex, "r" ) )
        {
            ByteBuffer readBuffer = ByteBuffer.allocate( length );
            //noinspection StatementWithEmptyBody
            while ( readBuffer.hasRemaining() && storeChannel.read( readBuffer ) > 0 )
            {
                // read till the end of store channel
            }
            readBuffer.flip();
            return readBuffer;
        }
    }

    private void storeFileContent( File nativeLabelIndex, ByteBuffer sourceBuffer ) throws IOException
    {
        try ( StoreChannel storeChannel = fileSystem.create( nativeLabelIndex ) )
        {
            storeChannel.write( sourceBuffer );
        }
    }
}
