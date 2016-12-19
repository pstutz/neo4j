package saschapeukert;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.stubbing.Answer;
import org.neo4j.graphdb.*;
import org.neo4j.helpers.collection.Iterators;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.ReadOperations;
import org.neo4j.kernel.api.Statement;
import org.neo4j.kernel.api.constraints.PropertyConstraint;
import org.neo4j.kernel.api.exceptions.ProcedureException;
import org.neo4j.kernel.api.index.IndexDescriptor;
import org.neo4j.kernel.api.index.InternalIndexState;
import org.neo4j.kernel.api.proc.BasicContext;
import org.neo4j.kernel.api.proc.Key;
import org.neo4j.kernel.api.proc.ProcedureSignature;
import org.neo4j.kernel.builtinprocs.BuiltInDbmsProcedures;
import org.neo4j.kernel.builtinprocs.BuiltInProcedures;
import org.neo4j.kernel.builtinprocs.SpecialBuiltInProcedures;
import org.neo4j.kernel.impl.factory.Edition;
import org.neo4j.kernel.impl.proc.Procedures;
import org.neo4j.kernel.impl.proc.TypeMappers;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.storageengine.api.Token;

import java.util.*;

import static java.util.Collections.emptyIterator;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.neo4j.kernel.api.proc.Context.KERNEL_TRANSACTION;
import static org.neo4j.kernel.api.proc.Neo4jTypes.*;

/**
 * Created by Sascha Peukert on 18.12.2016.
 */
public class TestViewProcedures {

    private GraphDatabaseService db;


    private final List<IndexDescriptor> indexes = new LinkedList<>();
    private final List<IndexDescriptor> uniqueIndexes = new LinkedList<>();
    private final List<PropertyConstraint> constraints = new LinkedList<>();
    private final Map<Integer, String> labels = new HashMap<>();
    private final Map<Integer, String> propKeys = new HashMap<>();
    private final Map<Integer, String> relTypes = new HashMap<>();

    private final ReadOperations read = mock( ReadOperations.class );
    private final Statement statement = mock( Statement.class );
    private final KernelTransaction tx = mock( KernelTransaction.class );
    private final DependencyResolver resolver = mock( DependencyResolver.class );
    private final GraphDatabaseAPI graphDatabaseAPI = mock(GraphDatabaseAPI.class);

    private final Procedures procs = new Procedures();

    /*@Before
    public void setUp() throws Exception {
        //db = new TestGraphDatabaseFactory().newImpermanentDatabase();
        //Procedures proceduresService = ((GraphDatabaseAPI) db).getDependencyResolver().resolveDependency(Procedures.class);
        //proceduresService.registerProcedure(ViewProcedures.class);

    }*/

    @Before
    public void setup() throws Exception
    {
        procs.registerComponent( KernelTransaction.class, ( ctx ) -> ctx.get( KERNEL_TRANSACTION ) );
        procs.registerComponent( DependencyResolver.class, ( ctx ) -> ctx.get( DEPENDENCY_RESOLVER ) );
        procs.registerComponent( GraphDatabaseAPI.class, ( ctx ) -> ctx.get( GRAPHDATABASEAPI ) );

        procs.registerType( Node.class, new TypeMappers.SimpleConverter( NTNode, Node.class ) );
        procs.registerType( Relationship.class, new TypeMappers.SimpleConverter( NTRelationship, Relationship.class ) );
        procs.registerType( Path.class, new TypeMappers.SimpleConverter( NTPath, Path.class ) );

        new SpecialBuiltInProcedures("1.3.37", Edition.enterprise.toString() ).accept( procs );
        procs.registerProcedure( BuiltInProcedures.class );
        procs.registerProcedure( BuiltInDbmsProcedures.class );
        procs.registerProcedure(ViewProcedures.class);

        when(tx.acquireStatement()).thenReturn( statement );
        when(statement.readOperations()).thenReturn( read );

        when(read.propertyKeyGetAllTokens()).thenAnswer( asTokens(propKeys) );
        when(read.labelsGetAllTokens()).thenAnswer( asTokens(labels) );
        when(read.relationshipTypesGetAllTokens()).thenAnswer( asTokens(relTypes) );
        when(read.indexesGetAll()).thenAnswer( (i) -> indexes.iterator() );
        when(read.uniqueIndexesGetAll()).thenAnswer( (i) -> uniqueIndexes.iterator() );
        when(read.constraintsGetAll()).thenAnswer( (i) -> constraints.iterator() );
        when(read.proceduresGetAll() ).thenReturn( procs.getAllProcedures() );

        when(read.propertyKeyGetName( anyInt() ))
                .thenAnswer( (invocation) -> propKeys.get( (int)invocation.getArguments()[0] ) );
        when(read.labelGetName( anyInt() ))
                .thenAnswer( (invocation) -> labels.get( (int)invocation.getArguments()[0] ) );
        when(read.relationshipTypeGetName( anyInt() ))
                .thenAnswer( (invocation) -> relTypes.get( (int)invocation.getArguments()[0] ) );

        // Make it appear that labels are in use
        // TODO: We really should just have `labelsInUse()` on the Kernel API directly,
        //       it'd make testing much easier.
        when(read.constraintsGetForRelationshipType(anyInt())).thenReturn( emptyIterator() );
        when(read.indexesGetForLabel( anyInt() )).thenReturn( emptyIterator() );
        when(read.constraintsGetForLabel( anyInt() )).thenReturn( emptyIterator() );
        when(read.countsForNode( anyInt() )).thenReturn( 1L );
        when(read.countsForRelationship( anyInt(), anyInt(), anyInt() )).thenReturn( 1L );
        when(read.indexGetState( any( IndexDescriptor.class)  )).thenReturn( InternalIndexState.ONLINE );
    }

    @After
    public void tearDown() {
        //db.shutdown();
    }

    @Test
    public void test() throws Throwable
    {
        List<Object[]> arr = call("db.createView","Test",1);//"MATCH...",new ArrayList<String>(),new ArrayList<String>());

        Result r = db.execute("MATCH (n) RETURN n");
        System.out.println(r.resultAsString());

        for(Object[] o :arr){
            for(Object obj:o) {
                System.out.println(obj.toString());
            }
        }
    }

    private List<Object[]> call(String name, Object ... args) throws ProcedureException
    {
        BasicContext ctx = new BasicContext();
        ctx.put( KERNEL_TRANSACTION, tx );
        ctx.put( DEPENDENCY_RESOLVER, resolver );
        ctx.put( GRAPHDATABASEAPI, graphDatabaseAPI);
        when( graphDatabaseAPI.getDependencyResolver() ).thenReturn( resolver );
        when( resolver.resolveDependency( Procedures.class ) ).thenReturn( procs );
        return Iterators.asList( procs.callProcedure( ctx, ProcedureSignature.procedureName( name.split( "\\." ) ), args ) );
    }

    private Answer<Iterator<Token>> asTokens(Map<Integer,String> tokens )
    {
        return (i) -> tokens.entrySet().stream()
                .map( (entry) -> new Token(entry.getValue(), entry.getKey()))
                .iterator();
    }

    private static final Key<DependencyResolver> DEPENDENCY_RESOLVER =
            Key.key( "DependencyResolver", DependencyResolver.class );

    private static final Key<GraphDatabaseAPI> GRAPHDATABASEAPI =
            Key.key( "GraphDatabaseAPI", GraphDatabaseAPI.class );
}
