package org.fbc.neo4j.tt;

import org.joda.time.Interval;
import org.joda.time.ReadableInterval;
import org.neo4j.graphdb.*;
import org.neo4j.graphdb.traversal.Evaluators;
import org.neo4j.graphdb.traversal.InitialBranchState;
import org.neo4j.graphdb.traversal.TraversalBranch;
import org.neo4j.graphdb.traversal.TraversalDescription;
import org.neo4j.tooling.GlobalGraphOperations;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import java.util.*;

@Path("/")
public class TimeTunnelsExtension {
    public static Logger log = LoggerFactory.getLogger("org.fbc");

    public static String FROM_PROPERTY_NAME = "dateFrom";
    public static String TO_PROPERTY_NAME = "dateTo";
    public static String DATE_PATTERN = "yyyy-MM-dd HH:mm:ss.SSSS";
    public static String IDENTITY_PROPERTY = "symbol";
    public static Label MS_LABEL = DynamicLabel.label("MS");

    public enum RelTypes implements RelationshipType {
        HAS_MANAGER,
        HAS_WORKER,
        SUPERIOR,
    }

    @Context
    GraphDatabaseService graphDatabaseService;

    private static TraversalDescription timeTunnelTraversal = null;

    @GET
    @Path("/version")
    public String version() {
        return "0.02.04-SNAPSHOT";
    }

    @GET
    @Path("/warmup")
    public String warmUp() {
        Node start;
        try (Transaction tx = graphDatabaseService.beginTx()) {
            GlobalGraphOperations ggo = GlobalGraphOperations.at(graphDatabaseService);

            Iterable<Node> nodes = ggo.getAllNodes();
            for (Node n : nodes) {
                n.getPropertyKeys();
                for (Relationship relationship : n.getRelationships()) {
                    start = relationship.getStartNode();
                }
            }

            Iterable<Relationship> relations = ggo.getAllRelationships();
            for (Relationship r : relations) {
                r.getPropertyKeys();
                start = r.getStartNode();
            }
        }
        return "Warmed up and ready to go!";
    }

    @GET
    @Path("fromMs/{symbol}")
    @Produces(MediaType.APPLICATION_JSON)
    public Collection<Map<String, Object>> findPathsWithTimeTunnelFromMs(
            @PathParam("symbol") String symbol
    ) {
        log.info(String.format("findPathsWithTimeTunnelFromMs symbol=%s", symbol));
        Collection<Map<String, Object>> result = new ArrayList<>();
        long startTime = System.currentTimeMillis();
        try (Transaction tx = graphDatabaseService.beginTx()) {
            getTraversalDescription();

            log.debug(timeTunnelTraversal.toString());
            // determine starting node
            ResourceIterable<Node> startingNodes;
            startingNodes = graphDatabaseService.findNodesByLabelAndProperty(
                    MS_LABEL, IDENTITY_PROPERTY, symbol
            );
            long startingNodesTime = System.currentTimeMillis();
            log.info("after find starting nodes in " + (startingNodesTime - startTime));
            ResourceIterable<org.neo4j.graphdb.Path> paths = timeTunnelTraversal.traverse(startingNodes);
            long afterTraverseTime = System.currentTimeMillis();
            log.info(String.format("after traverse in %d", afterTraverseTime - startingNodesTime));
            // consume traversal and build return data structure
            for (org.neo4j.graphdb.Path p : paths) {
                TraversalBranch path = (TraversalBranch) p; // need to cast here to get access to state variable

                Map<String, Object> map = new HashMap<>();
                map.put("labels", getLabels(path.endNode()));
                map.put(IDENTITY_PROPERTY, path.endNode().getProperty(IDENTITY_PROPERTY, null));

                ReadableInterval interval = (ReadableInterval) path.state();
                map.put(FROM_PROPERTY_NAME, interval.getStart().toDateTime().toString(DATE_PATTERN));
                map.put(TO_PROPERTY_NAME, interval.getEnd().toDateTime().toString(DATE_PATTERN));

                result.add(map);

                //log.info(org.neo4j.graphdb.traversal.Paths.defaultPathToString(path));
            }
            long endTime = System.currentTimeMillis();

            log.info(String.format("after iterate through paths in %d", endTime - afterTraverseTime));

            log.info(String.format("finished in %d", endTime - startTime));

            return result;
        }
    }

    private TraversalDescription getTraversalDescription() {
        if (timeTunnelTraversal == null) {
            // defining path expander
            TimeTunnelPathExpander timeTunnelPathExpander = new TimeTunnelPathExpander(FROM_PROPERTY_NAME, TO_PROPERTY_NAME, DATE_PATTERN);
            log.info("step 1");
            // should traverse: match (ms:MS)<-[r0:HAS_MANAGER]-(a1:AGENT)-[r1:SUPERIOR*0..]->(a2:AGENT)
            timeTunnelPathExpander.add(RelTypes.HAS_MANAGER, Direction.INCOMING);
            timeTunnelPathExpander.add(RelTypes.HAS_WORKER, Direction.OUTGOING);
            timeTunnelPathExpander.add(RelTypes.SUPERIOR, Direction.OUTGOING);
            log.info("step 2");
            // create traversal description
            timeTunnelTraversal = graphDatabaseService.traversalDescription()
                    .expand(timeTunnelPathExpander, new InitialBranchState.State<ReadableInterval>(
                            new Interval(0, Long.MAX_VALUE), null
                    ))
                    .evaluator(new HasOverlapPathEvaluator())       // add all nodes to resultset having time tunnel
                    .evaluator(Evaluators.excludeStartPosition());
        }
        return timeTunnelTraversal;
    }

    private List<String> getLabels(Node node) {
        List<String> result = new ArrayList<>();
        for (Label label : node.getLabels()) {
            result.add(label.name());
        }
        return result;
    }
}