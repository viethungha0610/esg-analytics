// Personalised PageRank

// Step 0 - Drop the graph if it already exists
CALL gds.graph.drop('gdelt-analytics');

// Step 1 - Create Graph Projection
CALL gds.graph.create(
    'gdelt-analytics',
    'Organisation',
    'RELATE',
    {
        relationshipProperties: 'freq'
    }
)

// Step 2 - Run implementation
MATCH (org:Organisation {id: 'citigroup'})
CALL gds.pageRank.stream('gdelt-analytics', {
    maxIterations: 100,
    dampingFactor: 0.85,
    sourceNodes: [org],
    relationshipWeightProperty: 'freq'
})
YIELD nodeId, score
RETURN gds.util.asNode(nodeId).id AS id, score
ORDER BY score DESC, id ASC
LIMIT 11