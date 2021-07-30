// A clean slate
MATCH (n)
DETACH DELETE n;

CREATE CONSTRAINT IF NOT EXISTS ON (org: Organisation) ASSERT org.id IS UNIQUE;

// Gdelt nodes
LOAD CSV WITH HEADERS FROM 'file:///gdelt_graph_nodes.csv' AS row
CREATE (n: Organisation {id: row.organisation, esg_index: toFloat(row.tone)});

// Gdelt edges
LOAD CSV WITH HEADERS FROM 'file:///gdelt_graph_edges.csv' AS row
MERGE (src: Organisation {id: row.src})
MERGE (dst: Organisation {id: row.dst})
MERGE (src)-[:RELATE {freq: toInteger(row.relationship), sentiment: toFloat(row.avg_tone)}]->(dst);