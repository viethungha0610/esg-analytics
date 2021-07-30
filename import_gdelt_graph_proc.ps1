$neo4j_shell = 'c:\users\zh834bt\.neo4jdesktop\relate-data\dbmss\dbms-2eaf39b9-8ef9-4ae3-9b71-1447ea06ddfb\bin\cypher-shell'
$cypher_script = 'import_gdelt_graph.cypher'
start-process -filepath $neo4j_shell -argumentlist "-u neo4j -p Convex2021/ -d esg --file $cypher_script"