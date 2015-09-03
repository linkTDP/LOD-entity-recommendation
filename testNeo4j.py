from py2neo import Graph


graph = Graph("http://neo4j:fagiolo@localhost:7474/db/data/")

for record in graph.cypher.execute("match (a:Person)-[u]-(b)-[w]-(c:Person) where a.name='Stephen Lang' return a ,u, b, w,c"):
    print record[0]['name']
    print record[1]
    print record[2]['type']
    print record[3]
    print record[4]['name']
    print type(record[0])

