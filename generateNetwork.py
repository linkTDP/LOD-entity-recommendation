import rdflib
from rdflib import Graph
import networkx as nx
import matplotlib.pyplot as plt
import mongoKnoesis





def traslateGraphFromFile(filename,targetFileName,setStartingNodes=None):
    g = Graph()
    g.parse(filename, format="turtle")
    DG=nx.MultiDiGraph()
    for s,p,o in g:
        DG.add_edge(s.encode('ascii', 'replace'),o.encode('ascii', 'replace'),p=p.encode('ascii', 'replace'),)
    
    
    
#     nx.draw(DG)
#     plt.savefig("path.png")
    
    
    if setStartingNodes is not None:
        for entity in setStartingNodes:
            if entity.encode('ascii', 'replace') in DG:
                DG.node[entity.encode('ascii', 'replace')]['stating']=True
    
    
    
    nx.write_gml(DG,targetFileName+".gml")
    
    
    
traslateGraphFromFile("files/connect2V2.ttl", "files/2id2stepsV2", mongoKnoesis.getArticleById(2)['entitySet'])    


