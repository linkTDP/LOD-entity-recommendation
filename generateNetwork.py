import rdflib
from rdflib import Graph
import networkx as nx
import matplotlib.pyplot as plt
import mongoKnoesis


def generateGraph(filename,setStartingNodes=None):
    g = Graph()
    g.parse(filename, format="turtle")
    DG=nx.MultiDiGraph()
    for s,p,o in g:
        DG.add_edge(s.encode('ascii', 'replace'),o.encode('ascii', 'replace'),p=p.encode('ascii', 'replace'),)
    
    if setStartingNodes is not None:
        for entity in setStartingNodes:
            if entity.encode('ascii', 'replace') in DG:
                DG.node[entity.encode('ascii', 'replace')]['stating']=True
    
    return DG

def traslateGraphFromFile(filename,targetFileName,setStartingNodes=None):
    DG=generateGraph(filename, setStartingNodes)
    nx.write_gml(DG,targetFileName+".gml")
    
    
    
traslateGraphFromFile("files/connect3_20150802-113200.ttl", "files/3id2stepsV2", mongoKnoesis.getArticleById(3)['entitySet'])    


