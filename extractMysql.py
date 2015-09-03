import MySQLdb
import mongoKnoesis
from rdflib import Graph, Literal, BNode, Namespace, RDF, URIRef, XSD
import itertools
import time
from SPARQLWrapper import SPARQLWrapper, XML, JSON
import queryGenerator
import random
import logging
import traceback
from alchemyapi_python.alchemyapi import AlchemyAPI
import spotlight
import newspaper as nw
import networkx as nx

x = logging.getLogger("logfun")
x.setLevel(logging.DEBUG)
h = logging.StreamHandler()
f = logging.Formatter("%(levelname)s %(asctime)s %(funcName)s %(lineno)d %(message)s")
h.setFormatter(f)
x.addHandler(h)
logfun = logging.getLogger("logfun")


db=MySQLdb.connect("localhost","root","mysqldata","wikipedia")



def getTextArticleByUrl(url):
    article = nw.Article(url)
    article.download()
    article.parse()
    return article.text,article.title,article.tags



def saveAnnotationMongoGivenUrl(entitySet,annotationsSorted,response,url):
    text,tile,tags=getTextArticleByUrl(url)
    _id=mongoKnoesis.getLastFreeId()+1
    element={'_id':_id,
              'url':url,
              'title':tile,
              'tags':[t for t in tags],
              'entitySet':[e for e in entitySet],
              'annSpot':annotationsSorted,
              'annAlch':response,
              'text':text}
    mongoKnoesis.addArticle(element)
    return _id



def getAnnotation(text):
    annotations = spotlight.annotate('http://spotlight.dbpedia.org/rest/annotate',text,confidence=0.25, support=40)
    annotationsSorted = sorted(annotations, key=lambda k: k['similarityScore']) 
    setSpotlight=set(map(lambda x:x['URI'],annotationsSorted))

    """
    { u'URI': u'http://dbpedia.org/resource/People',
      u'offset': 321,
      u'percentageOfSecondRank': -1.0,
      u'similarityScore': 0.08647863566875458,
      u'support': 426,
      u'surfaceForm': u'people',
      u'types': u'DBpedia:TopicalConcept'}
    """
    
    alchemyapi = AlchemyAPI()
    response = alchemyapi.entities('text', text, {'sentiment': 1})
    resFilt=filter(lambda x: 'disambiguated' in x, response['entities'])
    key=['dbpedia','geonames','yago','opencyc']
    resFilt
    
    
    entitySet=set()

    for r in resFilt:
        for k in key:
            if k in r['disambiguated']:
                entitySet.add(r['disambiguated'][k])
    
    
    """
    {u'count': u'1',
      u'disambiguated': {u'dbpedia': u'http://dbpedia.org/resource/Kathmandu',
       u'freebase': u'http://rdf.freebase.com/ns/m.04cx5',
       u'geo': u'27.716666666666665 85.36666666666666',
       u'geonames': u'http://sws.geonames.org/1283240/',
       u'name': u'Kathmandu',
       u'subType': [u'TouristAttraction'],
       u'website': u'http://www.kathmandu.gov.np/',
       u'yago': u'http://yago-knowledge.org/resource/Kathmandu'},
      u'relevance': u'0.33',
      u'sentiment': {u'type': u'neutral'},
      u'text': u'Kathmandu',
      u'type': u'City'},
    """
    
    entitySet.update(setSpotlight)
    
    return entitySet,annotationsSorted,response


def generateNetworkFromUrl(url):
    q = queryGenerator.QueryGenerator()
    #extract annotation
    entitySetF=set()
    annotationsSortedF=[]
    responseF=[]
    
    invPage={}
    invInfo={}
    
    for c in range(3):
        try:
            entitySet,annotationsSorted,response=getAnnotation(getTextArticleByUrl(url))
        except:
            entitySet=set()
            annotationsSorted=[]
            response=[]
        entitySetF=entitySetF|entitySet
        if len(annotationsSorted)>len(annotationsSortedF):
            annotationsSortedF=annotationsSorted
        if len(response) > len(responseF):
            responseF=response
        print 'resp'
    #save on Mongodb
    
    _id=saveAnnotationMongoGivenUrl(entitySetF,annotationsSortedF,responseF,url)

    """" filter entity set """
    """ get id from entity """
    entityPageSet=set()
    for e in entitySetF:
        c=db.cursor()
        print q.getIdsPagesFromEntity(e)
        c.execute(q.getIdsPagesFromEntity(e))
        current=c.fetchall()
        if len(current)>0:
            invPage[current[0][0]]=e
            invInfo[e]=current[0][0]
            entityPageSet.add((e,current[0][0],True))
            

    
    
    
    
    """ find connection """
        
    nodes=entityPageSet
    infoSet=set()
    linkSet=set()
    
    for a,b in itertools.combinations(entityPageSet,2):
        
        cur=set()
        for step in range(2):#4
            cur = getConnectedInfobox(step,a[0],b[0])
            infoSet = infoSet | cur
        
        for step in range(2):#3
            cur = getConnectedPages(step, a[1], b[1])
            linkSet = linkSet | cur
        if len(linkSet) + len(infoSet) > 15:
            break
    # (  )    
    """ merge data """
        
    for u in infoSet:
        for w in u:
            if w not in invInfo:
                c=db.cursor()
                c.execute(q.getIdsPagesFromEntity(w))
                current=c.fetchall()
                if len(current)>0:
                    invPage[current[0][0]]=w
                    invInfo[w]=current[0][0]
    
    for u in linkSet:
        for w in u:
            if w not in invPage:
                c=db.cursor()
                c.execute(q.getIdsEntityFromPageID(w))
                current=c.fetchall()
                if len(current)>0:
                    invInfo[current[0][0]]=w
                    invPage[w]=current[0][0]
                
    print len(invInfo)
    print len(invPage)
    
    print linkSet
    print infoSet
    
    nodes=set()
    edges=set()
    for e in linkSet:
        n1=(e[0],invPage[e[0]] if e[0] in invPage else None,True if e[0] in invPage and invPage[e[0]] in entitySetF else False)
        n2=(e[1],invPage[e[1]] if e[1] in invPage else None,True if e[1] in invPage and invPage[e[1]] in entitySetF else False)
        e=(e[0],e[1],False)
        nodes.add(n1)
        nodes.add(n2)
        edges.add(e)
    
    for e in infoSet:
        if (e[0],e[1],False) in edges:
            edges - (e[0],e[1],False)
            edges.add((e[0],e[1],True))
            print 'change'
        
    print nodes
    print edges
        
    DG=nx.MultiDiGraph()
    
    for e in edges:
        DG.add_edge(e[0],e[1],dbpedia=e[2])
    for n in nodes:
        if n[0] in invInfo:
            DG.node[invInfo[n[0]]]['dbpedia']=True if invInfo[n[1]] is not None else False
            DG.node[invInfo[n[0]]]['entity']=invInfo[[n[1]]]
            DG.node[invInfo[n[0]]]['starting']=n[2]
        
    nx.write_gml(DG,str(_id)+".gml")
                
                
    
        
    print '*****'
    print ""
#     
#     print "total triples : "+str(len(graph))
#     timestr = time.strftime("%Y%m%d-%H%M%S")
#     graph.serialize(destination='files/connect'+str(id)+'_'+timestr+'.ttl', format='turtle')
    
def getConnectedPages(step,source,target):   
    q = queryGenerator.QueryGenerator()
    trip=set()
    for query in q.getConnectedObjMysqlWikiLinks(step, source, target):
        try:
            c=db.cursor()
            print query[0]
            c.execute(query[0])
            results = c.fetchall()
            for res in results:
                current=[]
                for ed in range(len(query[1])):
                    curEdge=None
                    if query[1][ed]:
                        curEdge=(res[ed],res[ed+1])
                    else:
                        curEdge=(res[ed+1],res[ed])
                    current.append(curEdge)
                #print len(current)
                if len(current) > 0:   
                    setNodesQuery=set([e[0] for e in current])
                    map(lambda x: setNodesQuery.add(x[1]),current)

                    if len(current) < len(setNodesQuery):
                        map(lambda x:trip.add(x),current)
                        print "found "+str(len(current))+" triples :)"        
        except:
            logfun.exception("Something awful happened!")
            var = traceback.format_exc()
            print var
    return trip
    
def getConnectedInfobox(step,source,target):   
    q = queryGenerator.QueryGenerator()
    trip=set()
    for query in q.getConnectedObjMysqlInfobox(step, source, target):
        try:
            c=db.cursor()
            print query[0]
            c.execute(query[0])
            results = c.fetchall()
            for res in results:
                current=[]
                for ed in range(len(query[1])):
                    curEdge=None
                    if query[1][ed]:
                        curEdge=(res[ed],res[ed+1])
                    else:
                        curEdge=(res[ed+1],res[ed])
                    current.append(curEdge)
                #print len(current)
                if len(current) > 0:   
                    setNodesQuery=set([e[0] for e in current])
                    map(lambda x: setNodesQuery.add(x[1]),current)

                    if len(current) < len(setNodesQuery):
                        map(lambda x:trip.add(x),current)
                        print "found "+str(len(current))+" triples :)"       
        except:
            logfun.exception("Something awful happened!")
            var = traceback.format_exc()
            print var
    return trip

def getIdsPagesFromEntities(entities):
    q = queryGenerator.QueryGenerator()
    ids=set()
    for e in entities:
        query=q.getIdsPagesFromEntity(e)
        c=db.cursor()
        c.execute(query)
        ids.add(c.fetchall()[0][0])
    return ids
            
            
source = 'http://dbpedia.org/resource/Seal_of_Alabama'
target= 'http://dbpedia.org/resource/Flag_of_Alabama'
a="http://dbpedia.org/resource/AfghanistanHistory"
b="http://dbpedia.org/resource/AfghanistanPeople"
#print getConnectedDbpedia(1, source, target)
c=13
d=15
e=10254318
f=10254938

#getIdsPagesFromEntities(set([a,b]))
#print getConnectedPages(0,e,f)



urls=['http://www.ndtv.com/world-news/earthquake-aid-supplies-stuck-at-nepal-customs-un-official-777127',
      'http://www.miamiherald.com/news/nation-world/world/article27284962.html',
      'http://www.nytimes.com/2015/07/31/world/middleeast/us-trained-islamic-state-opponents-reported-kidnapped-in-syria.html?_r=0',
      'http://business.financialpost.com/news/economy/how-conrad-black-foresaw-chinas-crisis-even-during-the-countrys-years-of-double-digit-growth',
      'http://www.theguardian.com/media/2015/aug/29/julian-assange-told-edward-snowdon-not-seek-asylum-in-latin-america',
      'http://www.economist.com/news/business/21662618-one-will-run-and-run-google-finally-responds-europes-antitrust-charges',
      'http://www.foxnews.com/politics/2015/08/28/source-fbi-team-leading-serious-clinton-server-probe-focusing-on-defense-info/',
      'http://www.bloomberg.com/news/articles/2015-08-27/tesla-with-insane-mode-busts-curve-on-consumer-reports-ratings-idu1hfk0',
      'http://www.bloomberg.com/news/articles/2015-08-27/serena-williams-chases-tennis-history-while-trailing-at-the-bank'
      ]

for u in urls:
    
    generateNetworkFromUrl(u)
            