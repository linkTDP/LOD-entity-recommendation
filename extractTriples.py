from rdflib import Graph, Literal, BNode, Namespace, RDF, URIRef, XSD
import spotlight
import newspaper as nw
from alchemyapi_python.alchemyapi import AlchemyAPI
import threading
from concurrent.futures.thread import ThreadPoolExecutor
import concurrent.futures
import logging
import queryGenerator
from SPARQLWrapper import SPARQLWrapper, XML, JSON
import traceback
from pprint import pprint
import mongoKnoesis
import time
import random
import itertools

x = logging.getLogger("logfun")
x.setLevel(logging.DEBUG)
h = logging.StreamHandler()
f = logging.Formatter("%(levelname)s %(asctime)s %(funcName)s %(lineno)d %(message)s")
h.setFormatter(f)
x.addHandler(h)
logfun = logging.getLogger("logfun")


def isUri_engDbp(s):
    if isinstance(s, URIRef) and ('dbpedia' not in s) or ('dbpedia' in s and ('en.dbpedia' in s or '//dbpedia.org' in s)):
        return True
    else:
        return False

"""
Given an Url the method extracts and filter the semantic content related to this url

It returns: 
    an rdflib.Graph g
    a set containing a set of rdflib.URIRef discovered in the rdf content

"""



rdf_type='http://www.w3.org/1999/02/22-rdf-syntax-ns#type'



def extractFromEntity(url,n):
    
    print 'node number: '+str(n)
    
    time.sleep((3*random.random()))

    if  isinstance(url, URIRef):
#         urllib.quote(
#         url.se   
#         print 'URI *****'
        url=str(url)
        
    if 'value' in url:
        print 'value in uri type:  '+type(url)+' ; and uri is: '+url
        
    print url    
    if 'dbpedia' in url:
        g=dbpediaExtraction(url)
    else:
        g.load(url)
    uriSet=set()
    removeLang=[]
    for s,p,o in g:    
        
        if isinstance(o, Literal) and o.language <> None and o.language <> 'en':
            removeLang.append((s,p,o))
        
        if isUri_engDbp(s):
            uriSet.add(s)

        if isUri_engDbp(o) and "http://www.w3.org/1999/02/22-rdf-syntax-ns#type" not in p:
            uriSet.add(o)

    for a in removeLang:
        g.remove(a)

    return g,uriSet

    

def getTextArticleByUrl(url):
    article = nw.Article(url)
    article.download()
    article.parse()
    return article.text,article.title,article.tags


def saveAnnotationMongoGivenUrl(url):
    text,tile,tags=getTextArticleByUrl(url)
    entitySet,annotationsSorted,response=getAnnotation(text)
    element={'_id':mongoKnoesis.getLastFreeId()+1,
              'url':url,
              'title':tile,
              'tags':[t for t in tags],
              'entitySet':[e for e in entitySet],
              'annSpot':annotationsSorted,
              'annAlch':response}
    mongoKnoesis.addArticle(element)




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


def convertToTriples(triple):
           
    s=URIRef(triple['s']['value'])
    p=URIRef(triple['p']['value'])
    if u'uri' <> triple['o']['type']:
        if triple['o']['type'] == 'literal':
            o=Literal(triple['o']['value'],lang=triple['o']['xml:lang'] if 'xml:lang' in triple['o'] else 'en')
        else:
            o=Literal(triple['o']['value'],datatype=URIRef(triple['o']['datatype']))
#             print o.toPython()
#             print type(o.toPython())
    else:
        o=URIRef(triple['o']['value'])
    return (s,p,o)
    


def dbpediaExtraction(node):
#     print "dbpedia query"
#     print type(node)


    url="http://dbpedia.org/sparql"
    sparql = SPARQLWrapper(url)
    sparql.setTimeout(300)
    sparql.setReturnFormat(JSON)
    q = queryGenerator.QueryGenerator()
    g = Graph()
    try:
        sparql.setQuery(q.getSubjectTriples(node).query)
        print q.getSubjectTriples(node).query
        r=[]
        results = sparql.queryAndConvert()['results']['bindings']
        
        if len(results) > 0:
            r.extend(map(lambda x:convertToTriples({'s':{'type':'uri','value':node},
                                        'p':x['p'],
                                        'o':x['o']}),results))
        sparql.setQuery(q.getObjectTriples(node).query)
        results = []
        results = sparql.queryAndConvert()['results']['bindings']
        if len(results) > 0:
            r.extend(map(lambda x:convertToTriples({'o':{'type':'uri','value':node},
                                            'p':x['p'],
                                            's':x['s']}),results))
    except:
        logfun.exception("Something awful happened!")
        var = traceback.format_exc()
        print var
        print 'len r '+str(len(r))
        print results
    
    map(lambda x: g.add(x),r)
    print 'len g query: '+str(len(g))
    
    return g
            
    


def downloadGraph(text,urlArt,fromDb=False,id=None):
    
    #inizialize
    
    logging.basicConfig()
    if fromDb:
        setNodes=set(mongoKnoesis.getEntitySetFromId(id))
    else:
        setNodes=getAnnotation(text)[0]
    
    g=Graph()
    
    for a in setNodes:
        g.add((URIRef(a), URIRef('http://knoesis.org/test/startingNodeOf'), URIRef(urlArt)))
    
    
    
    visitedNode=set()
    
    for step in range(2):
        
        with ThreadPoolExecutor(max_workers=20) as executor:
            print "node to visit: "+str(len(setNodes))
            listNodes=list(setNodes)
            future_extract = {executor.submit(extractFromEntity, listNodes[n], n): listNodes[n] for n in range(len(listNodes))}
#             print type(visitedNode),type(setNodes)
            visitedNode|=setNodes
            setNodes=set()
            for future in concurrent.futures.as_completed(future_extract):
                node = future_extract[future]
                try:
                    gCur,nodeCur = future.result()
        #             print data
                except Exception as exc:
                    print('%r generated an exception: %s' % (node, exc))
                else:
                    print ""
                    print "entity: "+node
                    print "triples: "+str(len(gCur))
                    g = g + gCur
                    setNodes.update(nodeCur)
        setNodes.difference_update(visitedNode)
        print ""
        print "----"
        print "step: "+str(step)
        print "triples"+str(len(g))
        
        
    print ""
    print "***"
    print ""
    print "serialize"
    g.serialize(destination='test.ttl', format='turtle')
   
   
def extractFromEntitySpeedUp(url,n):
    
    print 'node number: '+str(n)
    
    time.sleep((3*random.random()))
 
    if  isinstance(url, URIRef):
#         urllib.quote(
#         url.se   
#         print 'URI *****'
        url=str(url)
        
    if 'value' in url:
        print 'value in uri type:  '+type(url)+' ; and uri is: '+url
        
    print url    
    if 'dbpedia' in url:
        g=dbpediaExtraction(url)
    else:
        g.load(url)
    uriSet=set()
#     removeLang=[]
    setTripl=set()
    for s,p,o in g:    
        
        if not ( isinstance(o, Literal) and o.language <> None and o.language <> 'en'):
            setTripl.add((s,p,o))
            
        
        if isUri_engDbp(s):
            uriSet.add(s)

        if isUri_engDbp(o) and "http://www.w3.org/1999/02/22-rdf-syntax-ns#type" not in p:
            uriSet.add(o)


    return setTripl,uriSet   
    

def downloadGraphSpeedUp(text,urlArt,fromDb=False,id=None):
    
    #inizialize
    
    logging.basicConfig()
    if fromDb:
        setNodes=set(mongoKnoesis.getEntitySetFromId(id))
    else:
        setNodes=getAnnotation(text)[0]
    
    g=Graph()
    
    for a in setNodes:
        g.add((URIRef(a), URIRef('http://knoesis.org/test/startingNodeOf'), URIRef(urlArt)))
    
    
    
    visitedNode=set()
    
    for step in range(2):
        
        with ThreadPoolExecutor(max_workers=20) as executor:
            print "node to visit: "+str(len(setNodes))
            listNodes=list(setNodes)
            future_extract = {executor.submit(extractFromEntitySpeedUp, listNodes[n], n): listNodes[n] for n in range(len(listNodes))}
#             print type(visitedNode),type(setNodes)
            visitedNode|=setNodes
            setNodes=set()
            tSet=set()
            for future in concurrent.futures.as_completed(future_extract):
                node = future_extract[future]
                try:
                    triplesSet,nodeCur = future.result()
        #             print data
                except Exception as exc:
                    print('%r generated an exception: %s' % (node, exc))
                else:
                    print ""
                    print "entity: "+node
                    print "triples: "+str(len(triplesSet))
                    tSet.update(triplesSet)
                    setNodes.update(nodeCur)
        setNodes.difference_update(visitedNode)
        print ""
        print "----"
        print "step: "+str(step)
        print "triples"+str(len(tSet))
        
    
    print "creating the graph"
    
    for x in tSet:
    #    print type(x)
        try:
            s,p,o=x
            g.add((s,p,o))
        except:
            a = ""
    
    
    print ""
    print "***"
    print ""
    print "serialize"
    g.serialize(destination='test.ttl', format='turtle')



def getConnectedDbpedia(step,source,target):   
    #url="http://live.dbpedia.org/sparql"
    url="http://dbpedia-live.openlinksw.com/sparql"
    sparql = SPARQLWrapper(url)
    sparql.setTimeout(300)
    sparql.setReturnFormat(JSON)
    q = queryGenerator.QueryGenerator()
    g = Graph()
    trip=[]
    for query in q.getConnectedObj2(step, source, target):
        time.sleep(random.random())
        try:
            sparql.setQuery(query[0].query)
            print query[0].query
            results = sparql.queryAndConvert()['results']['bindings']
            #pprint(results)
            
            
            for res in results:
                current=[]
                for ed in query[1]:
                    """ 
                    Heuristics:
                        - be sure that the data don't contain cicles ( for the long path can append that the path go back and forth throug a node )
                        - remove path that contains rdf-type -> passing through instantation with long path i can reach too much nodes
                        - remove Template: nodes of dbpedia 
                    """
                    if ed[2] == 's' or ed[2] == 't' or ('type' in res[ed[2]] and res[ed[2]]['type'] == 'uri'): 
                        s=None
                        p=None
                        o=None
                        if ed[0] == 's':
                            s=URIRef(source)
                        elif ed[2] == 's':
                            o=URIRef(source)
                        elif ed[0] == 't':
                            s=URIRef(target)
                        elif ed[2] == 't':
                            o=URIRef(target)
                        
                        if ed[0] <> 's' and ed[0] <> 't':
                            s=URIRef(res[ed[0]]['value'])
                        if ed[2] <> 's' and ed[2] <> 't':
                            o=URIRef(res[ed[2]]['value'])
                        
                        p=URIRef(res[ed[1]]['value'])
                        current.append((s,p,o))
                    else:
                        current = []
                        break
                #print len(current)
                if len(current) > 0:        
                    setNodesQuery=set([e[0] for e in current])
                    map(lambda x: setNodesQuery.add(x[2]),current)
                    # be sure that the data don't contain
                    # remove Template: nodes of dbpedia
#                     pprint(current) 
#                     pprint(setNodesQuery)
                    if len(current) < len(setNodesQuery) and len(filter(lambda x: rdf_type in x[1] or 'Template:' in x[0] or 'Template:' in x[2],current)) == 0 :
                        map(lambda x:trip.append(x),current)
                        print "found "+str(len(current))+" triples :)"
                
            #pprint(trip)
        except:
            logfun.exception("Something awful happened!")
            var = traceback.format_exc()
        
    
    map(lambda x: g.add(x),trip)
    print 'len g query: '+str(len(g))
    
    return g


def testDbpediaCOnnectedNodes(id):
    setEnt=mongoKnoesis.getArticleById(id)['entitySet']
    graph=Graph()
    for a,b in itertools.combinations(setEnt,2):
        if ('dbpedia' in a or 'yago' in a) and ('dbpedia' in b or 'yago' in b):
            for step in range(2):
                
                g = getConnectedDbpedia(step+1,a,b)
                graph = graph + g
    print '*****'
    print ""
    
    print "total triples : "+str(len(graph))
    timestr = time.strftime("%Y%m%d-%H%M%S")
    graph.serialize(destination='files/connect'+str(id)+'_'+timestr+'.ttl', format='turtle')

source = 'http://dbpedia.org/resource/Scuderia_Ferrari'
target = 'http://dbpedia.org/resource/Modena'

url = "http://www.ndtv.com/world-news/earthquake-aid-supplies-stuck-at-nepal-customs-un-official-777127"

url2 = "http://www.miamiherald.com/news/nation-world/world/article27284962.html"

url3 = "http://www.nytimes.com/2015/07/31/world/middleeast/us-trained-islamic-state-opponents-reported-kidnapped-in-syria.html?_r=0"

#getConnectedDbpedia(1,source,target)

#print getAnnotation(getTextArticleByUrl(url3))

#testDbpediaCOnnectedNodes(3)

#hibridExtraction(knoesis.mongoKnoesis.getArticleById(1)['entitySet'])


#downloadGraphSpeedUp(getTextArticleByUrl(url),url,True,1)


#saveAnnotationMongoGivenUrl(url3)

#saveAnnotationMongoGivenUrl(url)





