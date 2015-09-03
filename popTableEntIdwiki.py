import queryGenerator
from SPARQLWrapper import SPARQLWrapper, XML, JSON
import traceback
import time

def adjLimitVirt(query, countResults):
    query=query+" OFFSET "+str(countResults*10000)+" LIMIT 10000"
    return query


#UPDATE table1 SET col_a='k1', col_b='foo' WHERE key_col='1';
def populate():
    url="http://dbpedia-live.openlinksw.com/sparql"
    sparql = SPARQLWrapper(url)
    sparql.setTimeout(300)
    sparql.setReturnFormat(JSON)
    q = queryGenerator.QueryGenerator()
    
    countResults=411
    with open("insert.sql", "a") as myfile:
        try:
            while countResults >= 0:
#                 time.sleep(0.5)
                currQuery=adjLimitVirt(q.queryEntityPageIdWiki().query,countResults)
                sparql.setQuery(currQuery)
                print currQuery
                results = sparql.queryAndConvert()['results']['bindings']
                couple=[(r['s']['value'],int(r['id']['value'])) for r in results]
                if len(couple) > 0 :
#                     print couple[2]
                    map(lambda x:myfile.write(("UPDATE page SET dbpedia ='"+x[0]+"' where page_id = "+str(x[1])+";\n").encode('utf-8')),couple)
                    if len(couple) == 10000:
                        countResults=countResults+1
                    else:
                        countResults=-1    
                elif len(couple)==0:
                    countResults=-1
        except:
            var = traceback.format_exc()
            print var
        
populate()