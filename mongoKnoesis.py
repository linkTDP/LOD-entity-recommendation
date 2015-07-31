import pymongo as pm






def getLastFreeId():
    client = pm.MongoClient()
    db=client.knoesis
    a=db.article.find()
    if a.count() == 0:
        return 0
    else:
        cur = None
        for b in db.article.find().sort([['_id',pm.DESCENDING]]):
            cur=b
            break
        
        return cur['_id']

def addArticle(element):
    client = pm.MongoClient()
    db=client.knoesis
    db.article.save(element)

def getArticleById(id):
    client = pm.MongoClient()
    db=client.knoesis
    return db.article.find_one({'_id':id})

def getEntitySetFromId(id):
    client = pm.MongoClient()
    db=client.knoesis
    return db.article.find_one({'_id':id})['entitySet']


