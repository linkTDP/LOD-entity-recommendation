import threading
import Queue
import random
import time
from concurrent.futures.thread import ThreadPoolExecutor
import urllib2
import concurrent.futures  as features
import concurrent.futures

def get_mem(servername, q):
    res =servername
    time.sleep(random.random()*10)
    q.put(res)
    



URLS = ['http://www.foxnews.com/',
        'http://www.cnn.com/',
        'http://europe.wsj.com/',
        'http://www.bbc.co.uk/',
        'http://some-made-up-domain.com/',
        'http://www.google.com']

# Retrieve a single page and report the url and contents
def load_url(url, timeout):
    conn=urllib2.urlopen(url, timeout=timeout)
#     print conn.read()
        
    return conn.read()

# We can use a with statement to ensure threads are cleaned up promptly
with ThreadPoolExecutor(max_workers=6) as executor:
    # Start the load operations and mark each future with its URL
    future_to_url = {executor.submit(load_url, url, 60): url for url in URLS}
    for future in concurrent.futures.as_completed(future_to_url):
        url = future_to_url[future]
        print future_to_url
        print url
        try:
            data = future.result()
#             print data
        except Exception as exc:
            print('%r generated an exception: %s' % (url, exc))
        else:
            print('%r page is %d bytes' % (url, len(data)))

# threading.Thread(target=get_mem, args=("server01", q)).start()

