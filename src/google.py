#!/usr/bin/python
#http://virendra.me/googles-did-you-mean-hack-in-python/
#http://stackoverflow.com/questions/1657570/google-search-from-a-python-app/1657597#1657597

import json
import urllib
import os
import urllib2
import io
import gzip
import sys
import urllib
import re

from bs4 import BeautifulSoup
from StringIO import StringIO

class GoogleSearchEngine:
  def __init__(self):
    pass
  
  def is_wikipedia_link(self, url):
    if 'wikipedia.org' in url: return True
    return False
  
  def get_google_hit(self, words):
    query = urllib.urlencode({'q': words})
    url = 'http://ajax.googleapis.com/ajax/services/search/web?v=1.0&%s' % query
    search_response = urllib.urlopen(url)
    search_results = search_response.read()
    results = json.loads(search_results)
    
    data = results['responseData']
    
    hits = data['results']
    
    top_hit = None
    if len(hits) > 0:
      top_hit = hits[0]['url']
    
    return data['cursor']['estimatedResultCount'], top_hit

  def getPage(self, url):
    request = urllib2.Request(url)
    request.add_header('Accept-encoding', 'gzip')
    request.add_header('User-Agent','Mozilla/5.0 (Macintosh; Intel Mac OS X 10_7_3) AppleWebKit/535.20 (KHTML, like Gecko) Chrome/19.0.1036.7 Safari/535.20')
    response = urllib2.urlopen(request)
    if response.info().get('Content-Encoding') == 'gzip':
        buf = StringIO( response.read())
        f = gzip.GzipFile(fileobj=buf)
        data = f.read()
    else:
        data = response.read()
    return data

  def didYouMean(self, q):
    q = str(str.lower(q)).strip()
    url = "http://www.google.com/search?q=" + urllib.quote(q)
    html = self.getPage(url)
    soup = BeautifulSoup(html)
    ans = soup.find('a', attrs={'class' : 'spell'})
    try:
        result = repr(ans.contents)
        result = result.replace("u'","")
        result = result.replace("/","")
        result = result.replace("<b>","")
        result = result.replace("<i>","")
        result = re.sub('[^A-Za-z0-9\s]+', '', result)
        result = re.sub(' +',' ',result)
    except AttributeError:
        result = 1
    return result
    
if __name__ == "__main__":
  import sys
  gse = GoogleSearchEngine()
  hit, url = gse.get_google_hit(sys.argv[1])
  didyoumean = gse.didYouMean(sys.argv[1])
  print hit, url, didyoumean
  print gse.is_wikipedia_link(url)