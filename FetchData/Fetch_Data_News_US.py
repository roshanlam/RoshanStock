import sys, os, time, datetime, configparser
import pandas as pd
from eventregistry import *
import urllib.parse
import hashlib
from bs4 import BeautifulSoup
from googletrans import Translator

cur_path = os.path.dirname(os.path.abspath(__file__))
for _ in range(2):
    root_path = cur_path[0:cur_path.rfind('/', 0, len(cur_path))]
    cur_path = root_path
sys.path.append(root_path + "/" + 'Source/DataBase/')
from DB_API import queryStockList, queryNews, storeNews

global_eventRegistry = None

def getEventRegistry(root_path):
    global global_eventRegistry
    if global_eventRegistry is None:
        config = configparser.ConfigParser()
        config.read(root_path + "/" + "config.ini")
        global_eventRegistry = EventRegistry(apiKey = config.get('EventRegistry', 'KEY'))
    return global_eventRegistry



def line_translation(article):
    import math, re
    split = math.ceil(len(article) / 1000)
    
    if split == 1:
        return youdao_translator(article)
    
    splitLineList = re.split('. ', article)
    linesList = [line for line in splitLineList if len(line) > 0]
    lineCount = len(linesList)
    averageLine = math.ceil(lineCount / split)

    print(split, lineCount, averageLine)
    start = 0
    end = 0
    trans = ""

    for index in range(split):
        end += averageLine
        if end > lineCount:
            sublist = linesList[start:]
        else:
            sublist = linesList[start:end]
            start = end

        print(sublist)
        article = "".join(sublist)
        trans += youdao_translator(article) + " "
    
    return trans

def youdao_translator(query):
    def md5(code):
        md = hashlib.md5()
        md.update(code.encode())
        return md.hexdigest()
    
    def getUrlWithQueryString(url,name,param):
        if '?' in url:
            url= url +'&'+ name + '=' + param
        else:
            url = url + '?' + name + '=' + param
        return url
    
    def getJson(url):
        response = requests.get(url)
        
        try:
            s = response.json()
            if s['errorCode'] != '0': return s['errorCode']
            return s['translation'][0]
        except: return ''

    appKey ="4ccd169a383639ab"
    salt = str('%0d'%(time.time()*1000))
    src = "en"
    dest = "zh-CHS"
    hashStr = appKey + query + salt + "BSur61YY25NQvRh4WdfMdCJ0DU5WaJqk"
    sign = md5(hashStr)

    url = 'https://openapi.youdao.com/api'
    url = getUrlWithQueryString(url, 'q', query)
    url = getUrlWithQueryString(url, 'salt', salt)
    url = getUrlWithQueryString(url, 'sign', sign)
    url = getUrlWithQueryString(url, 'from', src)
    url = getUrlWithQueryString(url, 'appKey', appKey)
    url = getUrlWithQueryString(url, 'to', dest)

    time.sleep(1)

    return getJson(url)

def translation(article):
    translator = Translator()
    soup = BeautifulSoup(article, 'lxml')
    paragraphs = soup.findAll('p')
    for p in paragraphs:
        for content in p.contents:
            if content.name == None and len(content) > 0:
                trans = youdao_translator(content) 
                #trans = translator.translate(content, src='en', dest='zh-CN').text
                #print(len(content), content.name)
                #print(content)
                #print(trans)
                content.replace_with(trans)
    return str(soup.body.next)


def getSingleStockNewsArticle(root_path, symbol, name, from_date, till_date, count):
    config = configparser.ConfigParser()
    config.read(root_path + "/" + "config.ini")

    queryTitle = '("%s" OR "%s")' % (name, symbol)
    queryText = '("%s" OR "%s" OR "%s" OR "%s")' % ('stock', 'nasdaq', 'market', 'business')
    queryString = 'language:en ' + \
                  'AND discoverDate:[' + from_date + ' TO ' + till_date + '] ' + \
                  'AND title:' + queryTitle + ' ' + \
                  'AND text:' + queryText
                  
    url = "https://api.newsriver.io/v2/search?query=" + urllib.parse.quote(queryString)
    

    response = requests.get(url, headers={"Authorization": config.get('NewsRiver', 'KEY')}, timeout=15)
    jsonFile = response.json()

    df = pd.DataFrame(columns=['date', 'time', 'title', 'source', 'ranking', 'sentiment', 'uri', 'url', 'body_html', 'body_eng', 'body_chn'])
    
    for art in jsonFile:
        try:
            finSentiment = art['metadata']['finSentiment']['sentiment']
        except:
            finSentiment = "0.0"
        
        try: 
            source = art['website']['hostName']
        except:
            source = "N/A"

        try:
            ranking = art['website']['rankingGlobal']
        except:
            ranking = "N/A"

        trans = translation(art['structuredText'])
     

        df.loc[len(df)] = [art['discoverDate'][:10], 
                           art['discoverDate'][11:19], 
                           art['title'], 
                           source, 
                           ranking, 
                           finSentiment,
                           art['id'], 
                           art['url'],
                           art['structuredText'],
                           art['text'], 
                           trans
                          ]

    return df

def updateNewsArticle(root_path, symbol, name, from_date, till_date, count):
    startTime = time.time()
    message = ""

    if len(symbol) == 0: return startTime, message

    

    df = getSingleStockNewsArticle(root_path, symbol, name, from_date, till_date, count)
    
    storeNews(root_path, "DB_STOCK", "SHEET_US_NEWS", symbol, df)
    

    
if __name__ == "__main__":
    if len(sys.argv) != 4:
        print("please input Stock symbol and start date, end date after python file")
        exit()

    pd.set_option('precision', 3)
    pd.set_option('display.width',1000)
    warnings.filterwarnings('ignore', category=pd.io.pytables.PerformanceWarning)

    symbol = str(sys.argv[1])

    stocklist = queryStockList(root_path, "DB_STOCK", "SHEET_US_DAILY")
    
    result = stocklist[stocklist.index == symbol]

    if result.empty:
        print("symbol not exist.")
        exit()

    start_date = str(sys.argv[2])
    end_date = str(sys.argv[3])

    now = datetime.datetime.now().strftime("%Y-%m-%d")

    config = configparser.ConfigParser()
    config.read(root_path + "/" + "config.ini")
    storeType = int(config.get('Setting', 'StoreType'))

  
    
    name = result['name'].values[0]
    print("fetching news of stock:", symbol, name)
    updateNewsArticle(root_path, symbol, name, start_date, end_date, 1)
