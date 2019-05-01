import sys, os, time, datetime, warnings, pickle, re, configparser, pytz
import pandas as pd
from collections import Counter
from nltk.sentiment.vader import nltk
from nltk.sentiment.vader import SentimentIntensityAnalyzer
import concurrent.futures
from tqdm import tqdm

cur_path = os.path.dirname(os.path.abspath(__file__))
for _ in range(2):
    root_path = cur_path[0:cur_path.rfind('/', 0, len(cur_path))]
    cur_path = root_path
sys.path.append(root_path + "/" + 'Source/Utility/python-twitter/')
sys.path.append(root_path + "/" + 'Source/DataBase/')
from DB_API import queryStockList, queryTweets, storeTweets

gloabl_api = None

def getTwitterApi(root_path):
    global gloabl_api
    if gloabl_api is None:
        import twitter
        config = configparser.ConfigParser()
        config.read(root_path + "/" + "config.ini")
        api_key = config.get('Twitter', 'KEY')
        api_secret = config.get('Twitter', 'SECRET')
        access_token_key = config.get('Twitter', 'TOKEN_KEY')
        access_token_secret = config.get('Twitter', 'TOKEN_SECRET')
        http = config.get('Proxy', 'HTTP')
        https = config.get('Proxy', 'HTTPS')
        proxies = {'http': http, 'https': https}
        gloabl_api = twitter.Api(api_key, api_secret, access_token_key, access_token_secret, timeout = 15, proxies=proxies)
    return gloabl_api


def getSentiments(df):
    sid = SentimentIntensityAnalyzer()
    tweet_str = ""
    for tweet in df['Text']:
        tweet_str = tweet_str + " " + tweet
    print(sid.polarity_scores(tweet_str))

def getWordCount(df, symbol, stocklist):
    ignore = ['the', 'rt', 'of', 'in', 'to', 'is', 'at', 'for', 'you', 'on', 'thursday', 'this', 'with', 'today', 'no', 'still', 
              'into', 'and', 'datacenter', 'https', 'all', 'play', 'stocks', 'watch', 'earnings', 'but', 'price', 'tomorrow', 
              'want', 'rating', 'open', 'shop', 'autonomous', 'be', 'money', 'reason', 'companies', 'company', 'that', 'when', 
              'made', 'new', 'who', 'president', 'was', 'market', 'looking', 'can', 'fbi', 'your', 'it', 'day', 'him', 'chief',  
              'united', 'historically', 'fires', 'investigating', 'from', 'while', 'profits', 'again', 'didn', 'upon', 'make', 
              'states', 'been', 'stock', 'let', 'put', 'basket', 'since', 'time', 'were', 'bought', 'q1', 'as', 'than', 'trading',
              'co', 'inc', 'keep', 'bank', 'target', 'has', 'news', 'by', 'asset', 'management', 'position', 'ipo', 'first', 
              'reaffirmed', 'there', 'expectations', 'after', 'bid', 'report', 'its', 'results', 'sellers', 'puts', 'out', 'may',
              'they', 'over', 'months', 'parent', 'magnitude', 'pack', 'quarter', 'what', 'plc', 'weight', 'given', 'looked', 'see',
              'weekly', 'review']
    words = []
    for tweet in df['Text']:
        words.extend(re.split(r'[-;:,./$#\'"’\s]\s*', tweet))
        
    counts = Counter(word for word in words if len(word) > 1 and word not in ignore and word.isdigit() == False)
    counts = counts.most_common()
    top5 = []
    
    for count in counts:
        match_stock_name = ''
        match_rate = 0.6
        str_count = str(count[0])
        len_count = len(str_count)
        for stock in stocklist:
            if stock == symbol: continue
            if stock.lower() in str_count:
                temp_match_rate = len(stock) / len_count 
                if temp_match_rate > match_rate:
                    match_stock_name = stock
                    match_rate = temp_match_rate
        #print(match_stock_name, match_rate, count)
        if len(match_stock_name) > 0: top5.append(match_stock_name)
        if len(top5) == 5: break
    return top5


def getSingleStockTwitter(root_path, symbol, from_date, till_date):
    api = getTwitterApi(root_path)

    col = ['Date', 'ID', 'Text']
    df, lastUpdateTime = queryTweets(root_path, "DB_MEDIA", "SHEET_TWITTER", symbol, col)
    
    symbol = "$"+symbol
    now = datetime.datetime.now()
    today = str(now.year) + "-" + str(now.month) + "-" + str(now.day)
    yesterday = now - datetime.timedelta(days=1)
    yesterday = str(yesterday.year) + "-" + str(yesterday.month) + "-" + str(yesterday.day)

    totalTweets = api.GetSearch(symbol, count=200, result_type="recent", lang='en', since=from_date, until=till_date)

    
    for tweet in totalTweets:
        date = datetime.datetime.strptime(tweet.created_at, '%a %b %d %H:%M:%S +0000 %Y').replace(tzinfo=pytz.UTC)
        try:
            df.loc[len(df)] = [date.strftime("%Y-%m-%d %H:%M:%S"), tweet.id, tweet.text.lower()]
        except Exception as e:
            print("------------------")
            print("date", date)
            print("id", tweet.id)
            print("text", tweet.text)
            print("error", e)

    storeTweets(root_path, "DB_MEDIA", "SHEET_TWITTER", symbol, df)
    return df


def updateSingleStockTwitterData(root_path, symbol, from_date, till_date):
    startTime = time.time()
    getSingleStockTwitter(root_path, symbol, from_date, till_date)
    return startTime


def updateStockTwitterData(root_path, from_date, till_date, storeType):
    df = queryStockList(root_path, "DB_STOCK", "SHEET_US_DAILY")
    symbols = df['symbol'].values.tolist()

    pbar = tqdm(total=len(symbols))
    
    if storeType == 1 or storeType == 2:
        for symbol in symbols:
            startTime = updateSingleStockTwitterData(root_path, symbol, from_date, till_date)
            outMessage = '%-*s fetched in:  %.4s seconds' % (6, symbol, (time.time() - startTime))
            pbar.set_description(outMessage)
            pbar.update(1)
        

  

    pbar.close()

if __name__ == "__main__":
   
    pd.set_option('precision', 3)
    pd.set_option('display.width',1000)
    warnings.filterwarnings('ignore', category=pd.io.pytables.PerformanceWarning)
    
    now = datetime.datetime.now().strftime("%Y-%m-%d")

    config = configparser.ConfigParser()
    config.read(root_path + "/" + "config.ini")
    storeType = int(config.get('Setting', 'StoreType'))

    if storeType == 1:
        from Start_DB_Server import StartServer, ShutdownServer

        thread = StartServer(root_path)
        
       
        time.sleep(5)
    
    updateStockTwitterData(root_path, "1990-01-01", now, storeType)

    if storeType == 1:
      
        time.sleep(5)
        ShutdownServer()
