import os, requests, time, datetime, configparser, warnings
from bs4 import BeautifulSoup
import pandas as pd
from Fetch_Data_Stock_US_Daily import getStocksList
import concurrent.futures
from tqdm import tqdm

def getSignleStockShortInfo(stock):
    df = pd.DataFrame()
    url = "http://shortsqueeze.com/?symbol=" + stock + "&submit=Short+Quote%E2%84%A2"
    repeat_times = 3
    downloadFailed = True

    for _ in range(repeat_times): 
        try:
            response = requests.get(url, timeout=15)
            downloadFailed = False
            break
        except Exception as e:
            print ("exception in get stock:" + stock, str(e))
            continue

    if downloadFailed:
        return "", df
    
    try:    
        tables = pd.read_html(response.text, attrs={'cellpadding': '3', 'width': '100%'})
    except Exception as e:
        print ("exception in parse stock:" + stock, str(e))
        return "", df

    for table in tables:
        if df.empty:
            df = table
        else:
            df = pd.concat([df, table])
    df = df.reset_index(drop=True, inplace=True)
    #print(df)
        
    soup = BeautifulSoup(response.text, 'lxml')
    dateString = soup.find('span', {"style" : "color:#999999;font-family: verdana, arial, helvetica;font-size:10px"}).get_text()
    date = datetime.datetime.strptime(dateString, '%A %B %d, %Y')
    return date, df.T


def updateStockShortData_US():
    Config = configparser.ConfigParser()
    Config.read("../../config.ini")
    dir = Config.get('Paths', 'SHORT_US')
    
    if os.path.exists(dir) == False: 
        os.makedirs(dir)

    stocklist = getStocksList()['symbol'].values.tolist()

    symbols = stocklist

    pbar = tqdm(total=len(symbols))
    log_errors = []
    log_update = []
    
    short_df = pd.DataFrame()
    for stock in symbols:
        startTime = time.time()
        date, df = getSignleStockShortInfo(stock)
        if short_df.empty:
            short_df = df
        else:
            short_df = pd.concat([short_df, df])
        outMessage = '%-*s fetched in:  %.4s seconds' % (6, stock, (time.time() - startTime))
        pbar.set_description(outMessage)
        pbar.update(1)

    short_df.to_csv(dir+date.strftime("%Y-%m-%d")+".csv")
    


    

    pbar.close()
  
    return symbols


if __name__ == "__main__":
    pd.set_option('precision', 3)
    pd.set_option('display.width',1000)
    warnings.filterwarnings('ignore', category=pd.io.pytables.PerformanceWarning)

    updateStockShortData_US()
