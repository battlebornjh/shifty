import requests
import json
from datetime import datetime, timedelta
import decimal
import logging, sys
from types import SimpleNamespace
import DbWriter as db
from SeriesObjects import TimeSeriesDaily
from SeriesObjects import SeriesSet

API_KEY = 'KAKYPAXC475IQH11'

logging.basicConfig(
    stream=sys.stdout,
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def getStockData(symbol):
    stockData = []
    url = f'https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol={symbol}&apikey={API_KEY}'
    r = requests.get(url)
    data = r.json()

    # with open('/Users/jhanna/git/shifty/src/data.txt', 'r', encoding='utf-8') as f:
    #     json_string_data = f.read()
    # data = json.loads(json_string_data)

    ts = data.get("Time Series (Daily)")
    if ts:
        for key, value in ts.items():
            #print(value["1. open"])
            stockData.append(TimeSeriesDaily(key, symbol, decimal.Decimal(value["1. open"]), decimal.Decimal(value["2. high"]), decimal.Decimal(value["3. low"]), decimal.Decimal(value["4. close"]), int(value["5. volume"])))
    else:
        note = data.get("Note")
        if note and "API rate limit" in note:
            raise ApiRateLimitException()
        else:
            print(data)
    return stockData
    

def updateStockData(symbol):
    print(f"Updating {symbol}.")
    stockData = getStockData(symbol)
    if len(stockData) > 0:
        db.insertStockData(stockData)

def getSeriesData(dtStrt, dtEnd, symbols):
    data = []
    for i in range(len(symbols) + 1):
        data.append([])
    lastDt = dtStrt

    while lastDt <= dtEnd:
        founds = []
        for symbol in symbols:
            ts = db.getStockQuote(lastDt, symbol)
            if ts:
                founds.append(ts)
            else:
                break
        if len(founds) == len(symbols):
            data[0].append(lastDt)
            i = 1 #0 has dates
            for found in founds:
                data[i].append(float(found.close))
                i += 1
        lastDt = lastDt + timedelta(days=1)
    dts = data.pop(0)
    return SeriesSet(dts, symbols, data)

class ApiRateLimitException(Exception):

    def __init__(self, message="API rate limit reached"):
        self.message = f"{message}"
        super().__init__(self.message)

if __name__ == "__main__":
    symbols = ["AAPL", "ABAT", "NVDA", "NOW", "ORCL", "TTWO", "INTC", "IBM", "CRM", "GLD", "PLTR", "CSCO", "META", "FOXX", "MSFT", "AMZN"]
    #symbols = ["GLD", "PLTR", "CSCO", "META", "FOXX", "MSFT", "AMZN"]
    for symbol in symbols:
        updateStockData(symbol)