import requests
import json
import decimal
from types import SimpleNamespace
import DbWriter as db
from SeriesObjects import TimeSeriesDaily

API_KEY = 'KAKYPAXC475IQH11'

def getStockData(symbol):

    stockData = []
    url = f'https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol={symbol}&apikey={API_KEY}'
    r = requests.get(url)
    data = r.json()

    # with open('/Users/jhanna/git/shifty/src/data.txt', 'r', encoding='utf-8') as f:
    #     json_string_data = f.read()
    # data = json.loads(json_string_data)

    ts = data["Time Series (Daily)"]
    for key, value in ts.items():
        #print(value["1. open"])
        stockData.append(TimeSeriesDaily(key, symbol, decimal.Decimal(value["1. open"]), decimal.Decimal(value["2. high"]), decimal.Decimal(value["3. low"]), decimal.Decimal(value["4. close"]), int(value["5. volume"])))
    
    return stockData

def updateStockData(symbol):
    stockData = getStockData(symbol)
    db.insertStockData(stockData)

updateStockData("NOW")