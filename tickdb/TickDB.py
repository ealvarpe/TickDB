from arctic import Arctic
from datetime import datetime
from tqdm import tqdm
import pandas as pd
from selenium.webdriver.chrome.webdriver import WebDriver
from bs4 import BeautifulSoup


# LIBRARIES AND TIMEFRAMES SUPPORTED
# YAHOO - 1D

class TickDB:

    def __init__(self, library = 'YAHOO', timeframe = '1D'):

        self.library = library
        self.timeframe = timeframe

        # Connect to Local MONGODB
        store = Arctic('localhost')

        # Create the library - defaults to VersionStore
        libs = store.list_libraries()
        libName = library+'-'+timeframe
        if (libName) not in libs:
            store.initialize_library(libName)

        # Access the library
        self.lib = store[libName]
        self.tickers = self.lib.list_symbols()
        self.tickers.sort()

    def getTickDBName(self):
        return (self.library+'-'+self.timeframe)

    def addSymbols(self, symbols):

        if self.tickers is None or len(self.tickers) == 0:
            self.tickers = symbols
        else:
            self.tickers.extend(symbols)

        self.tickers = list(set(self.tickers))
        self.tickers.sort()

    def delAllSymbols(self):
        tickers = self.lib.list_symbols()
        for ticker in tickers:
            self.lib.delete(ticker)
        self.tickers = None

    def delSymbols(self, symbols):

        for sym in tqdm(symbols):
            self.lib.delete(sym)

        self.tickers = self.lib.list_symbols()
        self.tickers.sort()

    def getSymbols(self):
        syms = self.lib.list_symbols()
        syms.sort()
        return syms

    def hasSymbol(self, symbol):
        return self.lib.has_symbol(symbol)

    def getMetadata(self, symbol):
        return self.lib.read_metadata(symbol).metadata

    def updateMetadata(self, symbols=None, fast=False):

        if symbols is None:
            symbols = self.tickers

        for ticker in tqdm(symbols):
            if self.lib.has_symbol(ticker):
                if fast:
                    if 'currency' in self.getMetadata(ticker):
                        continue
                try:
                    metadata1 = self.getMetadata(ticker)
                    metadata2 = self.getRawMetadata(ticker)
                    metadata = {**metadata1, **metadata2}
                    self.lib.write_metadata(ticker, metadata=metadata)
                except:
                    print("Error retrieving %s metadata" % ticker)
                    metadata = self.getMetadata(ticker)
                    metadata['currency'] = "N/A"
                    self.lib.write_metadata(ticker, metadata=metadata)
                    continue
            else:
                print("%s is not on the DB" % ticker)

    def updateDB(self, symbols=None, fast=False):

        if symbols is None:
            symbols = self.tickers

        for ticker in tqdm(symbols):
            start_date = '1970-01-01'
            end_date = datetime.strftime(datetime.today(), '%Y-%m-%d')

            if fast:
                if self.lib.has_symbol(ticker):
                    continue

            if self.lib.has_symbol(ticker):
                metadataT = self.lib.read_metadata(ticker).metadata
                start_date = metadataT['lastTradeDate']

            if start_date == end_date:
                print("%s already updated" % ticker)
                continue

            try :
                df_data, metadata = self.getRawData(ticker, start_date, end_date)
            except:
                print("Error retrieving %s" % ticker)
                continue

            if self.lib.has_symbol(ticker):
                self.lib.append(ticker, df_data, upsert=False)
                metadataT['lastTradeDate'] = end_date
                self.lib.write_metadata(ticker, metadata=metadataT)
            else:
                self.lib.write(ticker, df_data, metadata=metadata)

        self.tickers = self.getSymbols()

    def getData(self, symbol, start_date = None, end_date = None):

        if start_date is None and end_date is None:
            return self.lib.read(symbol).data
        else:
            if start_date is None:
                start_date = self.lib.read_metadata(symbol).metadata['firstTradeDate']
            if end_date is None:
                end_date = datetime.strftime(datetime.today(), '%Y-%m-%d')
            from arctic.date import DateRange
            return self.lib.read(symbol, date_range=DateRange(start_date, end_date)).data

    def getStocksFromCSV(self, file):
        return list(pd.read_csv("tickdb\data\\"+file+".csv")['Ticker'])

    def writeStocksToCSV(self, symbols, file):
        with open("tickdb\data\\" + file + ".csv", 'w') as f:
            f.write("Ticker\n")
            for item in symbols:
                f.write("%s\n" % item)

    #Get all the symbols with data within limits (only if it covers all the timerange)
    #if start_date not provided, only checks that is updated until end_date
    #if end_date is not provided only checks that the data starts on the start_date
    def getSymbolsFromTo(self, symbols=None, start_date='', end_date=''):
        if symbols is None:
            symbols = self.getSymbols()
        symbolsT = []
        for sym in tqdm(symbols):
            met = self.getMetadata(sym)
            if start_date != '' and end_date != '':
                if pd.to_datetime(met['firstTradeDate']) <= pd.to_datetime(start_date) and pd.to_datetime(met['lastTradeDate']) >= pd.to_datetime(end_date):
                    symbolsT.append(sym)
            elif start_date != '':
                if pd.to_datetime(met['firstTradeDate']) <= pd.to_datetime(start_date):
                    symbolsT.append(sym)
            elif end_date != '':
                if pd.to_datetime(met['lastTradeDate']) >= pd.to_datetime(end_date):
                    symbolsT.append(sym)
            else:
                return symbols

        return symbolsT

    def getSymbolsByExchange(self, symbols=None, exchange=""):
        return self.getSymbolsByAttr(symbols, "exchange", exchange)

    def getSymbolsBySearchName(self, symbols=None, value=""):
        if symbols is None:
            symbols = self.getSymbols()

        symbolsT = []
        for sym in tqdm(symbols):
            met = self.getMetadata(sym)
            if 'longName' in met:
                if value.lower() in met['longName'].lower():
                    symbolsT.append(sym)

        return symbolsT

    def getSymbolsByInstrumentType(self, symbols=None, type=""):
        return self.getSymbolsByAttr(symbols, "quoteType", type)

    def getSymbolsByCurrency(self, symbols=None, ccy=""):
        return self.getSymbolsByAttr(symbols, "currency", ccy)

    def getSymbolsBySector(self, symbols=None, sector=""):
        return self.getSymbolsByAttr(symbols, "sector", sector)

    def getSymbolsByIndustry(self, symbols=None, industry=""):
        return self.getSymbolsByAttr(symbols, "industry", industry)

    def getSymbolsByAttr(self, symbols=None, attr="", value=""):
        if symbols is None:
            symbols = self.getSymbols()
        symbolsT = []
        for sym in tqdm(symbols):
            met = self.getMetadata(sym)
            if attr in met:
                if met[attr] == value:
                    symbolsT.append(sym)

        return symbolsT

    def getAllInstrumentTypes(self, symbols=None):
        return self.getAllAttr(symbols, 'quoteType')

    def getAllExchanges(self, symbols=None):
        return self.getAllAttr(symbols, 'exchange')

    def getAllCurrencies(self, symbols=None):
        return self.getAllAttr(symbols, 'currency')

    def getAllSectors(self, symbols=None):
        return self.getAllAttr(symbols, 'sector')

    def getAllIndustries(self, symbols=None):
        return self.getAllAttr(symbols, 'industry')

    def getAllAttr(self, symbols=None, attr=""):
        if symbols is None:
            symbols = self.getSymbols()
        values = []
        for sym in tqdm(symbols):
            met = self.getMetadata(sym)
            if attr in met:
                values.append(met[attr])

        return list(set(values))


    def getSymbolsList_SP500(self):
        #table = pd.read_html('https://en.wikipedia.org/wiki/List_of_S%26P_500_companies')
        #df = table[0]['Symbol']
        #return list(df)
        table = self.getETFHoldings("SPY").T
        return table

    def get_table(self, soup):
        for t in soup.select('table'):
            header = t.select('thead tr th')
            if len(header) > 2:
                if (header[0].get_text().strip() == 'Symbol'
                        and header[2].get_text().strip().startswith('% Holding')):
                    return t
        raise Exception('could not find symbol list table')

    def getETFHoldings(self, etf_symbol):
        url = 'https://www.barchart.com/stocks/quotes/{}/constituents?page=all'.format(etf_symbol)

        # Loads the ETF constituents page and reads the holdings table
        browser = WebDriver("..\\chromedriver_win32\\chromedriver.exe")  # webdriver.PhantomJS()
        browser.get(url)
        html = browser.page_source
        soup = BeautifulSoup(html, 'html')
        import time
        time.sleep(5)
        table = self.get_table(soup)

        # Reads the holdings table line by line and appends each asset to a
        # dictionary along with the holdings percentage
        asset_dict = {}
        for row in table.select('tr')[1:-1]:
            try:
                cells = row.select('td')
                # print(row)
                symbol = cells[0].get_text().strip()
                # print(symbol)
                name = cells[1].text.strip()
                celltext = cells[2].get_text().strip()
                percent = float(celltext.rstrip('%'))
                shares = int(cells[3].text.strip().replace(',', ''))
                if symbol != "" and percent != 0.0:
                    asset_dict[symbol] = {
                        'name': name,
                        'percent': percent,
                        'shares': shares,
                    }
            except BaseException as ex:
                print(ex)
        browser.quit()
        return pd.DataFrame(asset_dict)