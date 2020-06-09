from tickdb.TickDB import TickDB
import yfinance as yf

class TickDBYahoo(TickDB):

    def __init__(self, library='YAHOO', timeframe='1D'):
        print("Setting up TickDB for %s with Timeframe %s" % (library, timeframe))
        super().__init__(library, timeframe)

    def getRawData(self, ticker, start_date, end_date):
        sym = yf.Ticker(ticker)

        df_data = sym.history(start=start_date, end=end_date, auto_adjust=False)

        metadata = {'source': 'Yahoo',
                    'firstTradeDate': df_data.index[0].strftime("%Y-%m-%d")}

        return df_data, metadata

    def getRawMetadata(self, ticker):
        sym = yf.Ticker(ticker)
        metadata = sym.info
        metadata['isin'] = sym.isin

        return metadata
