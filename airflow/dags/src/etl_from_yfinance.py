import pandas as pd
import yfinance as yf
import logging

logging.getLogger().setLevel(logging.INFO)

class StockSpecificETL:
    def __init__(self, ticker_list):
        self.ticker_list = ticker_list
        self.df_ticker_news = None
        self.df_ticker_recommendations = None

    # extract and transform the news data captured in yF
    @staticmethod
    def extract_ticker_news(ticker):
        ticker_news = yf.Ticker(ticker).news
        for news in ticker_news:
            try:
                del news['uuid'], news['type']
            except KeyError:
                pass
            news.update({'news_feed_from': 'YF Stock - ' + ticker})
        return ticker_news

    # if multiple tickers: [ [{},{},{},{},{},{}], [{},{},{},{},{},{}], [{},{},{},{},{},{}] ] => depends on how many tickers (3 tickers in this case)
    # target data structures: [ {},{},{},{} ]
    def get_ticker_news(self):
        news_mapping = map(self.extract_ticker_news, self.ticker_list)
        return [each_news for sublist in list(news_mapping) for each_news in sublist]

    def transform_ticker_news(self):
        logging.info(f"Successfully extracted the news data from yFinance, starting data transformation...")
        self.df_ticker_news = pd.DataFrame(self.get_ticker_news())

        # rename the column that with capital letters/inconsistent with NewsAPI data
        self.df_ticker_news = self.df_ticker_news.rename(columns={'providerPublishTime': 'publish_time', 'link': 'url'})

        # convert to datetime with utc
        self.df_ticker_news['publish_time'] = pd.to_datetime(self.df_ticker_news['publish_time'], unit='s', utc=True)

        # re-arrange the columns
        self.df_ticker_news = self.df_ticker_news[['publish_time', 'news_feed_from', 'publisher', 'title', 'url']]

        return self.df_ticker_news

    def run_news(self):
        self.get_ticker_news()
        self.transform_ticker_news()
        return self.df_ticker_news

    @staticmethod
    def extract_ticker_recommendataions(ticker: str):
        # already a Pandas Dataframe
        ticker_recommendations = yf.Ticker(ticker).recommendations
        ticker_recommendations['ticker'] = ticker
        return ticker_recommendations

    def get_ticker_recommendations(self):
        # collect all ticker recommendations in a list of dataframe
        self.df_ticker_recommendations = pd.concat(
            [self.extract_ticker_recommendataions(ticker) for ticker in self.ticker_list])
        return self.df_ticker_recommendations

    def transform_ticker_recommendations(self):
        # already a dataframe
        self.df_ticker_recommendations.reset_index(inplace=True)

        # rename the column that with capital letters/inconsistent
        self.df_ticker_recommendations = self.df_ticker_recommendations.rename(columns={'Date': 'publish_time',
                                                                                        'Firm': 'firm',
                                                                                        'To Grade': 'to_grade',
                                                                                        'From Grade': 'from_grade',
                                                                                        'Action': 'action'})

        # convert to datetime
        self.df_ticker_recommendations['publish_time'] = pd.to_datetime(self.df_ticker_recommendations['publish_time'],
                                                                        utc=True)

        # convert empty str to None
        self.df_ticker_recommendations.loc[self.df_ticker_recommendations['from_grade'] == '', 'from_grade'] = None

        # re-arrange the columns
        self.df_ticker_recommendations = self.df_ticker_recommendations[
            ['publish_time', 'ticker', 'firm', 'to_grade', 'from_grade', 'action']]

        return self.df_ticker_recommendations

    def run_recommendations(self):
        self.get_ticker_recommendations()
        self.transform_ticker_recommendations()
        return self.df_ticker_recommendations


#news = StockSpecificETL(ticker_list).run_news()
#news.to_csv(f"data-collection-{today_date}/stock_specific_headlines_{time_created}.csv", index=False)
#logging.info(f"stock_specific_headlines_{time_created}.csv is successfully generated.")

#recommendations = StockSpecificETL(ticker_list).run_recommendations()
#recommendations.to_csv(f"data-collection-{today_date}/stock_recommendations_{time_created}.csv", index=False)
#logging.info(f"stock_recommendations_{time_created}.csv is successfully generated.")