import os
import logging
import pandas as pd

from .etl_from_api_endpoint import BaseETL

logging.getLogger().setLevel(logging.INFO)

## NOTE: move the key to .env and os.environ.get
newsapi_key = os.environ.get("NEWS_API_KEYS")

# Inherited from ETL general class -- now we extract and transform the data getting from NewsAPI
class NewsETL(BaseETL):
    def __init__(self):
        self.df_headlines = None
        self.news_api = f"https://newsapi.org/v2/top-headlines?sources=bloomberg,business-insider,fortune,the-wall-street-journal&pageSize=100&apiKey={newsapi_key}"

    def transform_news_headlines(self):
        self.headlines = self.response['articles']

        for headline in self.headlines:
            # drop the unnecessary key-value pairs (but they may/may not have such keys)
            try:
                del headline['author'], headline['content'], headline['description'], headline['urlToImage']
            except KeyError:
                pass

            # update the source name
            source_name = {'source': headline['source']['name']}
            headline.update(source_name)

        self.df_headlines = pd.DataFrame(self.headlines)

        # rename the column that with capital letters
        self.df_headlines = self.df_headlines.rename(columns={'publishedAt': 'publish_time', 'source': 'publisher'})

        # make the time code consistent **(write a to_datetime function?)**
        self.df_headlines['publish_time'] = pd.to_datetime(self.df_headlines['publish_time'])

        # add a column to show the news is from NewsAPI
        self.df_headlines['news_feed_from'] = 'NewsAPI Business'

        # re-arrange the columns
        self.df_headlines = self.df_headlines[['publish_time', 'news_feed_from', 'publisher', 'title', 'url']]

        return self.df_headlines

    def run(self):
        self.extract_from_api_endpoint(self.news_api)
        self.transform_news_headlines()
        return self.df_headlines


#news = NewsETL().run()
# news.to_csv(f"data-collection-{today_date}/newsapi_headlines_{time_created}.csv", index=False)
# logging.info(f"newsapi_headlines_{time_created}.csv is successfully generated.")