import requests
import logging

logging.getLogger().setLevel(logging.INFO)

# Base class for ETL from API Endpoint
class BaseETL:
    def __init__(self):
        pass

    def extract_from_api_endpoint(self, target_api: str):
        try:
            self.response = requests.get(target_api).json()
            logging.info(f"Successfully extracted the news data from NewsAPI, starting data transformation...")
        except requests.exceptions.HTTPError as error:
            raise SystemExit(error)

    def load(self):
        pass