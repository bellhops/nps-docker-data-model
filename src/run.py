import os
import sys
import logging
import json
import math
import urllib3
import pandas as pd

from argparse import ArgumentParser
from sqlalchemy import create_engine
from pandas.io.json import json_normalize

stdout_handler = logging.StreamHandler(sys.stdout)
logger = logging.getLogger(name="bellhops-data")
logger.addHandler(stdout_handler)
logger.setLevel(logging.DEBUG)


class PromoterScraper(object):

    def __init__(self, promoter_url, headers, gospel_db_url, destination_schema_name, destination_table_name):
        self.promoter_url = promoter_url
        self.headers = headers
        self.gospel_db_url = gospel_db_url
        self.destination_schema_name = destination_schema_name
        self.destination_table_name = destination_table_name

        self.feedback_length = self.get_total_count()
        self.total_number_of_pages = int(math.ceil(float(self.feedback_length)/100))

        self.columns = [
            'order_id',
            'order_number',
            'contact_email',
            'score',
            'score_type',
            'posted_date',
            'comment',
            'campaign'
        ]
        self.feedback = pd.DataFrame(columns=self.columns)
        self.get_feedback()

    def get_promoter_data(self, url):
        http = urllib3.PoolManager(cert_reqs='CERT_REQUIRED')
        response = http.request('GET', url, headers=headers)
        data = json.loads(str(response.data.decode("utf-8")))
        self.validate(response, data)
        return data

    def validate(self, response, data):
        if response.status != 200:
            raise Exception("Response status code: {status}".format(status=response.status))
        if 'count' not in data or 'results' not in data:
            raise Exception("Invalid keys in data: {keys}".format(keys=data.keys()))

    def get_total_count(self):
        url = self.promoter_url + str(1)
        logger.info("Getting count with url: "+url)
        data = self.get_promoter_data(url)
        return data['count']

    def clean_columns(self):
        column_names = list(self.feedback.columns)
        clean_column_names = []
        for column_name in column_names:
            column_name = column_name.replace('.','_')
            column_name = column_name.replace(' ','_')
            column_name = column_name.lower()
            column_name = column_name.replace('contact_attributes_','')
            clean_column_names.append(column_name)
        logger.info("Formatting column names: {clean_column_names}".format(clean_column_names=clean_column_names))
        return clean_column_names

    @staticmethod
    def text_replace(comment):
        # Functions for removing non-ASCII characters and null characters
        if comment is None:
            return None
        return ''.join([i if ord(i) < 128 else ' ' for i in comment])

    @staticmethod
    def null_character_replace(comment):
        if comment is None:
            return None
        return comment.replace('\x00', '')

    def get_feedback(self):

        for page_number in range(1, self.total_number_of_pages + 1):
            url = promoter_url + str(page_number)
            logger.info("Getting feedback from url: " + url)
            data = self.get_promoter_data(url)
            page_df = json_normalize(data['results'])
            if 'contact.attributes.Order Id' in page_df.columns and 'contact.attributes.order id' in page_df.columns:
                order_id = []
                for index, row in page_df.iterrows():
                    if not pd.isnull(row['contact.attributes.Order Id']):
                        order_id.append(row['contact.attributes.Order Id'])
                    else:
                        order_id.append(row['contact.attributes.order id'])
                page_df['order_id'] = order_id
                page_df.drop(['contact.attributes.order id', 'contact.attributes.Order Id'], axis=1, inplace=True)
            elif 'contact.attributes.Order Id' in page_df.columns:
                page_df.rename(columns={'contact.attributes.Order Id': 'order_id'}, inplace=True)
            elif 'contact.attributes.order id' in page_df.columns:
                page_df.rename(columns={'contact.attributes.order id': 'order_id'}, inplace=True)

            available_columns = [column for column in self.columns if column in page_df.columns]
            cleaned_df = page_df[available_columns]
            if len(self.feedback) == 0:
                logger.info("Storing first page results: {page_number}".format(page_number=page_number))
                self.feedback = self.feedback.append(cleaned_df)
                logger.info("Results in first page: {count}".format(count=len(self.feedback)))
            else:
                logger.info("Appending page results: {page_number}".format(page_number=page_number))
                self.feedback = self.feedback.append(cleaned_df)
                logger.info("Results in page: {page_number} count:{count}".format(page_number=page_number,
                                                                            count=len(self.feedback)))

        self.feedback.columns = self.clean_columns()
        self.feedback['comment'] = self.feedback.comment.apply(lambda x: self.text_replace(self.null_character_replace(x)))

    def store_feedback(self):
        gospel_conn = create_engine(self.gospel_db_url)
        logger.info("Storing in schema: {schema_name} table: {table_name}".format(schema_name=self.destination_schema_name,
                                                                                  table_name=self.destination_table_name))
        self.feedback.to_sql(self.destination_table_name, gospel_conn, schema=self.destination_schema_name, if_exists='replace')


def get_args():
    parser = ArgumentParser()
    parser.add_argument("--token", help="Promoter token", type=str)
    parser.add_argument("--destination_schema_name", help="Destination schema name", type=str)
    parser.add_argument("--destination_table_name", help="Destination table name", type=str)
    args = parser.parse_args()
    return args


if __name__ == "__main__":

    gospel_db_url = os.environ['GOSPEL_DB_URL']

    args = get_args()
    token = str(args.token)
    destination_schema_name = str(args.destination_schema_name)
    destination_table_name = str(args.destination_table_name)

    promoter_url = 'https://app.promoter.io/api/feedback/?posted_date_0=2016-08-01&page='

    headers = {
      'Authorization': "Token " + token,
      'Content-Type': 'application/json'
    }

    promoter_scraper = PromoterScraper(
        promoter_url=promoter_url,
        headers=headers,
        gospel_db_url=gospel_db_url,
        destination_schema_name=destination_schema_name,
        destination_table_name=destination_table_name
    )

    logger.info("Total number of feedbacks scraped: {count}".format(count=len(promoter_scraper.feedback)))

    promoter_scraper.store_feedback()