import pandas as pd
import requests
import luigi

from pandas import json_normalize
from sqlalchemy import create_engine

ENGINE = create_engine('sqlite:///covid.db')

class GetData(luigi.Task):

    def run(self):
        headers = {
            'accept': 'application/json',
            }
        
        response = requests.get('https://covid19-api.vost.pt/Requests/get_full_dataset_counties', 
        headers=headers)

        data = json_normalize(response.json())
        data.to_csv(self.output().path, index=False)

    def output(self):
        return luigi.LocalTarget('./extract/covid.csv')

class CleanData(luigi.Task):

    def requires(self):
        return GetData()

    def run(self):
        data = pd.read_csv(GetData().output().path)

        (data
        .filter(['data', 'distrito', 'concelho', 'confirmados_1', 'population', 'densidade_populacional'])
        .assign(data = lambda x: pd.to_datetime(x.data, format='%d-%m-%Y'))
        .rename(columns={'confirmados_1': 'confirmados', 'population': 'populacao'})
        .to_csv(self.output().path, index=False)
        )

    def output(self):
        return luigi.LocalTarget('./transform/covid.csv')

class SendToDatabase(luigi.Task):

    _complete = False

    def requires(self):
        return CleanData()

    def run(self):
        data = pd.read_csv(CleanData().output().path)

        (data
        .to_sql('covid', con=ENGINE, if_exists='replace')
        )

        self._complete = True

    def complete(self):
        return self._complete

if __name__ == '__main__':
    luigi.build([SendToDatabase()], local_scheduler=True)