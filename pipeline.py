# :::::::::::::::: IMPORT PACKAGES :::::::::::::::

import pandas as pd
import requests
import luigi
import os

from pandas import json_normalize
from sqlalchemy import create_engine

# :::::::::::: DEFINE GLOBAL VARIABLES :::::::::::

# Get current directory.
BASEDIR = os.getcwd()

# Define Data directory and Database directory.
DATADIR = os.path.join(BASEDIR, "data")
DATABASEDIR = os.path.join(DATADIR, "covid.db")

# Define DB Engine.
ENGINE = create_engine(f"sqlite:///{DATABASEDIR}")

# :::::::::::: THE LUIGI FRAMEWORK ::::::::::::::

# Tasks are defined using Classes (eg. Task GetData)
# The following methods can be used.
#   1. requires - defines the task dependencies.
#   2. run - the main code for the task
#   3. output - defines the output for the task (used in checking for completion)
#   4. Other. (see documentation)

# :::::::::::::::: DATA PIPELINE ::::::::::::::::


class GetData(luigi.Task):
    """Get data from COVID-19 public API"""

    # Gets data from COVID19 API.
    def run(self):
        headers = {
            "accept": "application/json",
        }

        response = requests.get(
            "https://covid19-api.vost.pt/Requests/get_full_dataset_counties",
            headers=headers,
        )

        data = json_normalize(response.json())
        data.to_csv(self.output().path, index=False)

    # The output is stored in 'data/extract/covid.csv'
    def output(self):
        return luigi.LocalTarget(os.path.join(DATADIR, "extract", "covid.csv"))


# Gets data from previous step and applies the cleanning steps.


class CleanData(luigi.Task):
    """Data Cleaning"""

    # Requires data from previous step.
    def requires(self):
        return GetData()

    # Cleans data.
    def run(self):
        data = pd.read_csv(GetData().output().path)

        (
            data.filter(
                [
                    "data",
                    "distrito",
                    "concelho",
                    "confirmados_1",
                    "population",
                    "densidade_populacional",
                ]
            )
            .assign(data=lambda x: pd.to_datetime(x.data, format="%d-%m-%Y"))
            .rename(columns={"confirmados_1": "confirmados", "population": "populacao"})
            .to_csv(self.output().path, index=False)
        )

    # The output is stored in 'data/transform/covid.csv'
    def output(self):
        return luigi.LocalTarget(os.path.join(DATADIR, "transform", "covid.csv"))


class SendToDatabase(luigi.Task):
    """Updates information on the Database"""

    _complete = False

    # Requires data from previous step.
    def requires(self):
        return CleanData()

    # Updates database.
    def run(self):
        data = pd.read_csv(CleanData().output().path)

        (data.to_sql("covid", con=ENGINE, if_exists="replace"))

        self._complete = True

    # Overwrites complete function.
    def complete(self):
        return self._complete


# ::::::::::::::: HANDLE FAILURE :::::::::::::::


@luigi.Task.event_handler(luigi.Event.FAILURE)
def handle_failure(task, exception):
    """Called directly after an failed execution"""

    # Defines custom code for handling Failure.
    with open(os.path.join(DATADIR, "failure.text"), "a") as f:
        f.write(f"Task failed: {task}. Due to {exception}.\n")


if __name__ == "__main__":
    luigi.build([SendToDatabase()], local_scheduler=True)
