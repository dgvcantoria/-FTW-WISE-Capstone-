# dlt/pipeline.py
import dlt, pandas as pd
import os


def file_path(filename):
    ROOT_DIR = os.path.dirname(__file__)
    return os.path.join(ROOT_DIR, "staging", "psacensus2020f3", filename)

@dlt.resource(name="car_abra")
def car_abra():
    yield pd.read_csv(file_path("CPH PUF 2020 Abra - HOUSEHOLD.CSV.csv"))

def run():
    p = dlt.pipeline(
        pipeline_name="02-dlt-capstone-pipeline",
        destination="clickhouse",
        dataset_name="psa_census_2020_f3",
    )
    print("Fetching and loading...")
    info1 = p.run(car_abra())          # dlt pulls creds from env-vars

    print("records loaded:", info1)


if __name__ == "__main__":
    run()