import csv

from fusion_etl.connectors import sharepoint
from fusion_etl.utils import Secrets

if __name__ == "__main__":
    secrets = Secrets("secrets.json")
    folder = "/BI Team/04. Processes/Fusion ETL"
    filename = "cost_center_mapping.csv"

    connector = sharepoint.Connector(secrets)
    filename = connector.download(folder, filename, headless_flag=False)

    with open(filename, encoding="utf-8") as f:
        reader = csv.reader(f)
        for row in reader:
            print(row)
