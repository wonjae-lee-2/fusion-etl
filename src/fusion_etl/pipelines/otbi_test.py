from fusion_etl.connectors import otbi
from fusion_etl.utils import Secrets

if __name__ == "__main__":
    secrets = Secrets("secrets.json")
    report_path = "/users/leew@unhcr.org/01 TA budget check/ABOD 2023 - analysis"

    connector = otbi.Connector(secrets)
    connector.download(report_path, headless_flag=False)
