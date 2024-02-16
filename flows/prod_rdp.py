from prefect import flow, task

from fusion_etl.connectors import fusion, rdp
from fusion_etl import utils


@flow
def prod_rdp(
    credentials_path: str = "config/credentials.json",
    etl_mappings_path: str = "config/prod_rdp.json",
    headless_flag: bool = True,
):
    # _ = input("Connect to VPN and press enter\n")
    credentials = utils.Credentials(credentials_path)
    etl_mappings = utils.read_etl_mappings(etl_mappings_path)
    rdp_connector = rdp.Connector(credentials, headless_flag=headless_flag)
    rdp_connector.open_conn()
    fusion_connector = fusion.Connector(credentials)
    fusion_connector.open_conn()
    copy_rdp(rdp_connector, fusion_connector, etl_mappings)
    rdp_connector.close_conn()
    fusion_connector.close_conn()


@task
def copy_rdp(
    rdp_connector: rdp.Connector,
    fusion_connector: fusion.Connector,
    etl_mappings: list[dict[str, str]],
):
    for etl_mapping in etl_mappings:
        (column_names, rows) = rdp_connector.query(etl_mapping)
        fusion_connector.insert_rows(etl_mapping, column_names, rows)


if __name__ == "__main__":
    prod_rdp()
