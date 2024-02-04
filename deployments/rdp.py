from prefect import flow, task

from fusion_etl.connectors import fusion, rdp
from fusion_etl import utils


@flow
def pipeline(
    credentials_path: str,
    etl_mappings_path: str,
    headless_flag: bool = True,
):
    credentials = utils.Credentials(credentials_path)
    etl_mappings = utils.read_etl_mappings(etl_mappings_path)
    rdp_connector = rdp.Connector(credentials, headless_flag=headless_flag)
    rdp_connector.open_conn()
    fusion_connector = fusion.Connector(credentials)
    fusion_connector.open_conn()
    for etl_mapping in etl_mappings:
        copy_table(rdp_connector, fusion_connector, etl_mapping)
    rdp_connector.close_conn()
    fusion_connector.close_conn()


@task
def copy_table(
    rdp_connector: rdp.Connector,
    fusion_connector: fusion.Connector,
    etl_mapping: dict[str, str],
):
    (column_names, rows) = rdp_connector.query(etl_mapping)
    fusion_connector.insert_rows(etl_mapping, column_names, rows)


if __name__ == "__main__":
    credentials_path = "config/credentials.json"
    etl_mappings_path = "config/rdp.json"
    pipeline(credentials_path, etl_mappings_path)
