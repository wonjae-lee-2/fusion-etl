import polars as pl
from fusion_etl import utils
from fusion_etl.connectors import erp, fusion
from prefect import flow, task


@flow
def otbi_ppm(
    credentials_path: str = "config/credentials.json",
    etl_mappings_path: str = "config/otbi_ppm.json",
    headless_flag: bool = True,
):
    credentials = utils.Credentials(credentials_path)
    etl_mappings = utils.read_etl_mappings(etl_mappings_path)
    otbi_connector = erp.Connector(credentials, headless_flag=headless_flag)
    otbi_connector.download(etl_mappings)
    pass

"""
class Pipeline(BasePipeline):
    def __init__(
        self,
        etl_mappings_path: str,
        unhcr_network_flag: bool,
    ):
        super().__init__(
            etl_mappings_path,
            unhcr_network_flag,
        )

    def _get_df(self, etl_mapping: dict[str, str]) -> pl.DataFrame:
        match etl_mapping["source_name"]:
            case "Test_BC_Balances_2023":
                df = self._transform_Test_BC_Balances_2023(etl_mapping["filename"])
                return df
            case "Test_PPM_Costs_2023":
                df = self._transform_Test_PPM_Costs_2023(etl_mapping["filename"])
                return df

    def _transform_Test_BC_Balances_2023(self, filename: str) -> pl.DataFrame:
        df = (
            pl.read_csv(filename, infer_schema_length=0)
            .with_columns(pl.col("Total Budget").cast(pl.Float64))
            .with_columns(pl.col("Expenditures").cast(pl.Float64))
            .with_columns(pl.col("Obligations").cast(pl.Float64))
            .with_columns(pl.col("Commitments").cast(pl.Float64))
            .with_columns(pl.col("Other Anticipated Expenditures").cast(pl.Float64))
        )
        return df

    def _transform_Test_PPM_Costs_2023(self, filename: str) -> pl.DataFrame:
        df = pl.read_csv(filename, infer_schema_length=0).with_columns(
            pl.col("Cost").cast(pl.Float64)
        )
        return df
"""

if __name__ == "__main__":
    otbi_ppm()