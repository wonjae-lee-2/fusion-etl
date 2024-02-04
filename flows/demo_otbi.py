import polars as pl

from fusion_etl.utils import BasePipeline


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


if __name__ == "__main__":
    etl_mappings_path = "config/demo_otbi.json"
    unhcr_network_flag = False

    pipeline = Pipeline(
        etl_mappings_path,
        unhcr_network_flag,
    )
    pipeline.run()
