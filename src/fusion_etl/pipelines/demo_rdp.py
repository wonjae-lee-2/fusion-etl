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
            case "cost_center_mapping.csv":
                df = self._transform_cost_center_mapping(etl_mapping["filename"])
                return df
            case "Situation Allocation Ratios 2024":
                df = self._transform_Situation_Allocation_Ratios_2024(
                    etl_mapping["filename"]
                )
                return df

    def _transform_cost_center_mapping(self, filename: str) -> pl.DataFrame:
        df = pl.read_csv(filename, infer_schema_length=0).with_columns(
            pl.col("Modified").str.to_datetime("%d/%m/%Y %H:%M")
        )
        return df

    def _transform_Situation_Allocation_Ratios_2024(
        self, filename: str
    ) -> pl.DataFrame:
        df = pl.read_csv(filename, infer_schema_length=0).with_columns(
            pl.col("Allocation Ratio").cast(pl.Float64)
        )
        return df


if __name__ == "__main__":
    etl_mappings_path = "config/demo_rdp.json"
    unhcr_network_flag = False

    pipeline = Pipeline(
        etl_mappings_path,
        unhcr_network_flag,
    )
    pipeline.run()
