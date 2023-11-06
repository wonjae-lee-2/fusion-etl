from fusion_etl.connectors import otbi

report_path = "/users/leew@unhcr.org/01 TA budget check/ABOD 2023 - analysis"
download_format = "csv"

connector = otbi.Connector()
connector.download(report_path, download_format)
