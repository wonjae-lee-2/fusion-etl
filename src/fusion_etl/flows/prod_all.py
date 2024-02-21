from prefect import flow

from fusion_etl.flows import prod_erp, prod_rdp, prod_sp


@flow(log_prints=True)
def prod_all(headless_flag: bool = False):
    prod_erp.prod_erp(headless_flag=headless_flag)
    prod_rdp.prod_rdp(headless_flag=headless_flag)
    prod_sp.prod_sp(headless_flag=headless_flag)
