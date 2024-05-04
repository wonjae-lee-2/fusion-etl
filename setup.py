from setuptools import find_packages, setup

setup(
    name="fusion_etl",
    packages=find_packages(exclude=["fusion_etl_tests"]),
    install_requires=[
        "dagster",
        "dagster-webserver",
        "dagster-dbt",
        "pyodbc",
        "playwright",
        "azure-storage-blob",
        "pyarrow",
    ],
    extras_require={"dev": ["pytest", "ruff"]},
)
