import struct
from datetime import datetime, timedelta, timezone

import pyodbc
from dagster import ConfigurableResource


class MSRPResource(ConfigurableResource):
    odbc_driver: str
    msrp_server: str
    msrp_database: str
    msrp_login: str
    msrp_password: str

    def connect(self) -> pyodbc.Connection:
        conn_str = f"""
                Driver={self.odbc_driver};
                Server={self.msrp_server};
                Database={self.msrp_database};
                UID={self.msrp_login};
                PWD={self.msrp_password};
            """
        conn = pyodbc.connect(conn_str)
        conn.add_output_converter(
            pyodbc.SQL_BIT,
            lambda x: int.from_bytes(
                x,
                byteorder="big",
            ),
        )
        conn.add_output_converter(
            -155,
            self._handle_datetimeoffset,
        )
        return conn

    def _handle_datetimeoffset(self, dto_value):
        # ref: https://github.com/mkleehammer/pyodbc/issues/134#issuecomment-281739794
        tup = struct.unpack(
            "<6hI2h", dto_value
        )  # e.g., (2017, 3, 16, 10, 35, 18, 500000000, -6, 0)
        return datetime(
            tup[0],
            tup[1],
            tup[2],
            tup[3],
            tup[4],
            tup[5],
            tup[6] // 1000,
            timezone(timedelta(hours=tup[7], minutes=tup[8])),
        )
