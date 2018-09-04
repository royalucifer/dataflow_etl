__all__ = ["get_query"]

import os
import re

PKG_DIR, _ = os.path.split(__file__)
SQL_DIR = os.path.join(PKG_DIR, "SQL")
FILE_PATH = {
    "channel": os.path.join(SQL_DIR, "Channel.sql"),
    "all": os.path.join(SQL_DIR, "All.sql"),
    "user": os.path.join(SQL_DIR, "User.sql"),
}


def get_query(data_type, *args, **kwargs):
    source = data_type.lower()

    if source not in ("channel", "all", "user"):
        raise ValueError("You give wrong value [{}]".format(source))

    return _read_sql_file(FILE_PATH[source], *args, **kwargs)


def _read_sql_file(file, *args, **kwargs):
    if not file:
        raise ValueError("You must give the directory of file")

    with open(file, 'r') as f:
        sql = "".join(f.readlines())
    sql = re.sub("\ufeff", "", sql).format(*args, **kwargs)

    return sql