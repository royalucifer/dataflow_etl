import re

def read_sql_file(file, *args, **kwargs):
    if not file:
        raise ValueError("You must give the directory of file")

    with open(file, 'r') as f:
        sql = "".join(f.readlines())
    sql = re.sub("\ufeff", "", sql).format(*args, **kwargs)

    return sql