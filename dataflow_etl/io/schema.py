# -*- coding: UTF-8 -*-
__all__ = ["create_schema"]

from apache_beam.io.gcp.internal.clients import bigquery


def create_schema(fields):
    table_schema = bigquery.TableSchema()
    for field in fields:
        tmp_schema = bigquery.TableFieldSchema()
        tmp_schema.name = field
        tmp_schema.mode = "nullable"

        if field == "cookies":
            tmp_schema.type = "STRING"
        elif field == "visitDate":
            tmp_schema.type = "DATE"
        else:
            tmp_schema.type = "INTEGER"

        table_schema.fields.append(tmp_schema)
    return table_schema
