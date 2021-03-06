# -*- coding: UTF-8 -*-
from __future__ import absolute_import
import os
import re
import logging
import argparse

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io import BigQuerySource, BigQuerySink, WriteToText, WriteToAvro

from dataflow_etl.utils import env
from dataflow_etl.data.BQuery import get_query
from dataflow_etl.transform.format import FormatAsCSV
from dataflow_etl.transform.project import ProjectionBQ
from dataflow_etl.io.schema import create_schema

PROJECT_FIELDS_USER = env.PROJECT_FIELDS["USER"]
HEADERS = "\x14".join(env.COLUMNS["USER"])
COLUMNS = env.COLUMNS["USER"]
SCHEMA = create_schema(COLUMNS)


def run(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", required=True, type=str, help="")
    parser.add_argument("--output", required=True, type=str, help="PROJECT:DATASET.TABLE")
    known_args, pipeline_args = parser.parse_known_args(argv)

    # file_path = os.path.join(known_args.output, known_args.date, "user", known_args.date+"-user")
    table_name = known_args.output + "$" + re.sub("-", "", known_args.date)

    pipeline_options = PipelineOptions(pipeline_args)

    with beam.Pipeline(options=pipeline_options) as p:
        query_of_user = get_query('user', date=known_args.date)

        # CSV FORMAT
        # result = (p
        # | "ReadFromBQ" >> beam.io.Read(BigQuerySource(query=query_of_user, use_standard_sql=True))
        # | "Projected" >> beam.ParDo(ProjectionBQ(), PROJECT_FIELDS_USER)
        # | "Format" >> beam.ParDo(FormatAsCSV(), COLUMNS, False)
        # | "Write" >> WriteToText(file_path, ".csv", shard_name_template="-SS", header=HEADERS))

        # BIGQUERY FORMAT
        result = (p
        | "ReadFromBQ" >> beam.io.Read(BigQuerySource(query=query_of_user, use_standard_sql=True))
        | "Projected" >> beam.ParDo(ProjectionBQ(), PROJECT_FIELDS_USER)
        | "Write" >> beam.io.Write(
                    BigQuerySink(
                        table_name,
                        schema=SCHEMA,
                        create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                        write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND)))
        # | "Write" >> WriteToAvro(file_path, schema=SCHEMA, use_fastavro=True, shard_name_template="-SS"))


if __name__ == "__main__":
    logger = logging.getLogger()
    logging.basicConfig(format="%(asctime)s - %(levelname)s - %(message)s",
                        datefmt="%m-%d %H:%M:%S")
    logging.root.setLevel(logging.INFO)
    run()