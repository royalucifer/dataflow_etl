# -*- coding: UTF-8 -*-
from __future__ import absolute_import
import os
import logging
import argparse

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io import BigQuerySource, WriteToText

from dataflow_etl.utils import env
from dataflow_etl.data.BQuery import get_query
from dataflow_etl.transform.format import FormatAsCSV
from dataflow_etl.transform.project import ProjectionBQ

PROJECT_FIELDS_USER = env.PROJECT_FIELDS["USER"]
HEADERS = "\x14".join(env.COLUMNS["USER"])
COLUMNS = env.COLUMNS["USER"]


def run(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", required=True, type=str, help="")
    parser.add_argument("--output", required=True, type=str, help="")
    known_args, pipeline_args = parser.parse_known_args(argv)

    file_path = os.path.join(known_args.output, known_args.date, "user", known_args.date+"-user")

    pipeline_options = PipelineOptions(pipeline_args)

    with beam.Pipeline(options=pipeline_options) as p:
        query_of_user = get_query('user', date=known_args.date)

        result = (p
        | "ReadFromBQ" >> beam.io.Read(BigQuerySource(query=query_of_user, use_standard_sql=True))
        | "Projected" >> beam.ParDo(ProjectionBQ(), PROJECT_FIELDS_USER)
        | "Format" >> beam.ParDo(FormatAsCSV(), COLUMNS, False)
        | "Write" >> WriteToText(file_path, ".csv", shard_name_template="-SS", header=HEADERS))


if __name__ == "__main__":
    logger = logging.getLogger()
    logging.basicConfig(format="%(asctime)s - %(levelname)s - %(message)s",
                        datefmt="%m-%d %H:%M:%S")
    logging.root.setLevel(logging.INFO)
    run()