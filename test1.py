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

PROJECT_FIELDS_USER = env.PROJECT_FIELDS["USER"]
HEADERS = "\x14".join(env.COLUMNS["USER"])
COLUMNS = env.COLUMNS["USER"]

class FormatAsCSV(beam.DoFn):

    def _flatten_dict(self, records):
        result = dict()
        for k, v in records.items():
            if isinstance(v, dict):
                result.update(self._flatten_dict(v))
            elif isinstance(v, (int, str)):
                result[k] = v if v else 0
            elif isinstance(v[0], dict):
                result.update(self._flatten_dict(v[0]))
            else:
                if len(v) == 0:
                    v = 0
                elif len(v) == 1:
                    v = v[0] if v[0] else 0
                else:
                    v = ",".join([str(x) for x in v])
                result[k] = v
        return result

    def process(self, element, columns, is_kv=True):
        if is_kv:
            key, val = element
            records_list = [key]
        else:
            val = element
            records_list = []

        flat_val = self._flatten_dict(val)
        records_list.extend(["%s" % flat_val[h] for h in columns])

        result = "\x14".join(records_list)
        yield result


class ProjectionUser(beam.DoFn):
    def process(self, element, fileds):
        project_dict = {f: element[f] for f in fileds}
        yield project_dict


def run(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", required=True, type=str, help="")
    parser.add_argument("--output", required=True, type=str, help="")
    known_args, pipeline_args = parser.parse_known_args(argv)

    file_path = os.path.join(known_args.output, known_args.date, known_args.date+"-user")

    pipeline_options = PipelineOptions(pipeline_args)

    with beam.Pipeline(options=pipeline_options) as p:
        query_of_user = get_query('user', date=known_args.date)

        result = (p
        | "ReadFromBQ" >> beam.io.Read(BigQuerySource(query=query_of_user, use_standard_sql=True))
        | "Projected" >> beam.ParDo(ProjectionUser(), PROJECT_FIELDS_USER)
        | "Format" >> beam.ParDo(FormatAsCSV(), COLUMNS, False)
        | "Write" >> WriteToText(file_path, ".csv", header=HEADERS))


if __name__ == "__main__":
    logger = logging.getLogger()
    logging.basicConfig(format="%(asctime)s - %(levelname)s - %(message)s",
                        datefmt="%m-%d %H:%M:%S")
    logging.root.setLevel(logging.INFO)
    run()