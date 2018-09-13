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

PROJECT_FIELDS = env.PROJECT_FIELDS
CHANNEL_LISTS = env.CHANNEL_LISTS
HEADERS = env.HEADERS

# class ZeroImputation(beam.DoFn):
#     def process(self, element, field):
#         ch = element["channel"]
#         val = element["totalPageviews"] if ch == field else 0
#         uid = element["cookies"]
#         yield (uid, val)
#
#
# def CombineChPColl(input_data):
#     def pivot(field):
#         fname = field.title()
#         filter_step_name = "FilterByChannel{}".format(fname)
#         sum_step_name = "SumByUser{}".format(fname)
#         return (
#                 input_data
#                 | filter_step_name >> beam.ParDo(ZeroImputation(), field)
#                 | sum_step_name >> beam.CombinePerKey(sum)
#         )
#     return {ch: pivot(ch) for ch in CHANNEL_LISTS}


def CombineChPCollTest(input_data):
    def pivot(field):
        fname = field.title()
        filter_step_name = "FilterByChannel{}".format(fname)
        project_step_name = "KeyValueProject{}".format(fname)
        sum_step_name = "SumByUser{}".format(fname)
        return (
                input_data
                | filter_step_name >> beam.Filter(lambda row: row["channel"] == field)
                | project_step_name >> beam.Map(lambda row: (row["cookies"], row["totalPageviews"]))
                | sum_step_name >> beam.CombinePerKey(sum)
        )
    return {ch: pivot(ch) for ch in CHANNEL_LISTS}


def FormatAsCSV(element):

    def flatten_dict(records):
        result = dict()
        for k, v in records.items():
            if isinstance(v, dict):
                result.update(flatten_dict(v))
            else:
                if len(v) == 0:
                    v = 0
                elif len(v) == 1:
                    v = v[0]
                result[k] = v
        return result

    key, val = element
    records_list = ['"%s"' % v for k, v in flatten_dict(val).items()]
    records_list.insert(0, key)
    return "\x14".join(records_list)


class Projection(beam.DoFn):
    def process(self, element, fileds):
        project_dict = {f: element[f] for f in fileds}
        yield project_dict


def run(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", required=True, type=str, help="")
    parser.add_argument("--output", required=True, type=str, help="")
    known_args, pipeline_args = parser.parse_known_args(argv)

    FILE_NAME = ".csv"
    FILE_PATH = os.path.join(known_args.output, "{}-view"+known_args.date)

    pipeline_options = PipelineOptions(pipeline_args)

    with beam.Pipeline(options=pipeline_options) as p:
        QUERY = get_query('channel', date=known_args.date)

        init_ch = (p
        | "ReadFromBQ" >> beam.io.Read(BigQuerySource(query=QUERY, use_standard_sql=True))
        | "Projected" >> beam.ParDo(Projection(), PROJECT_FIELDS))

        result = (CombineChPCollTest(init_ch)
                  | "Join" >> beam.CoGroupByKey()
                  | "Format" >> beam.Map(FormatAsCSV)
                  | "Write" >> WriteToText(FILE_PATH, FILE_NAME, header=HEADERS))


if __name__ == "__main__":
    logger = logging.getLogger()
    logging.basicConfig(format="%(asctime)s - %(levelname)s - %(message)s",
                        datefmt="%m-%d %H:%M:%S")
    logging.root.setLevel(logging.INFO)
    run()