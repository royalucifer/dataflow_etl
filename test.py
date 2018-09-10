# -*- coding: UTF-8 -*-
from __future__ import absolute_import
import logging
import argparse

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io.gcp.bigquery import BigQuerySource

from dataflow_etl.utils.env import CHANNEL_LISTS, PROJECT_FIELDS
from dataflow_etl.data.BQuery import get_query


class ZeroImputation(beam.DoFn):
    def process(self, element, channel):
        ch = element["channel"]
        val = element["totalPageviews"] if ch == channel else 0
        uid = element["cookies"]
        yield (uid, val)


def combine_channels(input_data):
    def pivot(channel):
        return (
                input_data
                | "FilterByChannel" >> beam.ParDo(ZeroImputation(), channel)
                | "SumByUser" >> beam.CombinePerKey(sum)
        )
    return {ch: pivot(ch) for ch in CHANNEL_LISTS}


def run(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", required=True, type=str, help="")
    # parser.add_argument("--output", required=True, type="", help="")
    known_args, pipeline_args = parser.parse_known_args(argv)

    pipeline_options = PipelineOptions(pipeline_args)

    with beam.Pipeline(options=pipeline_options) as p:
        QUERY = get_query('channel', date=known_args.date)

        init_ch = (p
        | "ReadFromBQ" >> beam.io.Read(BigQuerySource(query=QUERY, use_standard_sql=True))
        | "Projected" >> beam.Map(lambda row: {f: row[f] for f in PROJECT_FIELDS})
        | "FilterByChannel" >> beam.ParDo(ZeroImputation(), u"運動")
        | "SumByUser" >> beam.CombinePerKey(sum))

        # result = (combine_channels(init_ch)
        #           | "Join" >> beam.CoGroupByKey())
                  # | "Output" >> beam.FlatMap())


if __name__ == "__main__":
    logger = logging.getLogger()
    logging.basicConfig(format="%(asctime)s - %(levelname)s - %(message)s",
                        datefmt="%m-%d %H:%M:%S")
    logging.root.setLevel(logging.INFO)
    run()