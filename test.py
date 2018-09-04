from __future__ import absolute_import
import logging
import argparse

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io.gcp.bigquery import BigquerySource

from dataflow_etl.utils.env import CHANNEL_LISTS
from dataflow_etl.data.BQuery import get_query


class ZeroImputation(beam.DoFn):
    def process(self, element, channel):
        (uid, (ch, val)) = element
        new_val = val if ch == channel else 0
        return (uid, new_val)


def combine_channels(input_data):
    def pivot(channel):
        return (
                input_data
                | "FilterByChannel" >> beam.PaDo(ZeroImputation(), channel)
                | "SumByUser" >> beam.CombinePerKey(sum)
        )
    return {ch: pivot(ch) for ch in CHANNEL_LISTS}


def projected_channel(row):
    PROJECT_FIELDS = ["channel", "totalPageviews"]
    key = row['cookies']
    val = (row[idx] for idx in PROJECT_FIELDS)
    return (key, val)


def run(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", required=True, type=str, help="")
    # parser.add_argument("--output", required=True, type="", help="")
    known_args, pipeline_args = parser.parse_known_args(argv)

    pipeline_options = PipelineOptions(pipeline_args)

    with beam.Pipeline(option=pipeline_options) as p:
        QUERY = get_query('channel', known_args)

        init_ch = p
        | "ReadFromBQ" >> beam.io.Read(BigquerySource(query=QUERY))
        | "Projected" >> beam.Map(projected_channel)

        logger.info(init_ch)
        print init_ch

        result = (combine_channels(init_ch)
                  | "Join" >> beam.CoGroupByKey())
                  # | "Output" >> beam.FlatMap())

        logger.info(result)
        print result


if __name__ == "__main__":
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    run()