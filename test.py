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

PROJECT_FIELDS_CH = env.PROJECT_FIELDS['CH']
PROJECT_FIELDS_ALL = env.PROJECT_FIELDS['ALL']
CHANNEL_LISTS = env.CHANNEL_LISTS
HEADERS = "\x14".join(env.COLUMNS["VIEW"])
COLUMNS = env.COLUMNS["VIEW"][1:]


def CombineChPColl(input_data):
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


def run(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", required=True, type=str, help="")
    parser.add_argument("--output", required=True, type=str, help="")
    known_args, pipeline_args = parser.parse_known_args(argv)

    file_path = os.path.join(known_args.output, known_args.date, known_args.date+"-view")

    pipeline_options = PipelineOptions(pipeline_args)

    with beam.Pipeline(options=pipeline_options) as p:
        query_of_all = get_query('all', date=known_args.date)
        query_of_ch = get_query('channel', date=known_args.date)

        init_all = (p
        | "ReadFromBQ_All" >> beam.io.Read(BigQuerySource(query=query_of_all, use_standard_sql=True))
        | "Projected_All" >> beam.ParDo(ProjectionBQ(), PROJECT_FIELDS_ALL, True, "cookies"))


        init_ch = (p
        | "ReadFromBQ_Ch" >> beam.io.Read(BigQuerySource(query=query_of_ch, use_standard_sql=True))
        | "Projected_Ch" >> beam.ParDo(ProjectionBQ(), PROJECT_FIELDS_CH))

        combine_pcoll = CombineChPColl(init_ch)
        combine_pcoll.update({'All': init_all})

        (combine_pcoll
        | "Join" >> beam.CoGroupByKey()
        | "Format" >> beam.ParDo(FormatAsCSV(), COLUMNS)
        | "Write" >> WriteToText(file_path, ".csv", shard_name_template="-SS", header=HEADERS))


if __name__ == "__main__":
    logger = logging.getLogger()
    logging.basicConfig(format="%(asctime)s - %(levelname)s - %(message)s",
                        datefmt="%m-%d %H:%M:%S")
    logging.root.setLevel(logging.INFO)
    run()