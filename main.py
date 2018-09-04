import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io.gcp.bigquery import BigquerySource, WriteToBigQuery, BigQueryDisposition

from utils.env import CHANNEL_LISTS


class ZeroImputation(beam.DoFn):
    def process(self, element, channel):
        (uid, (ch, val)) = element
        new_val = val if ch == channel else 0
        yield (uid, new_val)


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


def projected_all(row):
    PROJECT_FIELDS = ["totalSessions", "totalDuration", "totalPageviews",
                      "avgTimePerSession", "avgPageviewsPerSession"]
    key = row["cookies"]
    val = {idx: row[idx] for idx in PROJECT_FIELDS}
    return (key, val)


def formate_to_dict(element):
    uid, val = element
    uid = {"cookies": uid}
    all = val['all']
    del all['all']
    uid.update(all)
    uid.update(val)

    yield uid


def run():
    with beam.Pipeline(option=PipelineOptions()) as p:
        all = p
        | "ReadFromBQ" >> beam.io.Read(BigquerySource(query=""))
        | "Projected" >> beam.Map(projected_all)

        init_ch = p
        | "ReadFromBQ" >> beam.io.Read(BigquerySource(query=""))
        | "Projected" >> beam.Map(projected_channel)

        (combine_channels(init_ch).update({"all": all})
        | "Formate" >> beam.Map(formate_to_dict)
        | "SaveToBQ" >> beam.io.Write(WriteToBigQuery(
                    "",
                    schema="",
                    create_disposition=BigQueryDisposition.CREATE_IF_NEEDED,
                    write_disposition=BigQueryDisposition.WRITE_APPEND
                )))

