# -*- coding: UTF-8 -*-
import apache_beam as beam


class ProjectionBQ(beam.DoFn):
    def process(self, element, fields, is_kv=False, key=None):
        if is_kv and key is None:
            ValueError("You must give the value to parameter key")

        if is_kv:
            key = element[key]
            val = {idx: element[idx] for idx in fields}
            yield (key, val)
        else:
            project_dict = {idx: element[idx] for idx in fields}
            yield project_dict