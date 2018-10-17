# -*- coding: UTF-8 -*-
import logging
import apache_beam as beam


class FormatAsCSV(beam.DoFn):

    def _flatten_dict(self, records):
        result = dict()
        for k, v in records.items():
            if isinstance(v, dict):
                result.update(self._flatten_dict(v))
            elif isinstance(v, (int, float, str, unicode)) or v is None:
                # For Python 2, the type of character also could be unicode.
                result[k] = v if v else 0
            elif len(v) > 0 and isinstance(v[0], dict):
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


class FormatAsAvro(beam.DoFn):

    def _flatten_dict(self, records):
        result = dict()
        for k, v in records.items():
            if isinstance(v, dict):
                result.update(self._flatten_dict(v))
            elif isinstance(v, (int, float, str, unicode)) or v is None:
                # For Python 2, the type of character also could be unicode.
                result[k] = v if v else 0
            elif len(v) > 0 and isinstance(v[0], dict):
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

    def process(self, element, is_kv=True):
        if is_kv:
            key, val = element
            records_list = {"cookies": key}
        else:
            val = element
            records_list = {}

        flat_val = self._flatten_dict(val)
        records_list.update(flat_val)

        yield records_list