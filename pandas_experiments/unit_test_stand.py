import unittest
import pandas as pd

from defaults.log import log, logging_levels
from timeseries_separator import separator_v2, separator_v1
from timeseries_separator_data_gen import generate_default_timeseries_ds, generate_timeseries_ds

log.level = logging_levels[1]


class TestSeparator(unittest.TestCase):
    def setUp(self):
        self.func = separator_v2
        self.chunk_size = 10

    def tearDown(self):
        pass

    def test_canonical_example(self):
        log.info("run test_canonical_example")
        ds = generate_default_timeseries_ds()
        default_result = """2023-01-01 00:00:01
2023-01-01 00:00:01

2023-01-01 00:00:02
2023-01-01 00:00:02
2023-01-01 00:00:02

2023-01-01 00:00:03"""

        res = self.func(ds, chunk_size=2)
        res_to_str = '\n\n'.join([d.to_string(index=False, header=False) for d in res])

        self.assertEqual(res_to_str, default_result)

    def test_min_chunk_size(self):
        log.info("run test_min_chunk_size")

        ds = generate_timeseries_ds(records_size=100)
        res = self.func(ds, chunk_size=self.chunk_size)

        if len(res) == 1:
            min_chunk_size = min([d.size for d in res])
        else:
            min_chunk_size = min([d.size for d in res][:-1])

        self.assertGreaterEqual(
            min_chunk_size, self.chunk_size
        )

    def test_none(self):
        log.info("run test_none")
        self.assertRaises(TypeError, self.func, pd_df=None, chunk_size=self.chunk_size)

    def test_wrong_ts_column(self):
        log.info("run test_ts_column_existence")
        self.assertRaises(KeyError, self.func, pd_df=pd.DataFrame(), chunk_size=self.chunk_size)


if __name__ == '__main__':
    unittest.main()

