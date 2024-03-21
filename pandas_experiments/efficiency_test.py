from codetiming import Timer
from timeseries_separator import separator_v1, separator_v2
from timeseries_separator_data_gen import generate_timeseries_ds
from memory_profiler import memory_usage


if __name__ == '__main__':
    timeseries = generate_timeseries_ds(records_size=1000000)

    for func in [separator_v1, separator_v2]:
        with Timer(name=func.__name__, text="{name:20}: {milliseconds:.2f} ms"):
            func(timeseries, chunk_size=100)

        def f():
            return func(timeseries, chunk_size=100)

        mem_usage = memory_usage(f)

        print('Memory usage (in chunks of .1 seconds): %s' % mem_usage)
        print('Maximum memory usage: %s' % max(mem_usage))
        print()
