import abc


class Cache(type):
    kwargs_mark = object()

    def __init__(cls, *args, **kwargs):
        cls._instances = {}

    def __call__(cls, *args, **kwargs):
        key = args + (Cache.kwargs_mark,) + tuple(sorted(kwargs.items()))

        try:
            return cls._instances[key]
        except KeyError:
            instance = super().__call__(*args, **kwargs)
            cls._instances[key] = instance
            return instance


class CacheMeta(abc.ABCMeta, Cache):
    pass
