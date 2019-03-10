from django.core.cache import cache


DAY = 86400
HOUR = 3600

def combine_namespace(namespace, key, default=None):
    if namespace is None:
        namespace = default
    if namespace == '' or namespace is None:
        return key
    return '{}.{}'.format(namespace, key)

def redis_cached(namespace=None, timeout=60):
    """Cache func inputs and outputs in redis. Timeout is in seconds,
    if None, the entry never expires (caution!)."""
    def _redis_cached(func):
        def wrapper(key, *args, **kwargs):
            realkey = combine_namespace(namespace, key, default=func.__name__)
            value = cache.get(realkey)
            if value is not None:
                return value
            value = func(key, *args, **kwargs)
            cache.set(realkey, value, timeout=timeout)
            return value
        return wrapper
    return _redis_cached

def redis_cached_for_classmethod(namespace=None, timeout=60):
    def _redis_cached(func):
        def wrapper(cls, key, *args, **kwargs):
            realkey = combine_namespace(namespace, key, default=cls.__name__)
            value = cache.get(realkey)
            if value is not None:
                return value
            value = func(cls, key, *args, **kwargs)
            cache.set(realkey, value, timeout=timeout)
            return value
        return wrapper
    return _redis_cached

