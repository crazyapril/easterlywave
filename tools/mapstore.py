import os
import pickle

__warehouse__ = os.path.join(os.path.dirname(__file__), 'mapstore')


def get_filename_by_key(key):
    return os.path.join(__warehouse__, key + '.map')

def load_map(key):
    try:
        return pickle.load(open(get_filename_by_key(key), 'rb'))
    except FileNotFoundError:
        return None

def save_map(key, m):
    pickle.dump(m, open(get_filename_by_key(key), 'wb'))

def delete_map(key):
    os.remove(get_filename_by_key(key))

def _save_basemap(key, **kwargs):
    from mpl_toolkits.basemap import Basemap
    m = Basemap(**kwargs)
    save_map(key, m)
