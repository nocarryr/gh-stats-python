import datetime
import pytz
import jsonfactory

UTC = pytz.UTC

DT_FMT = '%Y-%m-%dT%H:%M:%SZ'
EPOCH = UTC.localize(datetime.datetime(1970, 1, 1))

def now():
    dt = datetime.datetime.utcnow()
    dt = UTC.localize(dt)
    return dt

def dt_to_timestamp(dt):
    td = dt - EPOCH
    return td.total_seconds()

def timestamp_to_dt(ts):
    td = datetime.timedelta(seconds=ts)
    dt = EPOCH + td
    return dt

def make_aware(dt, tz=UTC):
    return tz.localize(dt)

def parse_dt(s):
    dt = datetime.datetime.strptime(s, DT_FMT)
    dt = UTC.localize(dt)
    return dt

def dt_to_str(dt):
    return dt.strftime(DT_FMT)

def is_dt_str(o):
    if not isinstance(o, str):
        return False
    if len(o) != 20:
        return False
    if 'T' not in o and not o.endswith('T'):
        return False
    if o.count('-') != 2 and o.count(':') != 2:
        return False
    return True

def iter_parse_datetimes(o):
    if isinstance(o, dict):
        o_iter = o.items()
    elif isinstance(o, list):
        o_iter = enumerate(o)
    else:
        return o
    for key, val in o_iter:
        if type(val) in (list, dict):
            o[key] = iter_parse_datetimes(val)
            continue
        if not is_dt_str(val):
            continue
        dt = parse_dt(val)
        o[key] = dt
    return o

def clean_dict_dt_keys(d):
    keys = [key for key in d.keys() if isinstance(key, datetime.datetime)]
    for key in keys:
        newkey = dt_to_str(key)
        d[newkey] = d[key]
        del d[key]

@jsonfactory.register
class JsonHandler:
    def encode(self, o):
        if isinstance(o, dict):
            clean_dict_dt_keys(o)
        elif isinstance(o, datetime.datetime):
            o = UTC.normalize(o)
            d = {'__class__':'datetime.datetime'}
            d['value'] = dt_to_str(o)
            return d
        return None
    def decode(self, d):
        keys = [key for key in d.keys() if is_dt_str(key)]
        for key in keys:
            newkey = parse_dt(key)
            d[newkey] = d[key]
            del d[key]
        if d.get('__class__') == 'datetime.datetime':
            return parse_dt(d['value'])
        return d
