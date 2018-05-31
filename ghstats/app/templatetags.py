

def list_to_str(value):
    if not isinstance(value, list):
        value = list(value)
    return ','.join([str(v) for v in value])

def setup(environment):
    environment.filters['list_to_str'] = list_to_str
