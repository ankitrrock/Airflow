import json


def gen_query_string(params):
    return "&".join(map(lambda k: f"{k}={params[k]}", params))


def jsonify_values(data):
    for item in data:
        for k, v in item.items():
            if type(v) == dict:
                item[k] = json.dumps(v)