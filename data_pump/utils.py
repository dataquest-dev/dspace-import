import json
import os
import requests

from support.dspace_proxy import rest_proxy
from migration_const import DATA_PATH
from const import API_URL


def read_json(file_name, file_path=DATA_PATH):
    """
    Read data from file as json.
    @param file_name: file name
    @return: data as json
    """
    f_path = os.path.join(file_path, file_name)
    assert os.path.exists(f_path)
    with open(f_path, mode='r', encoding='utf-8') as f:
        json_p = json.load(f)
    return json_p


def convert_response_to_json(response: requests.models.Response):
    """
    Convert response to json.
    @param response: response from api call
    @return: json created from response
    """
    return json.loads(response.content.decode('utf-8'))


def do_api_post(url, params: dict, json_p):
    """
    Insert data into database by api.
    @param url: url for api post
    @param params: parameters for api post
    @param json_p: posted data
    @return: response from api post
    """
    url = API_URL + url
    response = rest_proxy.d.api_post(url, params, json_p)
    return response


def do_api_get_one(url, object_id):
    """
    Get data with id from table.
    @param url: url for api get
    @param object_id: id of object
    @return: response from api get
    """
    url = API_URL + url + '/' + str(object_id)
    response = rest_proxy.d.api_get(url, {}, None)
    return response


def do_api_get_all(url):
    """
    Get all data from table.
    @param url: url for api get
    @return: response from api get
    """
    url = API_URL + url
    # is the default value of how many items you get when you want all data from a table
    # need to increase this value, or you won't get all data
    # you increase this value by param 'size'
    params = {'size': 1000}
    response = rest_proxy.d.api_get(url, params, None)
    return response