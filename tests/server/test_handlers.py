import mock

from mlflow.server.handlers import get_endpoints, _create_experiment, _get_request_message, \
    handle_errors
from mlflow.protos.service_pb2 import CreateExperiment


def test_get_endpoints():
    endpoints = get_endpoints()
    create_experiment_endpoint = [e for e in endpoints if e[1] == _create_experiment]
    assert len(create_experiment_endpoint) == 2


def test_can_parse_json():
    request = mock.MagicMock()
    request.query_string = ""
    request.get_json = mock.MagicMock()
    request.get_json.return_value = {"name": "hello"}
    msg = _get_request_message(CreateExperiment(), flask_request=request)
    assert msg.name == "hello"


# Previous versions of the client sent a doubly string encoded JSON blob,
# so this test ensures continued compliance with such clients.
def test_can_parse_json_string():
    request = mock.MagicMock()
    request.query_string = ""
    request.get_json = mock.MagicMock()
    request.get_json.return_value = '{"name": "hello2"}'
    msg = _get_request_message(CreateExperiment(), flask_request=request)
    assert msg.name == "hello2"


def test_handle_errors_wraps_errors():
    @handle_errors
    def my_func():
        raise Exception('Oh no!')
    response = my_func()
    assert 'Oh no!' in str(response.response)
    assert response.status_code == 500
