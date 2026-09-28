import argparse
import pytest

from meshweaver.cli import _parse_arguments


def test_parse_arguments_standard_args():
    args = argparse.Namespace(args=["10", "3.14", "hello"], args_json=None, kwargs_json=None)
    call_args, call_kwargs = _parse_arguments(args)
    assert call_args == (10, 3.14, "hello")
    assert call_kwargs == {}


def test_parse_arguments_args_json_nested():
    json_str = '[42, 3.14159, "text", true, false, null, [1, 2, 3], {"inner": "val"}]'
    args = argparse.Namespace(args=None, args_json=json_str, kwargs_json=None)
    call_args, call_kwargs = _parse_arguments(args)
    assert call_args == (42, 3.14159, "text", True, False, None, [1, 2, 3], {"inner": "val"})
    assert call_kwargs == {}


def test_parse_arguments_kwargs_json_nested():
    json_str = '{"int": 1, "float": 2.5, "bool": true, "list": ["a", "b"], "nested": {"k": "v"}}'
    args = argparse.Namespace(args=None, args_json=None, kwargs_json=json_str)
    call_args, call_kwargs = _parse_arguments(args)
    assert call_args == ()
    assert call_kwargs == {
        "int": 1,
        "float": 2.5,
        "bool": True,
        "list": ["a", "b"],
        "nested": {"k": "v"},
    }


def test_parse_arguments_both_args_and_args_json_raises():
    args = argparse.Namespace(args=["1", "2"], args_json="[1, 2]", kwargs_json=None)
    with pytest.raises(ValueError, match="Cannot specify both --args and --args-json"):
        _parse_arguments(args)


def test_parse_arguments_malformed_args_json():
    args = argparse.Namespace(args=None, args_json="[1, 2, broken", kwargs_json=None)
    with pytest.raises(ValueError, match="Malformed JSON for --args-json"):
        _parse_arguments(args)


def test_parse_arguments_malformed_kwargs_json():
    args = argparse.Namespace(args=None, args_json=None, kwargs_json='{"unclosed": ')
    with pytest.raises(ValueError, match="Malformed JSON for --kwargs-json"):
        _parse_arguments(args)


def test_parse_arguments_non_list_args_json():
    args = argparse.Namespace(args=None, args_json='{"not": "a list"}', kwargs_json=None)
    with pytest.raises(ValueError, match="must evaluate to a JSON list"):
        _parse_arguments(args)


def test_parse_arguments_non_dict_kwargs_json():
    args = argparse.Namespace(args=None, args_json=None, kwargs_json="[1, 2, 3]")
    with pytest.raises(ValueError, match="must evaluate to a JSON object"):
        _parse_arguments(args)
