from lightflow.models.exceptions import LightflowException


def test_exception_str_and_repr():
    exc = LightflowException(message='the-message')
    assert 'the-message' in str(exc)
    assert 'the-message' in repr(exc)
