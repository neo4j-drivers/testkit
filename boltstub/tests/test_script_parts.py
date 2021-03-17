import json
import pytest
from unittest.mock import patch

from ..bolt_protocol import TranslatedStructure
from ..errors import ServerExit
from ..parsing import (
    ClientLine,
    ServerLine,
)


class TestClientLine:
    def test_matches_tag_name(self):
        line = ClientLine(10, "C: MSG", "MSG")
        msg = TranslatedStructure("MSG", b"\00")
        assert line.match(msg)

    def test_doesnt_match_wrong_tag_name(self):
        line = ClientLine(10, "C: MSG2", "MSG2")
        msg = TranslatedStructure("MSG", b"\00")
        assert not line.match(msg)

    @pytest.mark.parametrize("fields", (
        ["a"],
        [1],
        [1.2],
        [-500],
        [None],
        [{"a": 1, "b": {"c": "c"}}],
        [["str", 4]],
        ["hello", "world"],
        [["hello", "world"]],
    ))
    def test_matches_fields(self, fields):
        content = "MSG " + " ".join(map(json.dumps, fields))
        line = ClientLine(10, "C: " + content, content)
        msg = TranslatedStructure("MSG", b"\00", *fields)
        assert line.match(msg)

    @pytest.mark.parametrize(("expected", "received"), (
        [["a"], [["a"]]],
        [[1], ["1"]],
        [[1], [1.0]],
        [[1, 2, 3], [[1, 2, 3]]],
        [[None], ["null"]],
        [[None], ["None"]],
        [[{"a": 1}], [{"a": "1"}]],
        [[{"a": 1}], [{}]],
        [[{"a": 1}], [{"a": 1, "b": 2}]],
        [["hello", "world"], ["world", "hello"]],
        [[["hello", "world"]], [["world", "hello"]]],
    ))
    @pytest.mark.parametrize("flip", (True, False))
    def test_doesnt_match_wrong_fields(self, expected, received, flip):
        content = "MSG " + " ".join(map(json.dumps,
                                        received if flip else expected))
        line = ClientLine(10, "C: " + content, content)
        msg = TranslatedStructure("MSG", b"\00",
                                  *(expected if flip else received))
        assert not line.match(msg)

    @pytest.mark.parametrize(("expected", "received", "match"), (
        # optional
        [[{"[a]": 1}], [{"a": 1}], True],
        [[{"[a]": 1}], [{}], True],
        [[{"[a]": 1, "b": 2}], [{"a": 1, "b": 2}], True],
        [[{"[a]": 1, "b": 2}], [{"b": 2}], True],
        [[{"[a]": 1}], [{"a": None}], False],
        [[{"[a]": 1}], [{"a": "whatever"}], False],
        [[{"[a]": 1}], [{"a": 1, "b": 2}], False],
        [[{"[a]": 1, "[b]": 1}], [{"c": 1}], False],
        # escaping of optional
        [[{"[a]": 1}], [{"[a]": 1}], False],
        [[{"[[a]]": 1}], [{"[a]": 1}], True],
        [[{"[[a]]": 1}], [{}], True],
        [[{r"\[a\]": 1}], [{"[a]": 1}], True],
        [[{r"\[a\]": 1}], [{}], False],
        [[{r"\[[a]\]": 1}], [{"[[a]]": 1}], True],
        [[{r"a[]": 1}], [{"a[]": 1}], True],
        [[{r"\\[a\\]": 1}], [{r"\[a\]": 1}], True],
        [[{r"a\[]": 1}], [{"a[]": 1}], True],
        [[{r"a\\[\\]": 1}], [{r"a\[\]": 1}], True],
        [[{r"a\\[\\\]": 1}], [{r"a\[\]": 1}], True],
        # wildcard
        [[{"a": "*"}], [{"a": 1}], True],
        [[{"a": "*"}], [{"a": "*"}], True],
        [[{"a": "*"}], [{"a": False}], True],
        [[{"a": "*"}], [{"a": None}], True],
        [[{"a": "*"}], [{"a": 1.2}], True],
        [[{"a": "*"}], [{"a": [1, 2]}], True],
        [[{"a": "*"}], [{"a": {"b": 1}}], True],
        [[{"a": "*"}], [{}], False],
        # escaping of wildcard
        [[{"a": r"\*"}], [{"a": "*"}], True],
        [[{"a": r"\\*"}], [{"a": r"\*"}], True],
        [[{"a": r"\\\*"}], [{"a": r"\*"}], True],
        [[{"a": "*test"}], [{"a": "*test"}], True],
        [[{"a": "te*st"}], [{"a": "te*st"}], True],
        [[{"a": "test*"}], [{"a": "test*"}], True],
        # sorted
        [[{"a{}": [1, 2]}], [{"a": [1, 2]}], True],
        [[{"a{}": [1, 2]}], [{"a": [2, 1]}], True],
        [[{"a{}": [1, 2]}], [{"a": [1]}], False],
        [[{"a{}": [1, 2]}], [{"a": []}], False],
        [[{"a{}": [1, 2]}], [{"a": [1, 2, 3]}], False],
        [[{"a{}": [1, 2]}], [{"a": [1, 2, 1]}], False],
        [[{"a{}": [2, 1]}], [{"a": [1, 2]}], True],
        [[{"a{}": [2, 1]}], [{"a": [2, 1]}], True],
        [[{"a{}": [2, 1]}], [{"a": [1]}], False],
        [[{"a{}": [2, 1]}], [{"a": []}], False],
        [[{"a{}": [2, 1]}], [{"a": [1, 2, 3]}], False],
        [[{"a{}": [2, 1]}], [{"a": [1, 2, 1]}], False],
        [[{"a{}": {"a1": 1, "a2": 2}}], [{"a": {"a1": 1, "a2": 2}}], True],
        [[{"a{}": {"a1": 1, "a2": 2}}], [{"a": {"a2": 1, "a1": 2}}], False],
        [[{"a{}": {"a1": 1}}], [{"a": {"a1": 1, "a2": 2}}], False],
        [[{"a{}": {"a1": 1, "a2": 2}}], [{"a": {"a1": 1}}], False],
        [[{"a{}": "abc"}], [{"a": "abc"}], True],
        [[{"a{}": "abc"}], [{"a": "bac"}], False],
        [[{"a{}": 1}], [{"a": 1}], True],
        [[{"a{}": 1}], [{"a": 2}], False],
        [[{"a{}": False}], [{"a": False}], True],
        [[{"a{}": True}], [{"a": False}], False],
        [[{"a{}": None}], [{"a": None}], True],
        [[{"a{}": 1.2}], [{"a": 1.2}], True],
        [[{"a{}": 1.3}], [{"a": 1.2}], False],
        # escaping of sorted
        [[{"a{}": [1, 2]}], [{"a{}": [1, 2]}], False],
        [[{"a{}{}": [1, 2]}], [{"a{}": [1, 2]}], True],
        [[{"a{}{}": [1, 2]}], [{"a{}": [2, 1]}], True],
        [[{"a{}{}": [1, 2]}], [{"a{}{}": [1, 2]}], False],
        [[{r"a\{\}": [1, 2]}], [{"a{}": [1, 2]}], True],
        [[{r"a\{\}": [1, 2]}], [{"a{}": [2, 1]}], False],
        [[{r"a{}\{\}": [1, 2]}], [{"a{}": [1, 2]}], False],
        [[{r"a{}\{\}": [1, 2]}], [{"a{}{}": [1, 2]}], True],
        [[{r"a{}\{\}": [1, 2]}], [{"a{}{}": [2, 1]}], False],
        [[{r"a\{\}\{\}": [1, 2]}], [{"a{}": [1, 2]}], False],
        [[{r"a\{\}\{\}": [1, 2]}], [{"a{}{}": [1, 2]}], True],
        [[{r"a\{\}\{\}": [1, 2]}], [{"a{}{}": [2, 1]}], False],
        [[{r"a\{\}{}": [1, 2]}], [{"a{}": [1, 2]}], True],
        [[{r"a\{\}{}": [1, 2]}], [{"a{}": [2, 1]}], True],
        [[{r"a\{\}{}": [1, 2]}], [{"a{}{}": [1, 2]}], False],
        [[{r"{a}": [1, 2]}], [{"{a}": [1, 2]}], True],
        [[{r"{a}": [1, 2]}], [{"{a}": [2, 1]}], False],
        [[{r"a\\{\\}": [1, 2]}], [{r"a\{\}": [1, 2]}], True],
        [[{r"a\\{\\}": [1, 2]}], [{r"a\{\}": [2, 1]}], False],
        [[{r"\{a}": [1, 2]}], [{"{a}": [1, 2]}], True],
        [[{r"\{a}": [1, 2]}], [{"{a}": [2, 1]}], False],
        [[{r"a\\{\\}": [1, 2]}], [{r"a\{\}": [1, 2]}], True],
        [[{r"a\\{\\}": [1, 2]}], [{r"a\{\}": [2, 1]}], False],
        [[{r"a\\{\\\}": [1, 2]}], [{r"a\{\}": [1, 2]}], True],
        [[{r"a\\{\\\}": [1, 2]}], [{r"a\{\}": [2, 1]}], False],
        # wildcard and optional
        [[{"[a]": "*"}], [{"a": 1}], True],
        [[{"[a]": "*"}], [{"a": "*"}], True],
        [[{"[a]": "*"}], [{"a": False}], True],
        [[{"[a]": "*"}], [{"a": None}], True],
        [[{"[a]": "*"}], [{"a": 1.2}], True],
        [[{"[a]": "*"}], [{"a": [1, 2]}], True],
        [[{"[a]": "*"}], [{"a": {"b": 1}}], True],
        [[{"[a]": "*"}], [{}], True],
        # optional and sorted
        [[{"[a{}]": [1, 2]}], [{"a": [1, 2]}], True],
        [[{"[a{}]": [1, 2]}], [{"a": [2, 1]}], True],
        [[{"[a{}]": [1, 2]}], [{}], True],
        [[{"[a{}]": [1, 2]}], [{"a": [1, 3]}], False],
        # sorted and wildcard
        [[{"a{}": "*"}], [{"a": 1}], True],
        [[{"a{}": "*"}], [{"a": "*"}], True],
        [[{"a{}": "*"}], [{"a": False}], True],
        [[{"a{}": "*"}], [{"a": None}], True],
        [[{"a{}": "*"}], [{"a": 1.2}], True],
        [[{"a{}": "*"}], [{"a": [1, 2]}], True],
        [[{"a{}": "*"}], [{"a": {"b": 1}}], True],
        [[{"a{}": "*"}], [{}], False],
        # all together now (sorted, optional, wildcard)
        # if you are really using this, please reconsider your design choices
        [[{"[a{}]": "*"}], [{"a": 1}], True],
        [[{"[a{}]": "*"}], [{"a": "*"}], True],
        [[{"[a{}]": "*"}], [{"a": False}], True],
        [[{"[a{}]": "*"}], [{"a": None}], True],
        [[{"[a{}]": "*"}], [{"a": 1.2}], True],
        [[{"[a{}]": "*"}], [{"a": [1, 2]}], True],
        [[{"[a{}]": "*"}], [{"a": {"b": 1}}], True],
        [[{"[a{}]": "*"}], [{}], True],
    ))
    @pytest.mark.parametrize("nested", (None, "list", "dict"))
    def test_optional_wildcard_set_dict_entries(self, expected, received,
                                                match, nested):
        if nested == "list":
            expected = [expected]
            received = [received]
        elif nested == "dict":
            if expected and received:
                expected[0] = {"key": expected[0]}
                received[0] = {"key": received[0]}
        content = "MSG " + " ".join(map(json.dumps, expected))
        line = ClientLine(10, "C: " + content, content)
        msg = TranslatedStructure("MSG", b"\00", *received)
        assert match == line.match(msg)


@pytest.fixture()
def channel_mock():
    class ChannelMock:
        def __init__(self):
            self.buffer = bytearray()

        def send_raw(self, b):
            self.buffer.extend(b)

    return ChannelMock()


class TestServerLine:
    def test_plain_server_line_runs_no_command(self, channel_mock):
        content = "MSG [1, 2]"
        line = ServerLine(10, "S: " + content, content)
        assert not line.try_run_command(channel_mock)

    def test_exit_server_line(self, channel_mock):
        content = "<EXIT>"
        line = ServerLine(10, "S: " + content, content)
        with pytest.raises(ServerExit):
            line.try_run_command(channel_mock)

    def test_noop_server_line(self, channel_mock):
        content = "<NOOP>"
        line = ServerLine(10, "S: " + content, content)
        assert line.try_run_command(channel_mock)
        assert channel_mock.buffer == b"\x00\x00"

    @pytest.mark.parametrize(("string", "bytes_"), (
        ["fF 12", b"\xff\x12"],
        ["fF12", b"\xff\x12"],
        ["fF 1 2", b"\xff\x01\x02"],
        ["fF1 2", b"\xff\x01\x02"],
    ))
    def test_noop_server_line(self, string, bytes_, channel_mock):
        content = "<RAW> " + string
        line = ServerLine(10, "S: " + content, content)
        assert line.try_run_command(channel_mock)
        assert bytes(channel_mock.buffer) == bytes_

    @pytest.mark.parametrize("duration", (0, 0.5, 1.5, 200))
    def test_sleep_server_line(self, duration, channel_mock, mocker):
        sleep_path = __name__.rsplit(".", 2)[0] + ".parsing.sleep"
        with patch(sleep_path, return_value=None) as patched_sleep:
            content = "<SLEEP> {}".format(duration)
            line = ServerLine(10, "S: " + content, content)
            assert line.try_run_command(channel_mock)
            patched_sleep.assert_called_once_with(duration)
