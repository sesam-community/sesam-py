# -*- coding: utf-8 -*-
# This is an older version of jsonformat.py from the lake repository
# The sorting order on dictionary keys only promotes _id to the front

# import json
import simplejson as json
from collections import OrderedDict
from collections.abc import Mapping


class FormatStyle(object):
    def __init__(self, **kwargs):
        prop_defaults = {
            "spaces_for_indent":
                (2, "Tab length", "Number of spaces to use for indentation"),
            "use_tab_for_indent":
                (False, "Indent with tabs", "Use real tab characters for indentation"),
            "space_after_colon":
                (True, "Space after colon", "Add an extra space after colons"),
            "space_after_comma":
                (True, "Space after comma", "Add an extra space after commas"),
            "new_line_before_dict_as_value":
                (False, "Newline before object-as-value", "Add a newline before an object when that object is a value of another object"),
            "newline_before_dict_in_array":
                (False, "Newline before object in array", "Add a newline before an object within an array"),
            "close_nested_array_on_new_line":
                (True, "Close nested array on new line", "If an array has a child array, place the parent's closing bracket on a new line"),
            "collapse_indent_for_dict_inside_array":
                (True, "Don't indent objects within arrays", "Avoid extra indention of objects inside arrays"),
            "elements_of_array_as_value_on_separate_lines":
                (False, "Array-as-value on separate lines", "If an array is a value of an object, split the array elements over multiple lines"),
            "sort_keys_by_convention":
                (True, "Sort by convention", "Sorts object keys in conventional order instead of alphabetically"),
        }

        for prop, (default, label, doc) in prop_defaults.items():
            setattr(self, prop, kwargs.get(prop, default))
            setattr(self, "_doc_" + prop, doc)
            setattr(self, "_label_" + prop, label)

    def __str__(self):
        attributes2print = []
        for attr in dir(self):
            if attr[0] != "_":
                attributes2print.append(str('{k}={v}'.format(k=attr, v=getattr(self,attr))))
        return ",".join(attributes2print)


def format_json(json_object, style=FormatStyle()):
    return format_object(json.loads(json_object), style)


def format_object(value, style=FormatStyle()):
    DICT = 0
    ARRAY = 1
    STRING = 2
    ESCAPE = 3
    # compact json representation with no extra whitespace
    SORT_ORDER = [
        # general sorting
        '_id', 'type', 'name',
        # pipes
        'source', 'sink', 'transform', 'pump', 'metadata',
        # sinks, sources
        'system',
        # hops
        'datasets', 'where', 'return', 'recurse', 'max_depth', 'exclude_root', 'track-dependencies', 'trace',
        # dtl transform
        'default',
    ]

    def key_weight(key):
        if not style.sort_keys_by_convention or key not in SORT_ORDER:
            # pad to make sure defined order get first
            return "0" + key
        else:
            return chr(SORT_ORDER.index(key))

    def sort_dict(dict):
        for key in dict:
            dict[key] = sort(dict[key])
        return OrderedDict(sorted(dict.items(), key=lambda t: key_weight(t[0])))

    def sort(v):
        if isinstance(v, Mapping):
            return sort_dict(v)
        elif isinstance(v, list):
            return [sort(i) for i in v]
        else:
            return v

    ordered_value = sort(value)

    minimal = json.dumps(ordered_value, separators=(',', ':'), ensure_ascii=False)
    output = ""
    stack = []
    indent = 0

    def new_line(offset=0):
        appendix = '\n'
        for i in range(indent + offset):
            if style.use_tab_for_indent:
                appendix += '\t'
            else:
                appendix += ' ' * style.spaces_for_indent
        return appendix

    def strip_trailing_whitespace(value):
        return value.rstrip()

    prev = None
    prevprev = None
    for i, c in enumerate(minimal):
        beginning = i == 0
        if len(stack) > 0 and stack[-1] is ESCAPE:
            # last character was escape symbol, so we ignore the current char and jump out of escape mode
            stack.pop()
        else:
            if c == '\\':
                # escape character, jump into escape mode
                stack.append(ESCAPE)
            if c == '"':
                if len(stack) > 0 and stack[-1] is STRING:
                    stack.pop()
                else:
                    stack.append(STRING)

            # we do not parse the inside of a string
            if len(stack) == 0 or stack[-1] is not STRING:
                # maintain stack and indentation so we know if we are inside a dict or an array
                if c == '{':
                    # we only indent dicts inside dicts
                    if not style.collapse_indent_for_dict_inside_array or len(stack) == 0 or stack[-1] == DICT:
                        indent += 1
                    stack.append(DICT)
                elif c == '[':
                    indent += 1
                    stack.append(ARRAY)
                elif c == ']':
                    stack.pop()
                    indent -= 1
                elif c == '}':
                    stack.pop()
                    # we only indented dicts inside dicts
                    if not style.collapse_indent_for_dict_inside_array or len(stack) < 1 or stack[-1] is DICT:
                        indent -= 1

                if c == '}':
                    # empty dicts should just collapse
                    if prev == '{':
                        output = strip_trailing_whitespace(output)
                    else:
                        # we want the end of dicts inside arrays to be on the _same_ indent as the array
                        # opening to avoid double indenting of the dict keys
                        if len(stack) > 0 and stack[-1] is ARRAY and style.collapse_indent_for_dict_inside_array:
                            output += new_line(-1)
                        else:
                            output += new_line()
                elif c == '{':
                    if style.new_line_before_dict_as_value and not beginning:
                        output = strip_trailing_whitespace(output)
                        output += new_line(-1)
                    elif style.newline_before_dict_in_array and len(stack) > 1 and stack[-2] is ARRAY:
                        output = strip_trailing_whitespace(output)
                        output += new_line(-1)
                elif c == '[':
                    if len(stack) > 1 and stack[-2] is ARRAY:
                        # prevent double newlines
                        if not output.endswith(new_line(-1)):
                            output = strip_trailing_whitespace(output)
                            output += new_line(-1)
                elif c == ']':
                    if prev == ']' and style.close_nested_array_on_new_line:
                        output += new_line()

        output += c

        # we do not parse the inside of a string
        if len(stack) == 0 or stack[-1] is not STRING:
            if c == '{':
                output += new_line()
            elif c == ':':
                if style.space_after_colon:
                    output += ' '
            elif c == ',':
                if stack[-1] is DICT:
                    output += new_line()
                elif len(stack) > 1 and stack[-2] is DICT and stack[-1] is ARRAY and \
                        style.elements_of_array_as_value_on_separate_lines:
                    output += new_line()
                elif style.space_after_comma:
                    output += ' '
        prevprev = prev
        prev = c
    output += '\n'
    return output
