#!/usr/bin/env python
"""
Load and validate database/topic.yml.

Expected shape:
    "Topic":
      "Subtopic": '"keyword A"OR"keyword B"'

Usage: python topic_lint.py [path]   (default: database/topic.yml)
"""

import re
import sys

import yaml

DEFAULT_PATH = "database/topic.yml"

# '"A"' or '"A"OR"B"OR...' ; build_query in main.py relies on this shape
KEYWORD_RE = re.compile(r'^"[^"]+"(OR"[^"]+")*$')


class TopicLintError(Exception):
    def __init__(self, path, problems):
        self.problems = problems
        super().__init__(f"{path} is invalid:\n" + "\n".join(f"  - {p}" for p in problems))


class _UniqueKeyLoader(yaml.SafeLoader):
    """SafeLoader that rejects duplicate keys instead of silently keeping the last one."""

    def construct_mapping(self, node, deep=False):
        seen = set()
        for key_node, _ in node.value:
            key = self.construct_object(key_node, deep=deep)
            if key in seen:
                raise yaml.constructor.ConstructorError(
                    None, None, f"duplicate key {key!r}", key_node.start_mark)
            seen.add(key)
        return super().construct_mapping(node, deep=deep)


def _check_name(kind, name, where):
    if not isinstance(name, str) or not name.strip():
        return [f"{where}: {kind} name must be a non-empty string, got {name!r}"]
    if "/" in name:
        # Names become paths under docs/, so '/' would create a nested directory
        return [f"{where}: {kind} name must not contain '/'"]
    return []


def validate(data):
    """Return a list of problems; empty when the data is valid."""
    if not isinstance(data, dict) or not data:
        return ["top level must be a mapping of topics"]
    problems = []
    for topic, subtopics in data.items():
        problems += _check_name("topic", topic, f"{topic!r}")
        if not isinstance(subtopics, dict) or not subtopics:
            problems.append(f"{topic!r}: must be a mapping of subtopics to keywords")
            continue
        for subtopic, keyword in subtopics.items():
            where = f"{topic!r} > {subtopic!r}"
            problems += _check_name("subtopic", subtopic, where)
            if not isinstance(keyword, str) or not keyword.strip():
                problems.append(f"{where}: keyword must be a non-empty string")
            elif not KEYWORD_RE.match(keyword):
                problems.append(f'{where}: keyword must look like \'"A"\' or \'"A"OR"B"\', got {keyword!r}')
    return problems


def load_topics(path=DEFAULT_PATH):
    """Load topic.yml, raising TopicLintError on YAML errors, duplicate keys or an invalid shape."""
    try:
        with open(path, "r", encoding="utf8") as f:
            data = yaml.load(f, Loader=_UniqueKeyLoader)
    except yaml.YAMLError as e:
        raise TopicLintError(path, [str(e)]) from e
    problems = validate(data)
    if problems:
        raise TopicLintError(path, problems)
    return data


if __name__ == "__main__":
    path = sys.argv[1] if len(sys.argv) > 1 else DEFAULT_PATH
    try:
        topics = load_topics(path)
    except TopicLintError as e:
        print(e, file=sys.stderr)
        sys.exit(1)
    print(f"{path}: OK ({len(topics)} topics, {sum(len(s) for s in topics.values())} subtopics)")
