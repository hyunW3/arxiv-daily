#!/usr/bin/env python
# -*- encoding: utf-8 -*-
"""
@File    :   daily_arxiv.py
@Time    :   2021-10-29 22:34:09
@Author  :   Bingjie Yan
@Email   :   bj.yan.pa@qq.com
@License :   Apache License 2.0
"""

import glob
import os.path
import re
import shutil
import sys

from gevent import monkey

monkey.patch_all()
import gevent
from gevent.queue import Queue
from datetime import datetime
import arxiv
import requests
import yaml

from fire import Fire

from config import (
    SERVER_PATH_TOPIC,
    SERVER_DIR_STORAGE,
    SERVER_PATH_README,
    SERVER_PATH_DOCS,
    SERVER_PATH_STORAGE_MD,
    SERVER_PATH_STORAGE_GLOB,
    TIME_ZONE_KR,
    logger
)


class ToolBox:
    @staticmethod
    def log_date(mode="log"):
        if mode == "log":
            return str(datetime.now(TIME_ZONE_KR)).split(".")[0]
        elif mode == "file":
            return str(datetime.now(TIME_ZONE_KR)).split(" ")[0]

    @staticmethod
    def get_yaml_data() -> dict:
        with open(SERVER_PATH_TOPIC, "r", encoding="utf8") as f:
            data = yaml.load(f, Loader=yaml.SafeLoader)
        print(data)
        return data

    @staticmethod
    def build_query(keyword: str) -> str:
        """
        Convert '"A B"OR"C"' from topic.yml into the arxiv query 'all:"A B" OR all:"C"'.
        Stripping quotes and gluing OR yields a single word like 'BORC', which returns unrelated results.
        """
        terms = [t.strip().strip('"').strip() for t in re.split(r"\bOR\b", keyword)]
        return " OR ".join(f'all:"{t}"' for t in terms if t)


class CodeFinder:
    """
    Find a paper's code repository link. (Replaces the shut-down Papers with Code API)
    1. A github.com link in the arxiv abstract/comment (no extra request, usually the authors' official repo)
    2. Otherwise, the githubRepo field from the Hugging Face Papers API
    """
    HF_API = "https://huggingface.co/api/papers/{}"
    GITHUB_RE = re.compile(r"github\.com/([\w.-]+)/([\w.-]+)", re.IGNORECASE)
    # Placeholder repos created when PwC data was migrated (e.g. github.com/pwc-1/Paper-9)
    PLACEHOLDER_OWNER_RE = re.compile(r"^pwc-\d+$", re.IGNORECASE)

    def __init__(self):
        self.session = requests.session()
        # The same paper can appear in several subtopics, so look it up only once
        self.cache = {}

    def _normalize(self, owner: str, repo: str):
        repo = re.sub(r"\.git$", "", repo.rstrip("."))
        if not repo or self.PLACEHOLDER_OWNER_RE.match(owner):
            return None
        return f"https://github.com/{owner}/{repo}"

    def from_text(self, *texts):
        for text in texts:
            for owner, repo in self.GITHUB_RE.findall(text or ""):
                url = self._normalize(owner, repo)
                if url:
                    return url
        return None

    def from_hf(self, paper_key: str):
        try:
            response = self.session.get(self.HF_API.format(paper_key), timeout=10)
            if response.status_code != 200:
                return None
            repo_url = response.json().get("githubRepo") or ""
        except Exception as e:
            logger.warning(f"hf papers lookup failed | id=`{paper_key}` {e}")
            return None
        match = self.GITHUB_RE.search(repo_url)
        return self._normalize(*match.groups()) if match else None

    def find(self, paper_key: str, result: arxiv.Result):
        if paper_key not in self.cache:
            self.cache[paper_key] = self.from_text(result.summary, result.comment) or self.from_hf(paper_key)
        return self.cache[paper_key]


class CoroutineSpeedup:
    """Lightweight coroutine controller"""

    def __init__(
            self,
            work_q: Queue = None,
            task_docker=None,
    ):
        # Task container: queue
        self.worker = work_q if work_q else Queue()
        self.channel = Queue()
        # Task container: iterator
        self.task_docker = task_docker
        # Number of coroutines
        self.power = 32
        # Task queue length when fully loaded
        self.max_queue_size = 0

        self.cache_space = []

        self.max_results = 30

        # The arxiv API asks for one request per 3 seconds, so share one client to respect the delay and retries
        self.client = arxiv.Client(page_size=self.max_results, delay_seconds=3, num_retries=5)
        # Subtopics that failed to fetch
        self.failed = []
        self.code_finder = CodeFinder()

    def _adaptor(self):
        while not self.worker.empty():
            task: dict = self.worker.get_nowait()
            context = task.get("pending") or task
            try:
                if task.get("pending"):
                    self.runtime(context=task.get("pending"))
                elif "response" in task:
                    self.parse(context=task)
            except Exception:
                # Keep the coroutine alive and move on to the next task even if one subtopic fails
                logger.exception(f"failed | topic=`{context.get('topic')}` subtopic=`{context.get('subtopic')}`")
                self.failed.append(context.get("subtopic"))

    def _progress(self):
        p = self.max_queue_size - self.worker.qsize() - self.power
        p = 0 if p < 1 else p
        return p

    def runtime(self, context: dict):
        keyword_ = context.get("keyword")

        search = arxiv.Search(
            query=keyword_,
            max_results=self.max_results,
            sort_by=arxiv.SortCriterion.SubmittedDate
        )
        # Fetch all results here so network errors surface at this step
        res = list(self.client.results(search))

        context.update({"response": res, "hook": context})
        self.worker.put_nowait(context)

    def parse(self, context):
        _paper = {}
        arxiv_res = context.get("response")
        for result in arxiv_res:
            try:
                paper_id = result.get_short_id()
                paper_first_author = result.authors[0] if result.authors else "Unknown"

                ver_pos = paper_id.find('v')
                paper_key = paper_id if ver_pos == -1 else paper_id[0:ver_pos]

                # |publish_time|paper_title|paper_first_author|[paper_id](paper_url)|[link](repo)|
                _paper.update({
                    paper_key: {
                        "publish_time": result.published.date(),
                        "title": result.title,
                        "authors": f"{paper_first_author} et.al.",
                        "id": paper_id,
                        "paper_url": result.entry_id,
                        "repo": self.code_finder.find(paper_key, result),
                    },
                })
            except Exception:
                logger.exception(f"skip paper | subtopic=`{context['subtopic']}`")
        self.channel.put_nowait({
            "paper": _paper,
            "topic": context["hook"]["topic"],
            "subtopic": context["hook"]["subtopic"],
            "fields": ["Publish Date", "Title", "Authors", "PDF", "Code"]
        })
        logger.success(
            f"handle [{self.channel.qsize()}/{self.max_queue_size}]"
            f" | topic=`{context['topic']}` subtopic=`{context['hook']['subtopic']}` papers={len(_paper)}")

    def offload_tasks(self):
        if self.task_docker:
            for task in self.task_docker:
                self.worker.put_nowait({"pending": task})
        self.max_queue_size = self.worker.qsize()

    def overload_tasks(self):
        ot = _OverloadTasks()
        file_obj: dict = {}
        paper_count = 0
        while not self.channel.empty():
            # Convert the context into Markdown text
            context: dict = self.channel.get()
            paper_count += len(context["paper"])
            md_obj: dict = ot.to_markdown(context)

            # Group by subtopic
            if not file_obj.get(md_obj["hook"]):
                file_obj[md_obj["hook"]] = md_obj["hook"]
            file_obj[md_obj["hook"]] += md_obj["content"]

        # Generate the Markdown template
        template_ = ot.generate_markdown_template(
            content="".join(list(file_obj.values())))

        return ot, template_, paper_count

    def go(self, power: int):
        # Load tasks
        self.offload_tasks()
        # Configure elastic crawling power
        if self.max_queue_size != 0:
            self.power = self.max_queue_size if power > self.max_queue_size else power
        # Start tasks
        task_list = []
        for _ in range(self.power):
            task = gevent.spawn(self._adaptor)
            task_list.append(task)
        gevent.joinall(task_list)


class _OverloadTasks:
    def __init__(self):
        self._build()

        # yyyy-mm-dd
        self.update_time = ToolBox.log_date(mode="log")

        self.storage_path_by_date = SERVER_PATH_STORAGE_MD.format(
            ToolBox.log_date('file'))
        self.storage_path_readme = SERVER_PATH_README
        self.storage_path_docs = SERVER_PATH_DOCS

    # -------------------
    # Private API
    # -------------------
    @staticmethod
    def _build():
        if not os.path.exists(SERVER_DIR_STORAGE):
            os.mkdir(SERVER_DIR_STORAGE)

    @staticmethod
    def _set_markdown_hyperlink(text, link):
        return f"[{text}]({link})"

    def _generate_markdown_table_content(self, paper: dict):
        paper['publish_time'] = f"**{paper['publish_time']}**"
        paper['title'] = f"**{paper['title']}**"
        _pdf = self._set_markdown_hyperlink(
            text=paper['id'], link=paper['paper_url'])
        _repo = self._set_markdown_hyperlink(
            text="link", link=paper['repo']) if paper.get('repo') else "-"

        line = f"|{paper['publish_time']}" \
               f"|{paper['title']}" \
               f"|{paper['authors']}" \
               f"|{_pdf}" \
               f"|{_repo}|\n"

        return line

    @staticmethod
    def _set_style_to(style: str = "center"):
        return " :---: " if style == "center" else " --- "

    # -------------------
    # Public API
    # -------------------
    def storage(self, template: str, obj_: str = "database"):
        """
        Save the Markdown template
        @param template:
        @param obj_: database: save the Markdown template to database/storage. Any other value replaces the root README
        @return:
        """
        path_factory = {
            'database': self.storage_path_by_date,
            'readme': self.storage_path_readme,
            'docs': self.storage_path_docs
        }
        if obj_ not in path_factory.keys():
            path_ = path_factory['readme']
        else:
            path_ = path_factory[obj_]
        with open(path_, "w", encoding="utf8") as f:
            for i in template:
                f.write(i)

    def generate_markdown_template(self, content: str):
        _project = f"# arxiv-daily\n"
        _pin = f" Automated deployment @ {self.update_time} Asia/Seoul\n"
        _tos = "> Welcome to contribute! Add your topics and keywords in " \
               "[`topic.yml`](https://github.com/weiningwei/arxiv-daily/blob/main/database/topic.yml).\n"
        _tos += "> You can also view historical data through the " \
                "[storage](https://github.com/weiningwei/arxiv-daily/blob/main/database/storage).\n"

        _form = _project + _pin + _tos + content

        return _form

    def to_markdown(self, context: dict) -> dict:
        _fields = context["fields"]
        _topic = context["topic"]
        _subtopic = context["subtopic"]
        _paper_obj = context["paper"]

        _topic_md = f"\n## {_topic}\n"
        _subtopic_md = f"\n### {_subtopic}\n"
        _fields_md = f"|{'|'.join(_fields)}|\n"
        _style_md = f"|{'|'.join([self._set_style_to('center') for _ in range(len(_fields))])}|\n"
        table_lines = "".join([self._generate_markdown_table_content(
            paper) for paper in _paper_obj.values()])

        _content_md = _subtopic_md + _fields_md + _style_md + table_lines

        return {"hook": _topic_md, "content": _content_md}


class Scaffold:
    def __init__(self):
        pass

    @staticmethod
    @logger.catch()
    def run(env: str = "development", power: int = 16):
        """
        Start the test sample.

        Usage: python daily_arxiv.py run
        or: python daily_arxiv.py run --env=production  run in production

        @param power:  synergy power. The recommended value interval is [2,16].The default value is 37.
        @param env: Optional with [development production]
        @return:
        """
        # Get tasks
        context = ToolBox.get_yaml_data()

        # Set tasks
        pending_atomic = [{"subtopic": subtopic, "keyword": ToolBox.build_query(keyword), "topic": topic}
                          for topic, subtopics in context.items() for subtopic, keyword in subtopics.items()]

        # Offload tasks
        booster = CoroutineSpeedup(task_docker=pending_atomic)
        booster.go(power=power)

        # Overload tasks
        ot, template_, paper_count = booster.overload_tasks()

        if booster.failed:
            logger.warning(f"failed subtopics: {booster.failed}")
        # If no papers were fetched, exit with failure instead of overwriting storage/docs with empty content
        if paper_count == 0:
            logger.error("no papers fetched; keep existing storage and docs")
            sys.exit(1)

        # Storage is the source of truth: save to storage, then build README and docs from it
        ot.storage(template_, obj_="database")
        if env == "production":
            shutil.copyfile(ot.storage_path_by_date, SERVER_PATH_README)
            build_docs_from_storage(ot.storage_path_by_date)


def build_docs_from_storage(storage_path: str):
    """
    Rebuild docs/ for mkdocs from the storage markdown.
    - docs/index.md: the full latest storage
    - docs/<topic>/<subtopic>.md: the latest storage split into one page per subtopic
    - docs/archive/<date>.md: per-date records from database/storage
    """
    with open(storage_path, "r", encoding="utf8") as f:
        content = f.read()

    # Remove previously generated pages (including old topics from the upstream repo) and rebuild
    for name in os.listdir(SERVER_PATH_DOCS):
        path_ = os.path.join(SERVER_PATH_DOCS, name)
        if os.path.isdir(path_):
            shutil.rmtree(path_)
        elif name.endswith(".md"):
            os.remove(path_)

    with open(os.path.join(SERVER_PATH_DOCS, "index.md"), "w", encoding="utf8") as f:
        f.write(content)

    topic, subtopic, buffer = None, None, []

    def flush():
        if topic and subtopic:
            os.makedirs(os.path.join(SERVER_PATH_DOCS, topic), exist_ok=True)
            with open(os.path.join(SERVER_PATH_DOCS, topic, f"{subtopic}.md"), "w", encoding="utf8") as f:
                f.write("".join(buffer))

    for line in content.splitlines(keepends=True):
        if line.startswith("## "):
            flush()
            topic, subtopic, buffer = line[3:].strip(), None, []
        elif line.startswith("### "):
            flush()
            subtopic, buffer = line[4:].strip(), [line]
        elif subtopic:
            buffer.append(line)
    flush()

    archive_dir = os.path.join(SERVER_PATH_DOCS, "archive")
    os.makedirs(archive_dir, exist_ok=True)
    for path_ in sorted(glob.glob(SERVER_PATH_STORAGE_GLOB)):
        date_ = re.search(r"storage_(.+)\.md$", path_).group(1)
        shutil.copyfile(path_, os.path.join(archive_dir, f"{date_}.md"))


if __name__ == "__main__":
    Fire(Scaffold)
