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
        topic.yml 의 '"A B"OR"C"' 를 arxiv 검색식 'all:"A B" OR all:"C"' 로 바꾼다.
        따옴표를 지우고 OR 를 붙여 쓰면 'BORC' 같은 한 단어로 검색되어 엉뚱한 결과가 나온다.
        """
        terms = [t.strip().strip('"').strip() for t in re.split(r"\bOR\b", keyword)]
        return " OR ".join(f'all:"{t}"' for t in terms if t)


class CoroutineSpeedup:
    """轻量化的协程控件"""

    def __init__(
            self,
            work_q: Queue = None,
            task_docker=None,
    ):
        # 任务容器：queue
        self.worker = work_q if work_q else Queue()
        self.channel = Queue()
        # 任务容器：迭代器
        self.task_docker = task_docker
        # 协程数
        self.power = 32
        # 任务队列满载时刻长度
        self.max_queue_size = 0

        self.cache_space = []

        self.max_results = 30

        # arxiv API 는 3초에 1회 요청을 권장하므로 클라이언트를 공유해 요청 간격과 재시도를 지킨다
        self.client = arxiv.Client(page_size=self.max_results, delay_seconds=3, num_retries=5)
        # 수집에 실패한 subtopic 목록
        self.failed = []

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
                # 한 subtopic 이 실패해도 코루틴이 죽지 않고 다음 작업을 계속 처리한다
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
        # 결과를 여기서 모두 받아 두어 네트워크 오류가 이 단계에서 드러나게 한다
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

                # |publish_time|paper_title|paper_first_author|[paper_id](paper_url)|
                _paper.update({
                    paper_key: {
                        "publish_time": result.published.date(),
                        "title": result.title,
                        "authors": f"{paper_first_author} et.al.",
                        "id": paper_id,
                        "paper_url": result.entry_id,
                    },
                })
            except Exception:
                logger.exception(f"skip paper | subtopic=`{context['subtopic']}`")
        self.channel.put_nowait({
            "paper": _paper,
            "topic": context["hook"]["topic"],
            "subtopic": context["hook"]["subtopic"],
            "fields": ["Publish Date", "Title", "Authors", "PDF"]
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
            # 将上下文替换成 Markdown 语法文本
            context: dict = self.channel.get()
            paper_count += len(context["paper"])
            md_obj: dict = ot.to_markdown(context)

            # 子主题分流
            if not file_obj.get(md_obj["hook"]):
                file_obj[md_obj["hook"]] = md_obj["hook"]
            file_obj[md_obj["hook"]] += md_obj["content"]

        # 生成 Markdown 模板文件
        template_ = ot.generate_markdown_template(
            content="".join(list(file_obj.values())))

        return ot, template_, paper_count

    def go(self, power: int):
        # 任务重载
        self.offload_tasks()
        # 配置弹性采集功率
        if self.max_queue_size != 0:
            self.power = self.max_queue_size if power > self.max_queue_size else power
        # 任务启动
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

        line = f"|{paper['publish_time']}" \
               f"|{paper['title']}" \
               f"|{paper['authors']}" \
               f"|{_pdf}|\n"

        return line

    @staticmethod
    def _set_style_to(style: str = "center"):
        return " :---: " if style == "center" else " --- "

    # -------------------
    # Public API
    # -------------------
    def storage(self, template: str, obj_: str = "database"):
        """
        将 Markdown 模板存档
        @param template:
        @param obj_: database:将 Markdown 模板存档至 database/store 中。其他值，替换根目录下的 README
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
        or: python daily_arxiv.py run --env=production  生产环境下运行

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
        # 한 편도 못 가져왔으면 기존 storage/docs 를 빈 내용으로 덮어쓰지 않고 실패로 종료한다
        if paper_count == 0:
            logger.error("no papers fetched; keep existing storage and docs")
            sys.exit(1)

        # Storage 가 원본: storage 에 저장한 뒤 이를 바탕으로 README 와 docs 를 만든다
        ot.storage(template_, obj_="database")
        if env == "production":
            shutil.copyfile(ot.storage_path_by_date, SERVER_PATH_README)
            build_docs_from_storage(ot.storage_path_by_date)


def build_docs_from_storage(storage_path: str):
    """
    storage 마크다운을 원본으로 mkdocs 용 docs/ 를 다시 만든다.
    - docs/index.md: 최신 storage 전체
    - docs/<topic>/<subtopic>.md: 최신 storage 를 subtopic 단위로 나눈 페이지
    - docs/archive/<date>.md: database/storage 의 날짜별 기록
    """
    with open(storage_path, "r", encoding="utf8") as f:
        content = f.read()

    # 이전에 생성된 페이지(원본 레포의 예전 토픽 포함)를 지우고 새로 만든다
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
