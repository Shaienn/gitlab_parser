#!/bin/python3
import time

import gitlab
import json
import zipfile
from junitparser import JUnitXml, Failure, Skipped, Error, TestCase, Attr
import os.path
import shutil
import tempfile
import xml
import pandas as pd  # data processing, CSV file I/O (e.g. pd.read_csv)
import asyncio
import aio_gitlab
import concurrent.futures
import io
import gitlab.v4.objects as gl_objects
import urllib.parse
from aiohttp import ClientSession


class AioGitlab(object):
    def __init__(self, gl, gitlab_url, gitlab_token):
        self._gitlab_url = gitlab_url
        self._gitlab_token = gitlab_token
        self._gl = gl

    def get_project_jobs(self, project, **kwargs):
        resources = {"projects": True, "project_id": project.id, "jobs": True}
        objects = get_gl_objects(self._gl, gl_objects.ProjectJob, gl_objects.ProjectJobManager, self._gitlab_url,
                                 self._gitlab_token, resources, project, **kwargs)
        return objects

    def get_project_commits(self, project, **kwargs):
        resources = {"projects": True, "project_id": project.id, "commits": True}
        objects = get_gl_objects(self._gl, gl_objects.ProjectCommit, gl_objects.ProjectCommitManager, self._gitlab_url,
                                 self._gitlab_token, resources, project, **kwargs)
        return objects

    def get_groups(self, **kwargs):
        resources = {"groups": True}
        objects = get_gl_objects(self._gl, gl_objects.Group, gl_objects.GroupManager, self._gitlab_url,
                                 self._gitlab_token, resources, **kwargs)
        return objects

    def get_issues(self, **kwargs):
        resources = {"issues": True}
        objects = get_gl_objects(self._gl, gl_objects.Issue, gl_objects.IssueManager, self._gitlab_url,
                                 self._gitlab_token, resources, **kwargs)
        return objects

    def get_project_issues(self, project, **kwargs):
        resources = {"projects": True, "project_id": project.id, "issues": True}
        objects = get_gl_objects(self._gl, gl_objects.ProjectIssue, gl_objects.ProjectIssueManager, self._gitlab_url,
                                 self._gitlab_token, resources, project, **kwargs)
        return objects

    async def get_project_pipelines(self, project, **kwargs):
        resources = {"projects": True, "project_id": project.id, "pipelines": True}
        objects = await get_gl_objects(self._gl, gl_objects.ProjectPipeline, gl_objects.ProjectPipelineManager,
                                 self._gitlab_url,
                                 self._gitlab_token, resources, project, **kwargs)
        return objects

    async def get_project_pipeline_jobs(self, project, pipeline, **kwargs):
        resources = {"projects": True, "project_id": project.id, "pipelines": True, "pipeline_id" : pipeline.id, "jobs" : True}
        objects = await get_gl_objects(self._gl, gl_objects.ProjectPipeline, gl_objects.ProjectPipelineJobManager,
                                 self._gitlab_url,
                                 self._gitlab_token, resources, project, **kwargs)
        return objects


def build_url(gitlab_url, resources, **kwargs):
    projects = "/projects" if resources.get("projects") else ""
    project_id = f"/{resources.get('project_id')}" if resources.get("project_id") else ""
    jobs = "/jobs" if resources.get("jobs") else ""
    job_id = f"/{resources.get('job_id')}" if resources.get("job_id") else ""
    commits = "/repository/commits" if resources.get("commits") else ""
    branches = "/repository/branches" if resources.get("branches") else ""
    issues = "/issues" if resources.get("issues") else ""
    groups = "/groups" if resources.get("groups") else ""
    group_id = f"/{resources.get('group_id')}" if resources.get("group_id") else ""
    pipelines = "/pipelines" if resources.get("pipelines") else ""
    pipeline_id = f"/{resources.get('pipeline_id')}" if resources.get("pipeline_id") else ""
    url = f"{gitlab_url}/api/v4" \
          f"{groups}" \
          f"{group_id}" \
          f"{projects}" \
          f"{project_id}" \
          f"{issues}" \
          f"{branches}" \
          f"{commits}" \
          f"{pipelines}" \
          f"{pipeline_id}" \
          f"{jobs}" \
          f"{job_id}"

    parameters = f"{urllib.parse.urlencode(kwargs)}"

    print(url)
    return f"{url}?{parameters}" if parameters else url


async def send_req(url, headers, session):
    async with session.get(url, headers=headers) as response:
        return await response.read()


async def reqs_batch(gitlab_url, gitlab_token, current_page, objects, resources, reqs_per_run=10, **kwargs):
    async with ClientSession() as session:
        futures = []
        for i in range(current_page, current_page + reqs_per_run):
            url = build_url(gitlab_url, resources, page=i, per_page=100, **kwargs)
            headers = {"PRIVATE-TOKEN": gitlab_token}
            future = asyncio.ensure_future(send_req(url=url, session=session, headers=headers))
            futures.append(future)
        responses = await asyncio.gather(*futures)
        print("have responses")
        for response in responses:
            resp_objects = json.load(io.BytesIO(response))
            for _object in resp_objects:
                objects.append(_object)


async def fetch(gitlab_url, gitlab_token, resources, reqs_per_run=10, **kwargs):
    objects = []
    more_objs_to_fetch = True
    current_page = 1
    while more_objs_to_fetch:
        loop = asyncio.get_event_loop()
        future = asyncio.ensure_future(
            reqs_batch(gitlab_url, gitlab_token, current_page, objects, resources, reqs_per_run, **kwargs))

        if not loop.is_running():
            loop.run_until_complete(future)

        await future

        if len(objects) == ((current_page + reqs_per_run - 1) * 100):
            current_page += reqs_per_run
        else:
            more_objs_to_fetch = False

    return objects


async def get_gl_objects(gl, gl_obj_class, gl_obj_mgr_class, gitlab_url, gitlab_token, resources, gl_obj_mgr_parent=None,
                   reqs_per_run=10, **kwargs):
    objects = []
    resp_objects = await asyncio.ensure_future(fetch(gitlab_url, gitlab_token, resources, reqs_per_run, **kwargs))
    manager = gl_obj_mgr_class(gl=gl, parent=gl_obj_mgr_parent)
    for _object in resp_objects:
        objects.append(gl_obj_class(manager=manager, attrs=_object))

    return objects



class Gitlab(gitlab.Gitlab):
    def __init__(
            self,
            url,
            private_token,
            oauth_token=None,
            ssl_verify=True,
            http_username=None,
            http_password=None,
            timeout=None,
            api_version="4",
            session=None,
            per_page=None,
    ):
        self.aio = AioGitlab(gl=self, gitlab_url=url, gitlab_token=private_token)
        super(Gitlab, self).__init__(
            url=url,
            private_token=private_token,
            oauth_token=oauth_token,
            ssl_verify=ssl_verify,
            http_username=http_username,
            http_password=http_password,
            timeout=timeout,
            api_version=api_version,
            session=session,
            per_page=per_page,
        )


global table

class Table:
    def __init__(self):
        self.dict = {}

    def insert(self, job_id, testsuite, testcase, timestamp, status, failure_text):
        if testsuite not in self.dict:
            self.dict[testsuite] = {}

        if testcase not in self.dict[testsuite]:
            self.dict[testsuite][testcase] = []

        Row = {"Job ID": job_id,
               "Time" : timestamp,
               "Result" : status,
               "FailureText": failure_text}
        self.dict[testsuite][testcase].append(Row)

    def store(self):
        print(f"Stored")
        for suite in self.dict.keys():
            for case in self.dict[suite].keys():
                df = pd.DataFrame(self.dict[suite][case])
                try:
                    df.to_json(f"./Results/{case}.json")
                except OSError as e:
                    print(f"OSError: {e}")
                    continue

class MyTestCase(TestCase):
    timestamp = Attr()

def do_job(job):
    print(f"do_job {job}")
    result = []
    with tempfile.TemporaryDirectory() as temp_dir:
        file_name = f'{temp_dir}/artifacts_{job.id}.zip'
        with open(file_name, "wb") as f:
            try:
                job.artifacts(streamed=True, action=f.write)
            except gitlab.exceptions.GitlabGetError:
                return result
        with zipfile.ZipFile(file_name) as z:
            for n in z.namelist():
                filename = os.path.basename(n)
                if not filename:
                    continue

                if filename == "junit_report.xml":
                    source = z.open(n)
                    p = f"{temp_dir}/{job.id}"
                    os.makedirs(p, exist_ok=True)
                    target = open(os.path.join(p, filename), "wb")
                    with source, target:
                        shutil.copyfileobj(source, target)
                        try:
                            report = JUnitXml.fromfile(os.path.join(p, filename))
                        except xml.etree.ElementTree.ParseError:
                            continue

                        for suite in report:
                            # handle suites
                            for case in suite:
                                my_case = MyTestCase.fromelem(case)
                                if my_case.result:
                                    # not success
                                    for f in my_case:
                                        if isinstance(f, Failure):
                                            result.append([job.id, suite.name, my_case.name, my_case.timestamp, "failure", f.text])
                                        if isinstance(f, Skipped):
                                            result.append([job.id, suite.name, my_case.name, my_case.timestamp, "skipped", ""])
                                else:
                                    # success test
                                    result.append([job.id, suite.name, my_case.name, my_case.timestamp, "success", ""])
    return result

async def handle_job(worker_n, task_n, gl, project, job):
    print(f"handle job {worker_n} - {task_n} - {job}")
    loop = asyncio.get_running_loop()
    with concurrent.futures.ThreadPoolExecutor() as pool:
        result = await loop.run_in_executor(pool, do_job, job)
        return result

async def handle_pipeline(worker_n, task_n, gl, project, pipeline, job_tasks):
    print(f"handle pipeline {worker_n} - {task_n} - {pipeline}")
    p_jobs = await gl.aio.get_project_pipeline_jobs(project, pipeline)
    jobs = [project.jobs.get(j.id, lazy=True) for j in p_jobs]
    await job_assigner(job_tasks, jobs)

async def job_worker(worker_n, gl, project, job_tasks, results):
    # individual worker task (sometimes called consumer)
    # - sequentially process tasks as they come into the queue
    # and emit the results
    print(f"Job worker: {worker_n} started")
    while True:
        task_n, job = await job_tasks.get()
        result = await handle_job(worker_n, task_n, gl, project, job)
        job_tasks.task_done()
        await results.put(result)

async def pipeline_worker(worker_n, gl, project, pipeline_tasks, job_tasks, results ):
    # individual worker task (sometimes called consumer)
    # - sequentially process tasks as they come into the queue
    # and emit the results
    print(f"Pipeline worker: {worker_n} started")
    while True:
        task_n, pipeline = await pipeline_tasks.get()
        await handle_pipeline(worker_n, task_n, gl, project, pipeline, job_tasks)
        pipeline_tasks.task_done()


async def pipeline_assigner(tasks, pipelines):
    print(f"Pipeline assigner started: {len(pipelines)}")
    task_n = 0
    for p in pipelines:
        task_n += 1
        while tasks.full() == True:
            await asyncio.sleep(0.1)
        await tasks.put((task_n, p))
    print("Assigner stopped")

async def job_assigner(tasks, jobs):
    print(f"Job assigner started: {len(jobs)}")
    task_n = 0
    for j in jobs:
        task_n += 1
        while tasks.full() == True:
            await asyncio.sleep(0.1)
        await tasks.put((task_n, j))
    print("Assigner stopped")


async def displayer(q):
    print("Displayer started")
    # show results of the tasks as they arrive
    while True:
        result = await q.get()
        q.task_done()
        [table.insert(*r) for r in result]
        # if result == None:
        #     continue
        # table.insert(*result)


async def collector(gl, project):
    pipeline_tasks = asyncio.Queue(100)
    job_tasks = asyncio.Queue(100)
    results = asyncio.Queue(100)

    pipelines = await gl.aio.get_project_pipelines(project)

    p_workers = [asyncio.create_task(pipeline_worker(i, gl, project, pipeline_tasks, job_tasks, results)) for i in range(10)]
    j_workers = [asyncio.create_task(job_worker(i, gl, project, job_tasks, results)) for i in range(10)]
    asyncio.create_task(displayer(results))

    await pipeline_assigner(pipeline_tasks, pipelines)
    print("Assigner done")
    await pipeline_tasks.join()
    print("pipeline_tasks done")
    await job_tasks.join()
    print("job_tasks done")
    await results.join()
    print("All done")


    # Cancel our worker tasks.
    for task in p_workers:
        task.cancel()

    for task in j_workers:
        task.cancel()
    # Wait until all worker tasks are cancelled.
    await asyncio.gather(*p_workers, *j_workers, return_exceptions=True)
    # await asyncio.gather(*workers, asyncio.create_task(displayer))
    table.store()

def main():
    global table
    table = Table()
    gl = Gitlab('http://git.erlyvideo.ru', 'XXXXXXXXXXXXXXXXXXXX') #GitLab token should be placed here 
    project = gl.projects.get(1)
    # pipelines = project.pipelines.list()
    # pipeline = project.pipelines.get(69887)
    # jobs = gl.aio.get_project_pipeline_jobs(project, pipeline)
    # print(len(jobs))
    # jobs = pipeline.jobs.list(all=True)
    # print(len(jobs))
        # exit
    asyncio.run(collector(gl, project))


if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        print("Interrupted")
        table.store()
