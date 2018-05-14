import asyncio
import jsonfactory
from ghstats import utils

class ApiObject(object):
    _serialize_attrs = []
    def __init__(self, **kwargs):
        self.request_handler = kwargs.get('request_handler')
    @property
    def api_path(self):
        return self._get_api_path()
    def _get_api_path(self):
        raise NotImplementedError('Must be defined by subclasses')
    async def make_request(self, verb, api_path=None, data=None):
        if api_path is None:
            api_path = self.api_path
        resp = await self.request_handler.make_request(verb, api_path, data)
        return resp
    def _serialize(self):
        attrs = self._serialize_attrs
        return {attr:getattr(self, attr) for attr in attrs}
    def __repr__(self):
        return '<{self.__class__.__name__}: {self}>'.format(self=self)
    def __str__(self):
        return self.api_path

class AllRepos(ApiObject):
    _serialize_attrs = ['repos']
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.repos = {}
    def _get_api_path(self):
        return 'user/repos'
    async def get_repos(self):
        resp_data = await self.make_request('get')
        for repo_data in resp_data:
            repo = Repo(
                owner=repo_data['owner']['login'],
                name=repo_data['name'],
                request_handler=self.request_handler,
            )
            self.repos[repo.api_path] = repo
        return self.repos
    async def get_repo_data(self, now=None):
        if now is None:
            now = utils.now()
        tasks = []
        for repo in self.repos.values():
            tasks.append(asyncio.ensure_future(repo.get_data(now=now)))
        await asyncio.wait(tasks)

class Repo(ApiObject):
    _serialize_attrs = ['owner', 'name', 'traffic_views', 'traffic_paths']
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.owner = kwargs.get('owner')
        self.name = kwargs.get('name')
        self.traffic_views = {}
        self.traffic_paths = {}
    def _get_api_path(self):
        return 'repos/{self.owner}/{self.name}'.format(self=self)
    async def get_data(self, now=None):
        if now is None:
            now = utils.now()
        tasks = [
            asyncio.ensure_future(self.get_traffic_views(now=now)),
            asyncio.ensure_future(self.get_traffic_paths(now=now)),
        ]
        await asyncio.wait(tasks)
    async def get_traffic_views(self, per='day', now=None):
        if now is None:
            now = utils.now()
        now_ts = utils.dt_to_timestamp(now)

        tv = RepoTrafficViews(repo=self, per=per, datetime=now)
        self.traffic_views = tv
        await tv.get_data()
        return tv
    async def get_traffic_paths(self, now=None):
        if now is None:
            now = utils.now()
        now_ts = utils.dt_to_timestamp(now)

        tp = RepoTrafficPaths(repo=self, datetime=now)
        self.traffic_paths = tp
        await tp.get_data()
        return tp

class RepoTrafficViews(ApiObject):
    _serialize_attrs = ['total_views', 'total_uniques', 'timeline', 'datetime']
    def __init__(self, **kwargs):
        self.repo = kwargs.get('repo')
        self.per = kwargs.get('per')
        kwargs['request_handler'] = self.repo.request_handler
        super().__init__(**kwargs)
        self.total_views = None
        self.total_uniques = None
        self.timeline = []
        self.datetime = kwargs.get('datetime')
    def _get_api_path(self):
        return '{self.repo.api_path}/traffic/views'.format(self=self)
    async def get_data(self):
        resp_data = await self.make_request('get', data={'per':self.per})
        self.total_views = resp_data['count']
        self.total_uniques = resp_data['uniques']
        self.timeline = resp_data['views']

class RepoTrafficPaths(ApiObject):
    _serialize_attrs = ['data', 'datetime']
    def __init__(self, **kwargs):
        self.repo = kwargs.get('repo')
        kwargs['request_handler'] = self.repo.request_handler
        super().__init__(**kwargs)
        self.datetime = kwargs.get('datetime')
        self.data = []
    def _get_api_path(self):
        return '{self.repo.api_path}/traffic/popular/paths'.format(self=self)
    async def get_data(self):
        resp_data = await self.make_request('get')
        self.data = resp_data

@jsonfactory.encoder
def json_encode(o):
    if isinstance(o, ApiObject):
        d = {'__class__':o.__class__.__name__}
        d.update(o._serialize())
        return d
    return None
