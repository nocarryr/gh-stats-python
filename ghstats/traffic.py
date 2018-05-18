import datetime
import asyncio
import jsonfactory
import pymongo
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
    def _serialize(self, attrs=None):
        if attrs is None:
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
    async def store_to_db(self, db_store):
        tasks = []
        for repo in self.repos.values():
            tasks.append(asyncio.ensure_future(repo.store_to_db(db_store)))
        await asyncio.wait(tasks)
    @classmethod
    async def from_db(cls, db_store, **kwargs):
        coll_name = 'repos'
        coll = db_store.get_collection(coll_name)
        obj = cls(**kwargs)
        async for doc in coll.find():
            rkwargs = {'request_handler':obj.request_handler}
            rkwargs.update(kwargs)
            rkwargs.update(doc)
            repo = await Repo.from_db(db_store, **rkwargs)
            obj.repos[repo.api_path] = repo
        return obj


class Repo(ApiObject):
    _serialize_attrs = ['owner', 'name', 'traffic_views', 'traffic_paths']
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.owner = kwargs.get('owner')
        self.name = kwargs.get('name')
        self.traffic_views = None
        self.traffic_paths = None
    @property
    def repo_slug(self):
        return '{self.owner}/{self.name}'.format(self=self)
    def _get_api_path(self):
        return 'repos/{self.owner}/{self.name}'.format(self=self)
    def _serialize(self, attrs=None):
        d = super()._serialize(attrs)
        tv = d.get('traffic_views')
        if isinstance(tv, dict):
            utils.clean_dict_dt_keys(tv)
        tp = d.get('traffic_paths')
        if isinstance(tp, dict):
            utils.clean_dict_dt_keys(tp)
        return d
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
    async def store_to_db(self, db_store):
        coll_name = 'repos'
        filt = {'repo_slug':self.repo_slug}
        attrs = [a for a in self._serialize_attrs if a not in ['traffic_views', 'traffic_paths']]
        doc = self._serialize(attrs)
        doc['repo_slug'] = self.repo_slug
        result = await db_store.update_doc(coll_name, filt, doc)
        tasks = [
            asyncio.ensure_future(self.traffic_views.store_to_db(db_store)),
            asyncio.ensure_future(self.traffic_paths.store_to_db(db_store)),
        ]
        await asyncio.wait(tasks)
    @classmethod
    async def from_db(cls, db_store, **kwargs):
        obj = cls(**kwargs)
        kwargs['repo'] = obj
        obj.traffic_views = await RepoTrafficViews.from_db(db_store, **kwargs)
        obj.traffic_paths = await RepoTrafficPaths.from_db(db_store, **kwargs)
        return obj


class RepoTrafficViews(ApiObject):
    _serialize_attrs = ['total_views', 'total_uniques', 'timeline', 'datetime']
    def __init__(self, **kwargs):
        self._repo_slug = kwargs.get('repo_slug')
        self.repo = kwargs.get('repo')
        self.per = kwargs.get('per')
        kwargs['request_handler'] = self.repo.request_handler
        super().__init__(**kwargs)
        self.total_views = kwargs.get('total_views')
        self.total_uniques = kwargs.get('total_uniques')
        self.timeline = []
        self.datetime = kwargs.get('datetime')
    @property
    def repo_slug(self):
        s = self._repo_slug
        if s is None:
            s = self._repo_slug = self.repo.repo_slug
        return s
    @repo_slug.setter
    def repo_slug(self, value):
        self._repo_slug = value
    def _get_api_path(self):
        return '{self.repo.api_path}/traffic/views'.format(self=self)
    async def get_data(self):
        resp_data = await self.make_request('get', data={'per':self.per})
        self.total_views = resp_data['count']
        self.total_uniques = resp_data['uniques']
        for tldata in resp_data['views']:
            tldata['traffic_view'] = self
            entry = TrafficTimelineEntry(**tldata)
            self.timeline.append(entry)
    def get_db_filter(self):
        td = datetime.timedelta(hours=1)
        dt_range = [self.datetime - td, self.datetime + td]
        filt = {
            'repo_slug':self.repo.repo_slug,
            '$and':[
                {'datetime':{'$gte':dt_range[0]}},
                {'datetime':{'$lt':dt_range[1]}},
            ],
        }
        return filt
    async def store_to_db(self, db_store):
        coll_name = 'traffic_view_counts'
        filt = self.get_db_filter()
        attrs = [a for a in self._serialize_attrs if a not in 'timeline']
        doc = self._serialize(attrs)
        doc['repo_slug'] = self.repo.repo_slug
        await db_store.add_doc_if_missing(coll_name, filt, doc)
        await self.store_timeline_to_db(db_store)
    async def store_timeline_to_db(self, db_store):
        tasks = []
        for entry in self.timeline:
            task = asyncio.ensure_future(entry.store_to_db(db_store))
            tasks.append(task)
        if len(tasks):
            await asyncio.wait(tasks)
    @classmethod
    def get_db_lookup_filter(cls, **kwargs):
        repo = kwargs.get('repo')
        repo_slug = kwargs.get('repo_slug')
        if repo_slug is None:
            repo_slug = repo.repo_slug
        start_dt = kwargs.get('start_datetime')
        end_dt = kwargs.get('end_datetime', utils.now())

        if start_dt is None:
            filt = {'datetime':{'$lte':end_dt}}
        else:
            filt = {'$and':[
                {'datetime':{'$gte':start_dt}},
                {'datetime':{'$lte':end_dt}},
            ]}
        filt['repo_slug'] = repo_slug
        return filt
    @classmethod
    async def from_db(cls, db_store, **kwargs):
        coll_name = 'traffic_view_counts'
        filt = cls.get_db_lookup_filter(**kwargs)
        repo_slug = filt['repo_slug']
        coll = db_store.get_collection(coll_name)
        results = {}
        async for doc in coll.find(filt):
            okwargs = kwargs.copy()
            okwargs.update(doc)
            okwargs['datetime'] = utils.make_aware(okwargs['datetime'])
            obj = cls(**okwargs)
            await obj.get_timeline_from_db(db_store)
            results[obj.datetime] = obj
        return results
    async def get_timeline_from_db(self, db_store):
        self.timeline = await TrafficTimelineEntry.from_db(db_store, traffic_view=self)

class TrafficTimelineEntry(ApiObject):
    _serialize_attrs = ['count', 'uniques', 'timestamp']
    def __init__(self, **kwargs):
        self.traffic_view = kwargs.get('traffic_view')
        super().__init__(**kwargs)
        self.count = kwargs.get('count')
        self.uniques = kwargs.get('uniques')
        self.timestamp = kwargs.get('timestamp')
    @property
    def repo_slug(self):
        return self.traffic_view.repo_slug
    def _get_api_path(self):
        return self.traffic_view.api_path
    async def store_to_db(self, db_store):
        coll_name = 'traffic_view_timeline'
        doc = {
            'repo_slug':self.repo_slug,
            'count':self.count,
            'uniques':self.uniques,
            'timestamp':self.timestamp,
            'datetime':self.traffic_view.datetime,
        }
        filt = {'datetime':self.traffic_view.datetime, 'repo_slug':self.repo_slug}
        await db_store.add_doc_if_missing(coll_name, filt, doc)
    @classmethod
    async def from_db(cls, db_store, **kwargs):
        traffic_view = kwargs.get('traffic_view')
        tl_coll_name = 'traffic_view_timeline'
        tl_coll = db_store.get_collection(tl_coll_name)
        tl_keys = ['count', 'timestamp', 'uniques']
        tl_filt = {
            'datetime':traffic_view.datetime,
            'repo_slug':traffic_view.repo_slug,
        }
        results = []
        async for tl_doc in tl_coll.find(tl_filt, sort=[('timestamp', pymongo.ASCENDING)]):
            tl_doc['timestamp'] = utils.make_aware(tl_doc['timestamp'])
            tlkwargs = {'traffic_view':traffic_view}
            tlkwargs.update(tl_doc)
            results.append(cls(**tlkwargs))
        return results
    def __str__(self):
        return '{self.timestamp} - {self.count}'.format(self=self)

class RepoTrafficPaths(ApiObject):
    _serialize_attrs = ['data', 'datetime']
    def __init__(self, **kwargs):
        self._repo_slug = kwargs.get('repo_slug')
        self.repo = kwargs.get('repo')
        kwargs['request_handler'] = self.repo.request_handler
        super().__init__(**kwargs)
        self.datetime = kwargs.get('datetime')
        self.data = []
    @property
    def repo_slug(self):
        s = self._repo_slug
        if s is None:
            s = self._repo_slug = self.repo.repo_slug
        return s
    @repo_slug.setter
    def repo_slug(self, value):
        self._repo_slug = value
    def _get_api_path(self):
        return '{self.repo.api_path}/traffic/popular/paths'.format(self=self)
    async def get_data(self):
        resp_data = await self.make_request('get')
        for d in resp_data:
            d['traffic_path'] = self
            self.data.append(TrafficPathEntry(**d))
    def get_db_filter(self):
        td = datetime.timedelta(days=14)
        dt_range = [self.datetime - td, self.datetime]
        filt = {
            'repo_slug':self.repo.repo_slug,
            '$and':[
                {'datetime':{'$gt':dt_range[0]}},
                {'datetime':{'$lte':dt_range[1]}},
            ],
        }
        return filt
    async def store_to_db(self, db_store):
        tasks = []
        for entry in self.data:
            task = asyncio.ensure_future(entry.store_to_db(db_store))
            tasks.append(task)
        if len(tasks):
            await asyncio.wait(tasks)
    @classmethod
    def get_db_lookup_filter(cls, **kwargs):
        repo = kwargs.get('repo')
        repo_slug = kwargs.get('repo_slug')
        if repo_slug is None:
            repo_slug = repo.repo_slug
        start_dt = kwargs.get('start_datetime')
        end_dt = kwargs.get('end_datetime', utils.now())

        if start_dt is None:
            filt = {'datetime':{'$lte':end_dt}}
        else:
            filt = {'$and':[
                {'datetime':{'$gte':start_dt}},
                {'datetime':{'$lte':end_dt}},
            ]}
        filt['repo_slug'] = repo_slug
        return filt
    @classmethod
    async def from_db(cls, db_store, **kwargs):
        coll_name = 'traffic_view_paths'
        filt = cls.get_db_lookup_filter(**kwargs)
        repo_slug = filt['repo_slug']

        coll = db_store.get_collection(coll_name)
        results = {}
        keys = await coll.distinct('datetime', filt)
        for key in keys:
            key = utils.make_aware(key)
            okwargs = kwargs.copy()
            okwargs['datetime'] = key
            obj = cls(**okwargs)
            await obj.get_data_from_db(db_store)
            results[key] = obj
        return results
    async def get_data_from_db(self, db_store):
        self.data = await TrafficPathEntry.from_db(db_store, traffic_path=self)

class TrafficPathEntry(ApiObject):
    _serialize_attrs = ['path', 'count', 'uniques', 'title']
    def __init__(self, **kwargs):
        self.traffic_path = kwargs.get('traffic_path')
        super().__init__(**kwargs)
        self.path = kwargs.get('path')
        self.count = kwargs.get('count')
        self.uniques = kwargs.get('uniques')
        self.title = kwargs.get('title')
    @property
    def repo_slug(self):
        return self.traffic_path.repo_slug
    def _get_api_path(self):
        return self.traffic_path.api_path
    async def store_to_db(self, db_store):
        coll_name = 'traffic_view_paths'
        filt = self.traffic_path.get_db_filter()
        doc = {'repo_slug':self.repo_slug, 'datetime':self.traffic_path.datetime}
        doc.update(self._serialize())
        await db_store.update_doc(coll_name, filt, doc)
    @classmethod
    async def from_db(cls, db_store, **kwargs):
        traffic_path = kwargs.get('traffic_path')
        coll_name = 'traffic_view_paths'
        coll = db_store.get_collection(coll_name)
        obj_filt = {
            'datetime':traffic_path.datetime,
            'repo_slug':traffic_path.repo_slug,
        }
        results = []
        async for doc in coll.find(obj_filt):
            ekwargs = {'traffic_path':traffic_path}
            ekwargs.update(doc)
            results.append(cls(**ekwargs))
        return results
    def __str__(self):
        return self.path

@jsonfactory.encoder
def json_encode(o):
    if isinstance(o, ApiObject):
        d = {'__class__':o.__class__.__name__}
        d.update(o._serialize())
        return d
    return None
