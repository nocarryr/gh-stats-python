import datetime
import asyncio
import logging
import jsonfactory
import pymongo
from ghstats import utils

logger = logging.getLogger(__name__)

def build_datetime_filter(filter_key, **kwargs):
    start_dt = kwargs.get('start_datetime')
    end_dt = kwargs.get('end_datetime', utils.now())

    if start_dt is None:
        filt = {filter_key:{'$lte':end_dt}}
    else:
        filt = {'$and':[
            {filter_key:{'$gte':start_dt}},
            {filter_key:{'$lte':end_dt}},
        ]}
    return filt

class ApiObject(object):
    _serialize_attrs = []
    _log_collection_name = 'db_update_log'
    def __init__(self, **kwargs):
        self._cached = True
        self._modified = kwargs.get('_modified', True)
        self.request_handler = kwargs.get('request_handler')
        self.db_store = kwargs.get('db_store')
    @property
    def api_path(self):
        return self._get_api_path()
    def _get_api_path(self):
        raise NotImplementedError('Must be defined by subclasses')
    async def log_db_update(self, log_timestamp, collection_name, update_count):
        doc = await self.get_db_update_log(log_timestamp)
        if doc is None:
            created = True
            doc = {
                'log_timestamp':log_timestamp,
                'total_updates':0,
                'collection_updates':{},
            }
        else:
            created = False
            _id = doc['_id']
        doc['total_updates'] += update_count
        if collection_name not in doc['collection_updates']:
            doc['collection_updates'][collection_name] = 0
        doc['collection_updates'][collection_name] += update_count
        if created:
            await self.db_store.add_doc(self._log_collection_name, doc)
        else:
            coll = self.db_store.get_collection(self._log_collection_name)
            await coll.replace_one({'_id':_id}, doc)
    async def get_db_update_log(self, log_timestamp):
        doc = await self.db_store.get_doc(
            self._log_collection_name, {'log_timestamp':log_timestamp}
        )
        return doc
    async def make_request(self, verb, api_path=None, data=None):
        if api_path is None:
            api_path = self.api_path
        cache = await self.get_etag_from_db(verb, api_path)
        if cache is not None:
            headers = {'If-None-Match':cache['etag']}
            self._etag = cache['etag']
        else:
            headers = None
        rh = self.request_handler
        status_code, header_data, resp_data = await rh.make_request(
            verb, api_path, data, headers
        )
        if status_code == 304:
            resp_data = cache['response_data']
            self._cached = True
            self._modified = False
        else:
            doc = await self.update_etag_to_db(verb, api_path, resp_data, header_data)
            self._etag = doc['etag']
            self._cached = False
        return resp_data
    async def get_etag_from_db(self, verb, api_path):
        if self.db_store is None:
            return None
        coll_name = 'request_etags'
        filt = {'verb':verb, 'api_path':api_path}
        return await self.db_store.get_doc(coll_name, filt)
    async def update_etag_to_db(self, verb, api_path, resp_data, header_data):
        if self.db_store is None:
            return
        coll_name = 'request_etags'
        etag = header_data.get('Conditional', {}).get('ETag')
        if etag is None:
            return
        filt = {'verb':verb, 'api_path':api_path}
        doc = {'etag':etag}
        doc.update(filt)
        doc['response_data'] = resp_data
        await self.db_store.update_doc(coll_name, filt, doc)
        return doc
    @classmethod
    async def create_indexes(cls, db_store):
        logger.info('creating indexes for {}...'.format(cls))
        coll = db_store.get_collection('request_etags')
        await coll.create_index(
            [
                ('verb', pymongo.ASCENDING),
                ('api_path', pymongo.ASCENDING),
            ],
            unique=True,
        )
        coll = db_store.get_collection(cls._log_collection_name)
        await coll.create_index('log_timestamp')
        logger.info('{} indexes created'.format(cls))
        for _cls in [Repo, RepoTrafficViews, TrafficTimelineEntry, TrafficPathEntry]:
            logger.info('creating indexes for {}...'.format(_cls))
            await _cls.create_indexes(db_store)
            logger.info('{} indexes created'.format(_cls))
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
    _collection_name = 'repos'
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
                db_store=self.db_store,
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
        if self.db_store is not None:
            await self.store_to_db()
    async def store_to_db(self, log_timestamp=None):
        if log_timestamp is None:
            log_timestamp = utils.now()
        db_store = self.db_store
        await self.log_db_update(log_timestamp, 'repos', 0)
        tasks = []
        for repo in self.repos.values():
            tasks.append(asyncio.ensure_future(repo.store_to_db(log_timestamp)))
        await asyncio.wait(tasks)
        log_doc = await self.get_db_update_log(log_timestamp)
        for coll_name, update_count in log_doc['collection_updates'].items():
            logger.info('{} Updates: {}'.format(coll_name, update_count))
        logger.info('Total Updates: {}'.format(log_doc['total_updates']))
    @classmethod
    async def from_db(cls, db_store, **kwargs):
        coll = db_store.get_collection(cls._collection_name)
        kwargs['db_store'] = db_store
        kwargs['_modified'] = False
        obj = cls(**kwargs)
        async for doc in coll.find():
            rkwargs = {'request_handler':obj.request_handler}
            rkwargs.update(kwargs)
            rkwargs.update(doc)
            repo = await Repo.from_db(**rkwargs)
            obj.repos[repo.api_path] = repo
        return obj


class Repo(ApiObject):
    _serialize_attrs = ['owner', 'name', 'traffic_views', 'traffic_paths']
    _collection_name = 'repos'
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.owner = kwargs.get('owner')
        self.name = kwargs.get('name')
        self.traffic_views = None
        self.traffic_paths = None
    @property
    def repo_slug(self):
        return '{self.owner}/{self.name}'.format(self=self)
    def get_gh_url(self, scheme='https'):
        return '{}://github.com/{}'.format(scheme, self.repo_slug)
    def _get_api_path(self):
        return 'repos/{self.owner}/{self.name}'.format(self=self)
    def _cmp(self, other, op):
        if not isinstance(other, Repo):
            return NotImplemented
        if self.traffic_views is None or other.traffic_views is None:
            return NotImplemented
        return self.traffic_views._cmp(other.traffic_views, op)
    def __eq__(self, other):
        return self._cmp(other, 'eq')
    def __ne__(self, other):
        return self._cmp(other, 'ne')
    def __gt__(self, other):
        return self._cmp(other, 'gt')
    def __lt__(self, other):
        return self._cmp(other, 'lt')
    def _serialize(self, attrs=None):
        d = super()._serialize(attrs)
        tv = d.get('traffic_views')
        if isinstance(tv, dict):
            utils.clean_dict_dt_keys(tv)
        tp = d.get('traffic_paths')
        if isinstance(tp, dict):
            utils.clean_dict_dt_keys(tp)
        return d
    @classmethod
    async def create_indexes(cls, db_store):
        coll = db_store.get_collection(cls._collection_name)
        await coll.create_index('repo_slug', unique=True)
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

        tv = RepoTrafficViews(
            repo=self, per=per, datetime=now, db_store=self.db_store,
        )
        self.traffic_views = tv
        await tv.get_data()
        return tv
    async def get_traffic_paths(self, now=None):
        if now is None:
            now = utils.now()
        now_ts = utils.dt_to_timestamp(now)

        tp = RepoTrafficPaths(repo=self, datetime=now, db_store=self.db_store)
        self.traffic_paths = tp
        await tp.get_data()
        return tp
    async def store_to_db(self, log_timestamp):
        coll_name = self._collection_name
        db_store = self.db_store
        filt = {'repo_slug':self.repo_slug}
        attrs = [a for a in self._serialize_attrs if a not in ['traffic_views', 'traffic_paths']]
        doc = self._serialize(attrs)
        doc['repo_slug'] = self.repo_slug
        if self._modified and not self._cached:
            updated, _id = await db_store.update_doc(coll_name, filt, doc)
            if updated:
                await self.log_db_update(log_timestamp, coll_name, 1)
        tasks = [
            asyncio.ensure_future(self.traffic_views.store_to_db(log_timestamp)),
            asyncio.ensure_future(self.traffic_paths.store_to_db(log_timestamp)),
        ]
        await asyncio.wait(tasks)
    @classmethod
    async def from_db(cls, **kwargs):
        db_store = kwargs.get('db_store')
        load_traffic = kwargs.get('load_traffic', True)
        kwargs['_modified'] = False
        obj = cls(**kwargs)
        kwargs['repo'] = obj
        if load_traffic:
            obj.traffic_views = await RepoTrafficViews.from_db(**kwargs)
            obj.traffic_paths = await RepoTrafficPaths.from_db(**kwargs)
        return obj
    async def traffic_views_from_db_flat(self, **kwargs):
        kwargs['repo'] = self
        kwargs['db_store'] = self.db_store
        self.traffic_views = await RepoTrafficViews.from_db_flat(**kwargs)
    async def traffic_paths_from_db_flat(self, **kwargs):
        kwargs['repo'] = self
        kwargs['db_store'] = self.db_store
        self.traffic_paths = await RepoTrafficPaths.from_db_flat(**kwargs)


class RepoTrafficViews(ApiObject):
    _serialize_attrs = ['total_views', 'total_uniques', 'timeline', 'datetime']
    _collection_name = 'traffic_view_counts'
    def __init__(self, **kwargs):
        self._repo_slug = kwargs.get('repo_slug')
        self.repo = kwargs.get('repo')
        self.per = kwargs.get('per')
        kwargs.setdefault('db_store', self.repo.db_store)
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
    def _cmp(self, other, op):
        if not isinstance(other, RepoTrafficViews):
            return NotImplemented
        result = 0
        if self.total_views < other.total_views:
            result = -1
        if self.total_views > other.total_views:
            result = 1
        if op == 'eq':
            return result == 0
        elif op == 'ne':
            return result != 0
        elif op == 'gt':
            return result == 1
        elif op == 'lt':
            return result == -1
    def __eq__(self, other):
        return self._cmp(other, 'eq')
    def __ne__(self, other):
        return self._cmp(other, 'ne')
    def __gt__(self, other):
        return self._cmp(other, 'gt')
    def __lt__(self, other):
        return self._cmp(other, 'lt')
    async def get_data(self):
        resp_data = await self.make_request('get', data={'per':self.per})
        self.total_views = resp_data['count']
        self.total_uniques = resp_data['uniques']
        for tldata in resp_data['views']:
            tldata.update({'traffic_view':self, 'db_store':self.db_store})
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
    async def store_to_db(self, log_timestamp):
        coll_name = self._collection_name
        db_store = self.db_store
        filt = self.get_db_filter()
        attrs = [a for a in self._serialize_attrs if a not in 'timeline']
        doc = self._serialize(attrs)
        doc['repo_slug'] = self.repo.repo_slug
        if self._modified and not self._cached:
            created = await db_store.add_doc_if_missing(coll_name, filt, doc)
            if created:
                await self.log_db_update(log_timestamp, coll_name, 1)
            await self.store_timeline_to_db(db_store, log_timestamp)
    async def store_timeline_to_db(self, db_store, log_timestamp):
        tasks = []
        for entry in self.timeline:
            task = asyncio.ensure_future(entry.store_to_db(log_timestamp))
            tasks.append(task)
        if len(tasks):
            await asyncio.wait(tasks)
    @classmethod
    async def create_indexes(cls, db_store):
        coll = db_store.get_collection(cls._collection_name)
        await coll.create_indexes([
            pymongo.IndexModel([
                ('repo_slug', pymongo.ASCENDING),
                ('datetime', pymongo.ASCENDING),
            ], unique=True),
        ])
    @classmethod
    def get_db_lookup_filter(cls, **kwargs):
        repo = kwargs.get('repo')
        repo_slug = kwargs.get('repo_slug')
        if repo_slug is None:
            repo_slug = repo.repo_slug
        filt = build_datetime_filter('datetime', **kwargs)
        filt['repo_slug'] = repo_slug
        return filt
    @classmethod
    async def from_db(cls, **kwargs):
        db_store = kwargs.get('db_store')
        filt = cls.get_db_lookup_filter(**kwargs)
        repo_slug = filt['repo_slug']
        coll = db_store.get_collection(cls._collection_name)
        results = {}
        kwargs['_modified'] = False
        async for doc in coll.find(filt):
            okwargs = kwargs.copy()
            okwargs.update(doc)
            okwargs['datetime'] = utils.make_aware(okwargs['datetime'])
            obj = cls(**okwargs)
            await obj.get_timeline_from_db()
            results[obj.datetime] = obj
        return results
    @classmethod
    async def from_db_flat(cls, **kwargs):
        kwargs['_modified'] = False
        obj = cls(**kwargs)
        await obj.get_timeline_from_db(**kwargs)
        obj.total_views = 0
        obj.total_uniques = 0
        for entry in obj.timeline:
            obj.total_views += entry.count
            obj.total_uniques += entry.uniques
        return obj
    async def get_timeline_from_db(self, **kwargs):
        kwargs['traffic_view'] = self
        kwargs['db_store'] = self.db_store
        self.timeline = await TrafficTimelineEntry.from_db(**kwargs)

class TrafficTimelineEntry(ApiObject):
    _serialize_attrs = ['count', 'uniques', 'timestamp']
    _collection_name = 'traffic_view_timeline'
    def __init__(self, **kwargs):
        self.traffic_view = kwargs.get('traffic_view')
        kwargs.setdefault('db_store', self.traffic_view.db_store)
        kwargs.setdefault('_modified', self.traffic_view._modified)
        super().__init__(**kwargs)
        self.count = kwargs.get('count')
        self.uniques = kwargs.get('uniques')
        self.timestamp = kwargs.get('timestamp')
    @property
    def repo_slug(self):
        return self.traffic_view.repo_slug
    def _get_api_path(self):
        return self.traffic_view.api_path
    @classmethod
    async def create_indexes(cls, db_store):
        coll = db_store.get_collection(cls._collection_name)
        await coll.create_indexes([
            pymongo.IndexModel(
                [
                    ('repo_slug', pymongo.ASCENDING),
                    ('datetime', pymongo.ASCENDING),
                ],
            ),
            pymongo.IndexModel(
                [
                    ('repo_slug', pymongo.ASCENDING),
                    ('timestamp', pymongo.ASCENDING),
                ],
            ),
            pymongo.IndexModel(
                [
                    ('repo_slug', pymongo.ASCENDING),
                    ('datetime', pymongo.ASCENDING),
                    ('timestamp', pymongo.ASCENDING),
                ],
                unique=True,
            ),
        ])
    async def store_to_db(self, log_timestamp):
        coll_name = self._collection_name
        db_store = self.db_store
        doc = {
            'repo_slug':self.repo_slug,
            'count':self.count,
            'uniques':self.uniques,
            'timestamp':self.timestamp,
            'datetime':self.traffic_view.datetime,
        }
        filt = {'datetime':self.traffic_view.datetime, 'repo_slug':self.repo_slug}
        if self.traffic_view._modified and not self.traffic_view._cached:
            created = await db_store.add_doc_if_missing(coll_name, filt, doc)
            if created:
                await self.log_db_update(log_timestamp, coll_name, 1)
    @classmethod
    async def from_db(cls, **kwargs):
        db_store = kwargs.get('db_store')
        traffic_view = kwargs.get('traffic_view')
        tl_coll_name = 'traffic_view_timeline'
        tl_coll = db_store.get_collection(tl_coll_name)
        tl_keys = ['count', 'timestamp', 'uniques']
        tl_filt = {'repo_slug':traffic_view.repo_slug}

        if traffic_view.datetime is not None:
            tl_filt['datetime'] = traffic_view.datetime
        else:
            tl_filt.update(build_datetime_filter('timestamp', **kwargs))

        results = []
        async for tl_doc in tl_coll.find(tl_filt, sort=[('timestamp', pymongo.ASCENDING)]):
            tl_doc['timestamp'] = utils.make_aware(tl_doc['timestamp'])
            tlkwargs = {
                'traffic_view':traffic_view,
                'db_store':db_store,
                '_modified':False,
            }
            tlkwargs.update(tl_doc)
            results.append(cls(**tlkwargs))
        return results
    def __str__(self):
        return '{self.timestamp} - {self.count}'.format(self=self)

class RepoTrafficPaths(ApiObject):
    _serialize_attrs = ['data', 'datetime']
    _collection_name = 'traffic_view_paths'
    def __init__(self, **kwargs):
        self._repo_slug = kwargs.get('repo_slug')
        self.repo = kwargs.get('repo')
        kwargs.setdefault('db_store', self.repo.db_store)
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
            d.update({'traffic_path':self, 'db_store':self.db_store})
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
    async def store_to_db(self, log_timestamp):
        db_store = self.db_store
        tasks = []
        for entry in self.data:
            task = asyncio.ensure_future(entry.store_to_db(log_timestamp))
            tasks.append(task)
        if len(tasks):
            await asyncio.wait(tasks)
    @classmethod
    def get_db_lookup_filter(cls, **kwargs):
        repo = kwargs.get('repo')
        repo_slug = kwargs.get('repo_slug')
        if repo_slug is None:
            repo_slug = repo.repo_slug
        filt = build_datetime_filter('datetime', **kwargs)
        filt['repo_slug'] = repo_slug
        return filt
    @classmethod
    async def from_db(cls, **kwargs):
        filt = cls.get_db_lookup_filter(**kwargs)
        repo_slug = filt['repo_slug']
        db_store = kwargs.get('db_store')
        kwargs['_modified'] = False
        coll = db_store.get_collection(cls._collection_name)
        results = {}
        keys = await coll.distinct('datetime', filt)
        for key in keys:
            key = utils.make_aware(key)
            okwargs = kwargs.copy()
            okwargs['datetime'] = key
            obj = cls(**okwargs)
            await obj.get_data_from_db()
            results[key] = obj
        return results
    @classmethod
    async def from_db_flat(cls, **kwargs):
        kwargs['_modified'] = False
        obj = cls(**kwargs)
        await obj.get_data_from_db(**kwargs)
        return obj
    async def get_data_from_db(self, **kwargs):
        kwargs['traffic_path'] = self
        kwargs['db_store'] = self.db_store
        self.data = await TrafficPathEntry.from_db(**kwargs)

class TrafficPathEntry(ApiObject):
    _serialize_attrs = ['path', 'count', 'uniques', 'title']
    _collection_name = 'traffic_view_paths'
    def __init__(self, **kwargs):
        self.traffic_path = kwargs.get('traffic_path')
        kwargs.setdefault('_modified', self.traffic_path._modified)
        kwargs.setdefault('db_store', self.traffic_path.db_store)
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
    @classmethod
    async def create_indexes(cls, db_store):
        coll = db_store.get_collection(cls._collection_name)
        await coll.create_indexes([
            pymongo.IndexModel(
                [
                    ('repo_slug', pymongo.ASCENDING),
                    ('datetime', pymongo.ASCENDING),
                ],
            ),
            pymongo.IndexModel(
                [
                    ('repo_slug', pymongo.ASCENDING),
                    ('datetime', pymongo.ASCENDING),
                    ('path', pymongo.ASCENDING),
                ],
                unique=True,
            ),
        ])
    async def store_to_db(self, log_timestamp):
        coll_name = self._collection_name
        db_store = self.db_store
        filt = self.traffic_path.get_db_filter()
        doc = {'repo_slug':self.repo_slug, 'datetime':self.traffic_path.datetime}
        doc.update(self._serialize())
        if self.traffic_path._modified and not self.traffic_path._cached:
            updated, _ = await db_store.update_doc(coll_name, filt, doc)
            if updated:
                await self.log_db_update(log_timestamp, coll_name, 1)
    @classmethod
    async def from_db(cls, **kwargs):
        db_store = kwargs.get('db_store')
        traffic_path = kwargs.get('traffic_path')
        coll = db_store.get_collection(cls._collection_name)
        obj_filt = {'repo_slug':traffic_path.repo_slug}
        if traffic_path.datetime is not None:
            obj_filt['datetime'] = traffic_path.datetime
        else:
            obj_filt.update(build_datetime_filter('timestamp', **kwargs))
        results = []
        async for doc in coll.find(obj_filt):
            ekwargs = {
                'traffic_path':traffic_path,
                'db_store':db_store,
                '_modified':False,
            }
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
