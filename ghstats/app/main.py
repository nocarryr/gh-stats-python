import os
import datetime
import numbers
import asyncio
from aiohttp import web
import aiohttp_jinja2
import jinja2
import pymongo

from ghstats import traffic
from ghstats.dbstore import DbStore
from ghstats import utils
from ghstats.app.colorutils import iter_colors

BASE_PATH = os.path.abspath(os.path.dirname(__file__))
STATIC_ROOT = os.path.join(BASE_PATH, 'static')


def parse_query_dt(o):
    if isinstance(o, datetime.datetime):
        return o
    if isinstance(o, str):
        if o.isalnum():
            o = float(o)
        else:
            return utils.parse_dt(o)
    if isinstance(o, numbers.Number):
        return utils.timestamp_to_dt(o)
    raise ValueError('Could not parse datetime from {}'.format(repr(o)))


async def create_dbstore(app):
    app['db_store'] = DbStore()


async def get_repos(app, context):
    if 'repos' in context:
        return context['repos']
    coll_name = traffic.Repo._collection_name
    coll = app['db_store'].get_collection(coll_name)
    d = {}
    async for doc in coll.find():
        kw = {'db_store':app['db_store']}
        kw.update(doc)
        repo = await traffic.Repo.from_db(load_traffic=False, **kw)
        d[repo.repo_slug] = repo
    context['repos'] = d
    return d

async def update_traffic_data(app, context):
    repos = await get_repos(app, context)
    tasks = []
    for repo in repos.values():
        task = asyncio.ensure_future(repo.traffic_views_from_db_flat(**context))
        tasks.append(task)
        task = asyncio.ensure_future(repo.traffic_paths_from_db_flat(**context))
        tasks.append(task)
    await asyncio.wait(tasks)
    for repo in repos.values():
        assert repo.traffic_views is not None

def update_context_dt_range(request, context=None):
    if context is None:
        context = {}
    for key in ['start_datetime', 'end_datetime']:
        dt = request.query.get(key)
        if isinstance(dt, str) and not len(dt):
            dt = None
        if dt is None:
            if key in context:
                del context[key]
        else:
            if isinstance(dt, str) and not dt.endswith('Z'):
                dt = '{}Z'.format(dt)
            dt = parse_query_dt(dt)
            context[key] = dt
            context['{}_str'.format(key)] = utils.dt_to_str(dt)
    if not context.get('end_datetime'):
        now = utils.now()
        context['end_datetime'] = now
        context['end_datetime_str'] = utils.dt_to_str(now)
    return context

async def get_repos_by_rank(app, context, metric='count', limit=10):
    repos = context['repos']
    db_store = app['db_store']
    coll = db_store.get_collection(traffic.TrafficTimelineEntry._collection_name)
    repo_slugs = [repo.repo_slug for repo in repos.values()]
    pipeline = [
        {'$match':{'repo_slug':{'$in':repo_slugs}}},
        {'$group':{
            '_id':'$repo_slug',
            'total':{'$sum':'${}'.format(metric)},
        }},
        {'$sort':{'total':-1}},
        {'$limit':limit},
    ]
    async for doc in coll.aggregate(pipeline):
        yield doc

async def get_timeline_for_repo(app, context, repo, metric):
    db_store = app['db_store']
    if isinstance(repo, traffic.Repo):
        repo_slug = repo.repo_slug
    else:
        repo_slug = repo
    coll = db_store.get_collection(traffic.TrafficTimelineEntry._collection_name)
    filt = traffic.build_datetime_filter('timestamp', **context)
    filt['repo_slug'] = repo_slug
    pipeline = [
        {'$match':filt},
        {'$group':{
            '_id':'$timestamp',
            'value':{'$max':'${}'.format(metric)},
        }},
        {'$sort':{'_id':1}},
    ]
    async for doc in coll.aggregate(pipeline):
        doc['timestamp'] = utils.make_aware(doc['_id'])
        yield doc


async def get_traffic_chart_data(app, context):
    repos = await get_repos(app, context)
    hidden_repos = context['hidden_repos']
    metric = context.get('data_metric', 'count')
    limit = context.get('limit', 10)
    all_dts = {}
    by_counts = {}
    data = {'dataset_ids':[]}
    chart_data = {'datasets':[]}
    color_iter = iter_colors()
    async for repo_doc in get_repos_by_rank(app, context, metric, limit):
        repo_slug = repo_doc['_id']
        color = next(color_iter)
        tdata = {
            'label':'{} Total'.format(repo_slug),
            'fill':False,
            'data':[],
            'backgroundColor':color,
            'borderColor':color,
            'lineTension':0,
            'spanGaps':True,
            'hidden':repo_slug in hidden_repos,
        }
        data['dataset_ids'].append(repo_slug)
        if metric == 'count':
            tdata['label'] = '{} Total'.format(repo_slug)
        elif metric == 'uniques':
            tdata['label'] = '{} Uniques'.format(repo_slug)
        async for tl_doc in get_timeline_for_repo(app, context, repo_slug, metric):
            dt = tl_doc['timestamp']
            dt_str = all_dts.get(dt)
            if dt_str is None:
                dt_str = utils.dt_to_str(dt)
                all_dts[dt] = dt_str
            tdata['data'].append({'t':dt_str, 'y':tl_doc['value']})
        chart_data['datasets'].append(tdata)
    start_dt = min(all_dts.keys())
    data['start_datetime'] = all_dts[start_dt]
    data['chart_data'] = chart_data
    return data


@aiohttp_jinja2.template('home.html')
async def home(request):
    context = update_context_dt_range(request)
    context.update({
        'request':request,
        'DT_FMT':utils.DT_FMT,
        'data_metric':'count',
        'limit':10,
        'hidden_repos':'',
    })
    return context

async def get_traffic_chart_data_json(request):
    context = update_context_dt_range(request)
    hidden_repos = request.query.get('hidden_repos', '')
    hidden_repos = hidden_repos.split(',')
    context['hidden_repos'] = hidden_repos
    metric = request.query.get('data_metric', 'count')
    assert metric in ['count', 'uniques']
    context['data_metric'] = metric
    limit = request.query.get('limit', 10)
    if isinstance(limit, str):
        assert limit.isalnum()
        limit = int(limit)
    context['limit'] = limit
    chart_data = await get_traffic_chart_data(request.app, context)
    return web.json_response(chart_data, dumps=utils.jsonfactory.dumps)


def create_app(*args):
    app = web.Application()
    app.add_routes([
        web.get('/', home),
        web.get('/traffic-data/', get_traffic_chart_data_json),
        web.static('/static', STATIC_ROOT, name='static'),
    ])
    app.on_startup.append(create_dbstore)
    aiohttp_jinja2.setup(
        app,
        loader=jinja2.FileSystemLoader(os.path.join(BASE_PATH, 'templates'))
    )
    return app

def main():
    app = create_app()
    web.run_app(app)

if __name__ == '__main__':
    main()
