import os
import urllib
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
from ghstats.app import templatetags

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
        repo.detail_url = app.router['repo_detail'].url_for(
            repo_slug=urllib.parse.quote_plus(repo.repo_slug),
        )
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

async def get_repo_traffic_paths(app, context, repo_slug):
    db_store = app['db_store']
    coll = db_store.get_collection(traffic.TrafficPathEntry._collection_name)
    filt = traffic.build_datetime_filter('datetime', **context)
    filt['repo_slug'] = repo_slug
    pipeline = [
        {'$match':filt},
        {'$group':{
            '_id':'$path',
            'count':{'$sum':'$count'},
            'uniques':{'$sum':'$uniques'},
            'path':{'$first':'$path'},
            'title':{'$first':'$title'},
        }},
        {'$sort':{'count':-1}},
    ]
    async for doc in coll.aggregate(pipeline):
        yield doc

async def get_repos_by_rank(app, context, metric='count', limit=10):
    repos = context['repos']
    repo_slugs = context.get('repo_slugs')
    db_store = app['db_store']
    coll = db_store.get_collection(traffic.TrafficTimelineEntry._collection_name)
    if not repo_slugs:
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


async def build_chart_datasets(app, context, metric, limit, hidden_repos):
    all_dts = {}
    color_iter = context.get('color_iter', iter_colors())
    dataset_ids = []

    async def build_chart_repo_dataset(repo_doc):
        repo_slug = repo_doc['_id']
        dataset_ids.append(repo_slug)
        async def build_rows():
            async for tl_doc in get_timeline_for_repo(app, context, repo_slug, metric):
                dt = tl_doc['timestamp']
                dt_str = all_dts.get(dt)
                if dt_str is None:
                    dt_str = utils.dt_to_str(dt)
                    all_dts[dt] = dt_str
                yield {'t':dt_str, 'y':tl_doc['value']}

        color = next(color_iter)
        tdata = {
            'label':'{} Total'.format(repo_slug),
            'fill':False,
            'backgroundColor':color,
            'borderColor':color,
            'lineTension':0,
            'spanGaps':True,
            'hidden':repo_slug in hidden_repos,
        }
        if metric == 'count':
            tdata['label'] = '{} Total'.format(repo_slug)
        elif metric == 'uniques':
            tdata['label'] = '{} Uniques'.format(repo_slug)
        tdata['data'] = [d async for d in build_rows()]
        return tdata

    repo_iter = get_repos_by_rank(app, context, metric, limit)
    datasets = [await build_chart_repo_dataset(repo_doc) async for repo_doc in repo_iter]
    start_dt = min(all_dts.keys())
    data = {
        'chart_data':{
            'datasets':datasets,
        },
        'dataset_ids':dataset_ids,
        'start_datetime':all_dts[start_dt],
    }
    return data

async def get_traffic_chart_data(app, context):
    repos = await get_repos(app, context)
    hidden_repos = context['hidden_repos']
    metric = context.get('data_metric', 'count')
    limit = context.get('limit', 10)
    return await build_chart_datasets(app, context, metric, limit, hidden_repos)

@aiohttp_jinja2.template('home.html')
async def home(request):
    context = update_context_dt_range(request)
    repos = await get_repos(request.app, context)
    context.update({
        'request':request,
        'repos':repos,
        'repo_slugs':[],
        'DT_FMT':utils.DT_FMT,
        'data_metric':'count',
        'limit':10,
        'hidden_repos':[],
        'chart_id':'timeline-chart',
        'chart_data_url':'/traffic-data/',
    })
    return context

@aiohttp_jinja2.template('repo_detail.html')
async def repo_detail(request):
    repo_slug = request.match_info['repo_slug']
    repo_slug = urllib.parse.unquote_plus(repo_slug)
    context = update_context_dt_range(request)
    context.update({
        'request':request,
        'repo_slug':repo_slug,
        'repo_slugs':[repo_slug],
        'DT_FMT':utils.DT_FMT,
        'data_metric':'count',
        'limit':10,
        'hidden_repos':[],
        'chart_id':'timeline-chart',
        'chart_data_url':'/combined-data/',
    })
    repos = await get_repos(request.app, context)
    repo = repos[repo_slug]
    context['repo'] = repo
    tp = [doc async for doc in get_repo_traffic_paths(request.app, context, repo_slug)]
    context['traffic_paths'] = tp
    return context

async def prepare_chart_data_view_context(request):
    context = update_context_dt_range(request)
    repo_slugs = request.query.get('repo_slugs', '')
    if not len(repo_slugs):
        repo_slugs = []
    else:
        repo_slugs = repo_slugs.split(',')
    context['repo_slugs'] = repo_slugs
    hidden_repos = request.query.get('hidden_repos', '')
    if not len(hidden_repos):
        hidden_repos = []
    else:
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
    return context

async def get_traffic_chart_data_json(request):
    context = await prepare_chart_data_view_context(request)
    chart_data = await get_traffic_chart_data(request.app, context)
    return web.json_response(chart_data, dumps=utils.jsonfactory.dumps)

async def get_combined_chart_data_json(request):
    context = await prepare_chart_data_view_context(request)
    context['color_iter'] = iter_colors()
    context['data_metric'] = 'count'
    count_data = await get_traffic_chart_data(request.app, context)
    context['data_metric'] = 'uniques'
    unique_data = await get_traffic_chart_data(request.app, context)
    count_data['chart_data']['datasets'].extend(unique_data['chart_data']['datasets'])
    count_data['dataset_ids'].extend(unique_data['dataset_ids'])
    return web.json_response(count_data, dumps=utils.jsonfactory.dumps)

def create_app(*args):
    app = web.Application()
    app.add_routes([
        web.get('/', home),
        web.get(r'/repos/detail/{repo_slug}', repo_detail, name='repo_detail'),
        web.get('/traffic-data/', get_traffic_chart_data_json),
        web.get('/combined-data/', get_combined_chart_data_json),
        web.static('/static', STATIC_ROOT, name='static'),
    ])
    app.on_startup.append(create_dbstore)
    j_env = aiohttp_jinja2.setup(
        app,
        loader=jinja2.FileSystemLoader(os.path.join(BASE_PATH, 'templates'))
    )
    templatetags.setup(j_env)
    return app

def main():
    app = create_app()
    web.run_app(app)

if __name__ == '__main__':
    main()
