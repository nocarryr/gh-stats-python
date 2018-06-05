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
from ghstats.app import chartdata

BASE_PATH = os.path.abspath(os.path.dirname(__file__))
STATIC_ROOT = os.path.join(BASE_PATH, 'static')


def parse_query_dt(o):
    if isinstance(o, datetime.datetime):
        return o
    if isinstance(o, str):
        if o.isalnum():
            o = float(o)
        else:
            if o.count(':') == 1:
                dt_fmt = '%Y-%m-%dT%H:%MZ'
            else:
                dt_fmt = utils.DT_FMT
            return utils.parse_dt(o, dt_fmt)
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

async def prepare_view_context(request):
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
    })
    return context

@aiohttp_jinja2.template('home.html')
async def home(request):
    context = await prepare_view_context(request)
    context.update({
        'chart_id':'timeline-chart',
        'chart_data_url':'/traffic-data/',
    })
    return context

@aiohttp_jinja2.template('repo_detail.html')
async def repo_detail(request):
    repo_slug = request.match_info['repo_slug']
    repo_slug = urllib.parse.unquote_plus(repo_slug)
    context = await prepare_view_context(request)
    context.update({
        'repo_slug':repo_slug,
        'repo_slugs':[repo_slug],
        'chart_id':'timeline-chart',
        'chart_data_url':'/combined-data/',
    })
    repo = context['repos'][repo_slug]
    context['repo'] = repo
    tp = [doc async for doc in chartdata.get_repo_traffic_paths(request.app, context, repo_slug)]
    context['traffic_paths'] = tp
    tr = [doc async for doc in chartdata.get_repo_referrals(request.app, context, repo_slug)]
    context['traffic_referrers'] = tr
    return context

async def prepare_chart_data_view_context(request):
    context = update_context_dt_range(request)
    await get_repos(request.app, context)
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
    chart_data = await chartdata.get_traffic_chart_data(request.app, context)
    return web.json_response(chart_data, dumps=utils.jsonfactory.dumps)

async def get_combined_chart_data_json(request):
    context = await prepare_chart_data_view_context(request)
    context['color_iter'] = iter_colors()
    context['data_metric'] = 'count'
    count_data = await chartdata.get_traffic_chart_data(request.app, context)
    context['data_metric'] = 'uniques'
    unique_data = await chartdata.get_traffic_chart_data(request.app, context)
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
