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
        if dt is None:
            if key in context:
                del context[key]
        else:
            dt = parse_query_dt(dt)
            context[key] = dt
    return context


async def get_traffic_chart_data(app, context):
    await update_traffic_data(app, context)
    repos = context['repos']
    metric = context.get('data_metric', 'count')
    limit = context.get('limit', 10)
    all_dts = set()
    by_counts = {}
    repo_data = {'datasets':[]}
    for i, repo in enumerate(reversed(sorted(repos.values()))):
        repo_slug = repo.repo_slug
        count = repo.traffic_views.total_views
        if not count:
            continue
        if i > limit:
            break
        dts = set((e.timestamp for e in repo.traffic_views.timeline))
        all_dts |= dts
        if count not in by_counts:
            by_counts[count] = {}
        by_counts[count][repo_slug] = repo
    all_dts = list(sorted(all_dts))
    color_iter = iter_colors()
    all_dt_str = [utils.dt_to_str(dt) for dt in all_dts]
    for count, _repos in by_counts.items():
        for repo_slug in sorted(_repos.keys()):
            repo = _repos[repo_slug]
            color = next(color_iter)
            tdata = {
                'label':'{} Total'.format(repo_slug),
                'fill':False,
                'data':[],
                'backgroundColor':color,
                'borderColor':color,
                'lineTension':0,
            }
            if metric == 'count':
                tdata['label'] = '{} Total'.format(repo_slug)
            elif metric == 'uniques':
                tdata['label'] = '{} Uniques'.format(repo_slug)
            timeline = repo.traffic_views.timeline

            entries = {e.timestamp:e for e in timeline}
            for dt, dtstr in zip(all_dts, all_dt_str):
                if dt not in entries:
                    value = None
                else:
                    e = entries[dt]
                    value = getattr(e, metric)
                tdata['data'].append({
                    't':dtstr, 'y':value,
                })
            repo_data['datasets'].append(tdata)
    return repo_data


@aiohttp_jinja2.template('home.html')
async def home(request):
    context = {'DT_FMT':utils.DT_FMT, 'data_metric':'count'}
    return context

async def get_traffic_chart_data_json(request):
    context = update_context_dt_range(request)
    metric = request.query.get('data_metric', 'count')
    assert metric in ['count', 'uniques']
    context['data_metric'] = metric
    chart_data = await get_traffic_chart_data(request.app, context)
    return web.json_response(chart_data)


def create_app(*args):
    app = web.Application()
    app.add_routes([
        web.get('/', home),
        web.get('/traffic-data/', get_traffic_chart_data_json),
    ])
    app.on_startup.append(create_dbstore)
    aiohttp_jinja2.setup(
        app,
        loader=jinja2.FileSystemLoader(os.path.join(BASE_PATH, 'templates'))
    )
    return app

if __name__ == '__main__':
    app = create_app()
    web.run_app(app)
