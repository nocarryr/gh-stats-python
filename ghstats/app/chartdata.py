from ghstats import traffic
from ghstats import utils
from ghstats.app.colorutils import iter_colors

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

async def get_repo_referrals(app, context, repo_slug):
    db_store = app['db_store']
    coll = db_store.get_collection(traffic.TrafficReferrer._collection_name)
    filt = traffic.build_datetime_filter('start_datetime', **context)
    filt['repo_slug'] = repo_slug
    pipeline = [
        {'$match':filt},
        {'$group':{
            '_id':'$referrer',
            'referrer':{'$first':'$referrer'},
            'count':{'$sum':'$count'},
            'uniques':{'$sum':'$uniques'},
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
    repos = context['repos']
    hidden_repos = context['hidden_repos']
    metric = context.get('data_metric', 'count')
    limit = context.get('limit', 10)
    return await build_chart_datasets(app, context, metric, limit, hidden_repos)
