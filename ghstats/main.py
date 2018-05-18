import asyncio

import jsonfactory

from ghstats.requests import RequestHandler
from ghstats.traffic import AllRepos, Repo
from ghstats.dbstore import DbStore


async def get_data(**kwargs):
    all_repos = AllRepos(**kwargs)
    await all_repos.get_repos()
    await all_repos.get_repo_data()
    return all_repos

async def store_data(all_repos):
    db_store = DbStore()
    await all_repos.store_to_db(db_store)

async def from_db(**kwargs):
    db_store = DbStore()
    all_repos = await AllRepos.from_db(db_store, **kwargs)
    return all_repos

def main():
    loop = asyncio.get_event_loop()
    rh = RequestHandler.from_conf()
    db_store = DbStore()
    all_repos = loop.run_until_complete(
        get_data(request_handler=rh, db_store=db_store)
    )
    return all_repos
