import asyncio

import jsonfactory

from ghstats.requests import RequestHandler
from ghstats.traffic import AllRepos, Repo


async def _main(all_repos):
    await all_repos.get_repos()
    await all_repos.get_repo_data()
    return all_repos

def main():
    loop = asyncio.get_event_loop()
    rh = RequestHandler.from_conf()
    all_repos = AllRepos(request_handler=rh)
    loop.run_until_complete(_main(all_repos))
    return all_repos
