import os
import aiohttp
import yaml
import logging
import jsonfactory
from ghstats import utils

API_ENDPOINT = 'https://api.github.com'

CONF_FILENAME = '~/.github-auth.yaml'

logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.DEBUG,
)

logger = logging.getLogger(__name__)

def log_request(verb, url, resp_data):
    d = {'verb':verb, 'url':url, 'response':resp_data}
    txt_data = jsonfactory.dumps(d)
    filename = 'request_data.log'
    with open(filename, 'a') as f:
        f.write('{}\n\n'.format(txt_data))

class RequestHandler(object):
    def __init__(self, **kwargs):
        self.username = kwargs.get('username')
        self.password = kwargs.get('password')
        self.token = kwargs.get('token')
        self._session = None
        self._acquire_count = 0
    @classmethod
    def from_conf(cls, filename=CONF_FILENAME):
        filename = os.path.expanduser(filename)
        with open(filename, 'r') as f:
            s = f.read()
        data = yaml.load(s)
        return cls(**data)
    @property
    def session(self):
        s = self._session
        if s is not None and s.closed:
            s = None
        if s is None:
            skwargs = {}
            if self.token is not None:
                skwargs['headers'] = {'Authorization':'token {}'.format(self.token)}
            elif self.username is not None and self.password is not None:
                skwargs['auth'] = aiohttp.BasicAuth(
                    login=self.username, password=self.password,
                )
            s = self._session = aiohttp.ClientSession(**skwargs)
        return s
    def parse_link_headers(self, headers):
        d = {}
        link_headers = headers.getall('Link', [])
        for link_header in link_headers:
            for lstr in link_header.split(','):
                u, rel = lstr.split(';')
                u = u.strip(' ').lstrip('<').rstrip('>')
                rel = rel.strip(' ').split('=')[1].strip('"')
                d[rel] = u
        return d
    async def _do_request(self, verb, url, data=None):
        req_kwargs = {}
        if data:
            req_kwargs['data'] = data
        async with self as session:
            verb_func = getattr(session, verb)
            async with verb_func(url, **req_kwargs) as resp:
                status_code = resp.status
                headers = resp.headers
                resp_data = await resp.json()
        logger.debug('headers: {}'.format(headers))
        pagination_links = self.parse_link_headers(headers)
        if 'next' in pagination_links:
            status_code, _resp_data = await self._do_request(verb, pagination_links['next'], data)
            resp_data.extend(_resp_data)
        return status_code, resp_data
    async def make_request(self, verb, path, data=None):
        if data is None:
            data = {}
        # if self.token is not None:
        #     data['access_token'] = self.token

        # req_kwargs = {}
        # if len(data):
        #     req_kwargs['data'] = data

        url = '/'.join([API_ENDPOINT, path])

        status_code, resp_data = await self._do_request(verb, url, data)

        logger.debug('make_request: verb={}, path={}, url={}, status_code={}'.format(
            verb, path, url, status_code,
        ))
        # log_request(verb, url, resp_data)
        if status_code != 200:
            raise Exception('status_code: {}, response: {}'.format(status_code, resp_data))
        resp_data = utils.iter_parse_datetimes(resp_data)

        return resp_data
    async def get(self, path, data=None):
        return await self.make_request('get', path, data)
    async def post(self, path, data=None):
        return await self.make_request('post', path, data)
    async def __aenter__(self):
        self._acquire_count += 1
        return self.session
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        self._acquire_count -= 1
        if self._acquire_count == 0:
            session = self._session
            self._session = None
            if session is not None:
                await session.close()
