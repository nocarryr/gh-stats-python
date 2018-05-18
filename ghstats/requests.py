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
    def parse_debug_headers(self, headers):
        def parse_conditional_headers():
            keys = ['ETag', 'Last-Modified']
            return {k:headers.get(k) for k in keys}
        def parse_rate_headers():
            total_limit = headers.get('X-RateLimit-Limit')
            remaining = headers.get('X-RateLimit-Remaining')
            reset_timestamp = headers.get('X-RateLimit-Reset')
            if total_limit is not None and remaining is not None:
                total_limit = int(total_limit)
                remaining = int(remaining)
                used = total_limit - remaining
            else:
                used = None
            if reset_timestamp is not None:
                reset_timestamp = utils.timestamp_to_dt(float(reset_timestamp))
            d = {
                'total_limit':total_limit,
                'remaining':remaining,
                'reset_timestamp':reset_timestamp
            }
            return d
        d = {
            'ApiLimits':parse_rate_headers(),
            'Conditional':parse_conditional_headers(),
        }
        return d
    async def _do_request(self, verb, url, data=None, request_headers=None):
        req_kwargs = {}
        if data:
            req_kwargs['data'] = data
        if request_headers is not None:
            req_kwargs['headers'] = request_headers
        async with self as session:
            verb_func = getattr(session, verb)
            async with verb_func(url, **req_kwargs) as resp:
                status_code = resp.status
                headers = resp.headers
                if status_code == 200:
                    resp_data = await resp.json()
                else:
                    resp_data = await resp.text()
        header_data = self.parse_debug_headers(headers)
        logger.debug('headers: {}'.format(header_data))
        pagination_links = self.parse_link_headers(headers)
        if 'next' in pagination_links:
            status_code, _header_data, _resp_data = await self._do_request(verb, pagination_links['next'], data)
            resp_data.extend(_resp_data)
        return status_code, header_data, resp_data
    async def make_request(self, verb, path, data=None, headers=None):
        if data is None:
            data = {}
        # if self.token is not None:
        #     data['access_token'] = self.token

        # req_kwargs = {}
        # if len(data):
        #     req_kwargs['data'] = data

        url = '/'.join([API_ENDPOINT, path])

        status_code, header_data, resp_data = await self._do_request(verb, url, data, headers)

        if status_code == 304:      # Not Modified
            logger.debug('request not modified: verb={}, url={}'.format(verb, url))
            resp_data = {}
        elif status_code != 200:
            raise Exception('status_code: {}, response: {}'.format(status_code, resp_data))
        else:
            resp_data = utils.iter_parse_datetimes(resp_data)

        return status_code, header_data, resp_data
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
