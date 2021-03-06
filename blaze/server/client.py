from __future__ import absolute_import, division, print_function

import requests
from flask import json
import flask
from dynd import nd
from datashape import dshape

from ..data.core import DataDescriptor
from ..data.utils import coerce
from ..expr import Expr, TableExpr
from ..dispatch import dispatch
from .index import emit_index
from ..api.resource import resource

# These are a hack for testing
# It's convenient to use requests for live production but use
# flask for testing.  Sadly they have different Response objects,
# hence the dispatched functions

def content(response):
    if isinstance(response, flask.Response):
        return response.data
    if isinstance(response, requests.Response):
        return response.content

def ok(response):
    if isinstance(response, flask.Response):
        return 'OK' in response.status
    if isinstance(response, requests.Response):
        return response.ok

def reason(response):
    if isinstance(response, flask.Response):
        return response.status
    if isinstance(response, requests.Response):
        return response.reason


class Client(DataDescriptor):
    __slots__ = 'url', '_name'
    def __init__(self, url, name):
        self.url = url.strip('/')
        self._name = name

    def _get_data(self, key):
        response = requests.put('%s/data/%s.json' % (self.url, self._name),
                                data=json.dumps({'index': emit_index(key)}),
                                headers = {'Content-type': 'application/json',
                                           'Accept': 'text/plain'})
        if not ok(response):
            raise ValueError("Bad Response: %s" % reason(response))

        data = json.loads(content(response))

        return data['datashape'], data['data']

    def get_py(self, key):
        dshape, data = self._get_data(key)
        return coerce(dshape, data)

    def get_dynd(self, key):
        dshape, data = self._get_data(key)
        return nd.array(data, type=str(dshape))


    @property
    def dshape(self):
        response = requests.get('%s/datasets.json' % self.url)

        if not ok(response):
            raise ValueError("Bad Response: %s" % reason(response))

        data = json.loads(content(response))

        return dshape(data[self._name])


class ExprClient(object):
    """ Expression Client for Blaze Server

    Parameters
    ----------

    url: str
        URL of a Blaze server
    name: str
        Name of dataset on that server

    Examples
    --------

    >>> # This example matches with the docstring of ``Server``
    >>> ec = ExprClient('localhost:5000', 'accounts')
    >>> t = Table(ec) # doctest: +SKIP

    See Also
    --------

    blaze.server.server.Server
    """
    __slots__ = 'url', 'name'
    def __init__(self, url, name):
        url = url.strip('/')
        if not url[:4] == 'http':
            url = 'http://' + url
        self.url = url
        self.name = name

    @property
    def dshape(self):
        response = requests.get('%s/datasets.json' % self.url)

        if not ok(response):
            raise ValueError("Bad Response: %s" % reason(response))

        data = json.loads(content(response))

        return dshape(data[self.name])


@dispatch(ExprClient)
def discover(ec):
    return ec.dshape


@dispatch(Expr, ExprClient)
def compute_down(expr, ec):
    from .server import to_tree
    from ..api import Table
    from ..api import into
    from pandas import DataFrame
    tree = to_tree(expr)

    r = requests.get('%s/compute/%s.json' % (ec.url, ec.name),
                     data = json.dumps({'expr': tree}),
                     headers={'Content-Type': 'application/json'})

    if not ok(r):
        raise ValueError("Bad response: %s" % reason(r))

    data = json.loads(content(r))

    return data['data']


@resource.register('blaze://.*')
def resource_blaze(uri, name):
    uri = uri[len('blaze://'):]
    return ExprClient(uri, name)


@resource.register('blaze://.*::\w*', priority=11)
def resource_blaze_all(uri):
    uri = uri[len('blaze://'):]
    uri, name = uri.rsplit('::', 1)
    return ExprClient(uri, name)
