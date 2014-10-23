#!/usr/bin/env python

import asyncio

import aiohttp.web


_STORAGE = {}


@asyncio.coroutine
def get_atomic(request):
    try:
        value = _STORAGE[request.match_info.get('key')]
    except KeyError:
        return aiohttp.web.Response(request=request, status=404)
    else:
        return aiohttp.web.Response(request=request, body=value, status=200)


@asyncio.coroutine
def post_atomic(request):
    data = yield from request.payload.read()
    _STORAGE[request.match_info.get('key')] = data
    return aiohttp.web.Response(request=request, status=200)


if __name__ == '__main__':
    app = aiohttp.web.Application()
    app.router.add_route('GET', '/{key}', get_atomic)
    app.router.add_route('POST', '/{key}', post_atomic)

    loop = asyncio.get_event_loop()
    f = loop.create_server(app.make_handler, '0.0.0.0', '80')
    srv = loop.run_until_complete(f)
    try:
        loop.run_forever()
    except KeyboardInterrupt:
        pass
