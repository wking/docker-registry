#!/usr/bin/env python

import asyncio

import aiohttp.web


@asyncio.coroutine
def auth(request):
    if request.headers.get('X-My-Auth') == 'open sesame':
        return aiohttp.web.Response(request=request, status=200)
    return aiohttp.web.Response(request=request, status=403)


if __name__ == '__main__':
    app = aiohttp.web.Application()
    app.router.add_route('GET', '/', auth)

    loop = asyncio.get_event_loop()
    f = loop.create_server(app.make_handler, '0.0.0.0', '80')
    srv = loop.run_until_complete(f)
    try:
        loop.run_forever()
    except KeyboardInterrupt:
        pass
