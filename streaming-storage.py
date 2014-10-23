#!/usr/bin/env python

import asyncio
import logging
import os.path

import aiohttp.web


_LOG = logging.getLogger('streaming-storage')
_LOG.setLevel(logging.DEBUG)
_LOG.addHandler(logging.StreamHandler())


@asyncio.coroutine
def get_streaming(request):
    path = os.path.join('/tmp', request.match_info.get('key'))
    with open(path, 'rb') as f:
        size = os.path.getsize(path)
        reader = asyncio.StreamReader()
        reader.set_transport(transport=f)
        response = aiohttp.web.StreamResponse(request=request, status=200)
        response.content_length = size
        _LOG.debug('streaming from {}'.format(path))
        while True:
            data = f.read()
            #data = yield from reader.read()
            if data:
                _LOG.debug('streaming {} bytes from {}'.format(len(data), path))
                yield from response.write(data)
            else:
                break
    _LOG.debug('streamed from {}'.format(path))
    return response


@asyncio.coroutine
def post_streaming(request):
    path = os.path.join('/tmp', request.match_info.get('key'))
    _LOG.debug('streaming to {}'.format(path))
    with open(path, 'wb') as f:
        loop = asyncio.get_event_loop()
        protocol = asyncio.StreamReaderProtocol(
            stream_reader=request.payload, loop=loop)
        writer = asyncio.StreamWriter(
            transport=f, protocol=protocol, reader=request.payload, loop=loop)
        while True:
            data = yield from request.payload.readany()
            if data:
                _LOG.debug('streaming {} bytes to {}'.format(len(data), path))
                writer.write(data)
                yield from writer.drain()
            else:
                break
    _LOG.debug('streamed to {}'.format(path))
    return aiohttp.web.Response(request=request, status=200)


if __name__ == '__main__':
    app = aiohttp.web.Application()
    app.router.add_route('GET', '/{key}', get_streaming)
    app.router.add_route('POST', '/{key}', post_streaming)

    loop = asyncio.get_event_loop()
    f = loop.create_server(app.make_handler, '0.0.0.0', '80')
    srv = loop.run_until_complete(f)
    try:
        loop.run_forever()
    except KeyboardInterrupt:
        pass
