#!/usr/bin/env python

import os
import random
import unittest
import urllib.request


REGISTRY_URL = os.environ.get('REGISTRY_URL', 'http://0.0.0.0:8080/')


class TestRegistry(unittest.TestCase):
    def _auth_request(self, **kwargs):
        request = urllib.request.Request(
            headers={'X-My-Auth': 'open sesame'}, **kwargs)
        return urllib.request.urlopen(url=request, timeout=5)

    def test_auth_timeout(self):
        request = urllib.request.Request(
            url=REGISTRY_URL, headers={'X-My-Auth': 'sleep'})
        try:
            result = urllib.request.urlopen(url=request, timeout=15)
        except urllib.error.HTTPError as e:
            self.assertEqual(e.code, 504)
        else:
            raise RuntimeError(
                'auth timeout registry request returned {}'.format(
                    result.status))

    def test_forbidden(self):
        try:
            result = urllib.request.urlopen(url=REGISTRY_URL, timeout=5)
        except urllib.error.HTTPError as e:
            self.assertEqual(e.code, 403)
        else:
            raise RuntimeError(
                'unauthorized registry request returned {}'.format(
                    result.status))

    def test_post_atomic(self):
        key = 'test-post-atomic'
        data = os.urandom(8)
        result = self._auth_request(
            url=REGISTRY_URL + 'atomic/' + key, data=data, method='POST')
        self.assertEqual(result.status, 200)

    def test_get_atomic(self):
        key = 'test-get-atomic'
        data = os.urandom(8)
        result = self._auth_request(
            url=REGISTRY_URL + 'atomic/' + key, data=data, method='POST')
        self.assertEqual(result.status, 200)
        result = self._auth_request(
            url=REGISTRY_URL + 'atomic/' + key, method='GET')
        self.assertEqual(result.status, 200)
        self.assertEqual(result.read(), data)

    def test_post_streaming(self):
        key = 'test-post-streaming'
        data = os.urandom(8192)
        result = self._auth_request(
            url=REGISTRY_URL + 'streaming/' + key, data=data, method='POST')
        self.assertEqual(result.status, 200)

    def test_get_streaming(self):
        key = 'test-get-streaming'
        data = os.urandom(8192)
        result = self._auth_request(
            url=REGISTRY_URL + 'streaming/' + key, data=data, method='POST')
        self.assertEqual(result.status, 200)
        result = self._auth_request(
            url=REGISTRY_URL + 'streaming/' + key, method='GET')
        self.assertEqual(result.status, 200)
        self.assertEqual(result.read(), data)
