# -*- coding: utf-8 -*-
# Copyright (c) 2014 Docker.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
# implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""
docker_registry.core.driver
~~~~~~~~~~~~~~~~~~~~~~~~~~

This file defines:
 * a generic interface that describes a uniform "driver"
 * methods to register / get these "connections"

Pretty much, the purpose of this is just to abstract the underlying storage
implementation, for a given scheme.
"""

__all__ = ["fetch", "available", "Base"]

import functools
import logging
import pkgutil
import urllib

import docker_registry.drivers

from .compat import json
from .exceptions import NotImplementedError

logger = logging.getLogger(__name__)


def check(value):
    value = str(value)
    if value == '..':
        value = '%2E%2E'
    if value == '.':
        value = '%2E'
    return urllib.quote_plus(value)


def filter_args(f):
    @functools.wraps(f)
    def wrapper(*args, **kwargs):
        args = list(args)
        ref = args.pop(0)
        args = [check(arg) for arg in args]
        args.insert(0, ref)
        for key, value in kwargs.iteritems():
            kwargs[key] = check(value)
        return f(*args, **kwargs)
    return wrapper


class Storage(object):

    """The docker-registry side API used for image storage.

    This class doesn't actually *do* anything, it just lays out the
    API for consistent logging.  You should either inherit from this
    class and override the private (log-wrapped) methods, or duck type
    the public methods.

    """
    def image_exists(self, image):
        "Do we have that image layer in storage?  True/False."
        raise NotImplementedError(
            'You must implement image_exists(self, image) on your storage {0}'
            .format(self.__class__.__name__))

    def image_is_private(self, image):
        "Does accessing this image layer require authorization?  True/False."
        raise NotImplementedError(
            'You must implement image_is_private(self, image) on your storage '
            '{0}'
            .format(self.__class__.__name__))

    def image_size(self, image):
        """Get the image layer size in bytes.

        Raise FileNotFoundError if the image doesn't exist.

        """
        raise NotImplementedError(
            'You must implement image_size(self, image) on your storage {0}'
            .format(self.__class__.__name__))

    def image_stream_read(self, image, bytes_range=None)
        """Get a reader for streaming an image layer from the registry.

        The reader should support the read() method.  bytes_range is
        an optional (start_offset, stop_offset) tuple, which should
        configure the reader to start at the start_offset-th byte and
        stop before the stop_offset-th byte (like Python's
        [start:stop] slicing).

        """
        raise NotImplementedError(
            'You must implement image_stream_read(self, image, '
            'bytes_range=None) on your storage {0}'
            .format(self.__class__.__name__))

    def image_stream_write(self, image, bytes_range=None)
        """Get a writer for streaming an image layer to the registry.

        The writer should support the write() method.

        """
        raise NotImplementedError(
            'You must implement image_stream_write(self, image) on your '
            'storage {0}'
            .format(self.__class__.__name__))

    def image_sendfile_uri(image)

        """Get an X-Sendfile URI for an image layer.

        This optional method gives us the information we need to serve
        an image layer directly from a reverse-proxy that's wrapping
        the registry [1].

        [1]: http://wiki.nginx.org/XSendfile

        """
        raise NotImplementedError(
            'You must implement image_sendfile_uri(self, image) on your '
            'storage {0}'
            .format(self.__class__.__name__))

    def image_redirect_url(self, image):
        """Get a redirect URL for an image layer.

        This optional method gives us a URL to which client can be
        redirected to get the content from the path.

        Note, this feature will only be used if the `storage_redirect`
        configuration key is set to `True`.

        """
        raise NotImplementedError(
            'You must implement image_redirect_url(self, image) on your
            storage {0}'
            .format(self.__class__.__name__))

    def get_image_metadata(self, image):
        "Get an image's Docker metadata (e.g. 'docker inspect IMAGE')."
        raise NotImplementedError(
            'You must implement get_image_metadata(self, image) on your
            storage {0}'
            .format(self.__class__.__name__))


class FileStorage(Storage):

    """A convenience class for filesystem-based storage.

    Inheriting this class allows you to provide the full Storage API
    by only writing a few methods:

    * _exists
    * 

    :param host: host name
    :type host: unicode
    :param port: port number
    :type port: int
    :param basepath: base path (will be prepended to actual requests)
    :type basepath: unicode

    """

    # Useful if we want to change those locations later without rewriting
    # the code which uses Storage
    repositories = 'repositories'
    images = 'images'

    def _repository_path(self, namespace, repository):
        return '{0}/{1}/{2}'.format(
            self.repositories, namespace, repository)

    def __init__(self, path=None, config=None):
        pass

    @filter_args
    def _images_list_path(self, namespace, repository):
        repository_path = self._repository_path(
            namespace=namespace, repository=repository)
        return '{0}/_images_list'.format(repository_path)

    @filter_args
    def _image_json_path(self, image):
        return '{0}/{1}/json'.format(self.images, image)

    @filter_args
    def _image_mark_path(self, image):
        return '{0}/{1}/_inprogress'.format(self.images, image)

    @filter_args
    def _image_checksum_path(self, image):
        return '{0}/{1}/_checksum'.format(self.images, image)

    @filter_args
    def _image_layer_path(self, image):
        return '{0}/{1}/layer'.format(self.images, image)

    @filter_args
    def _image_ancestry_path(self, image):
        return '{0}/{1}/ancestry'.format(self.images, image)

    @filter_args
    def _image_files_path(self, image):
        return '{0}/{1}/_files'.format(self.images, image)

    @filter_args
    def _image_diff_path(self, image):
        return '{0}/{1}/_diff'.format(self.images, image)

    @filter_args
    def _repository_path(self, namespace, repository):
        return '{0}/{1}/{2}'.format(
            self.repositories, namespace, repository)

    @filter_args
    def tag_path(self, namespace, repository, tagname=None):
        repository_path = self._repository_path(
            namespace=namespace, repository=repository)
        if not tagname:
            return repository_path
        return '{0}/tag_{1}'.format(repository_path, tagname)

    @filter_args
    def repository_json_path(self, namespace, repository):
        repository_path = self._repository_path(
            namespace=namespace, repository=repository)
        return '{0}/json'.format(repository_path)

    @filter_args
    def repository_tag_json_path(self, namespace, repository, tag):
        repository_path = self._repository_path(
            namespace=namespace, repository=repository)
        return '{0}/tag{1}_json'.format(repository_path, tag)

    @filter_args
    def index_images_path(self, namespace, repository):
        repository_path = self._repository_path(
            namespace=namespace, repository=repository)
        return '{0}/_index_images'.format(repository_path)

    @filter_args
    def private_flag_path(self, namespace, repository):
        repository_path = self._repository_path(
            namespace=namespace, repository=repository)
        return '{0}/_private'.format(repository_path)

    def is_private(self, namespace, repository):
        return self.exists(self.private_flag_path(namespace, repository))

    def content_redirect_url(self, path):
        """Get a URL for content at path

        Get a URL to which client can be redirected to get the content from
        the path. Return None if not supported by this engine.

        Note, this feature will only be used if the `storage_redirect`
        configuration key is set to `True`.
        """
        return None

    def get_json(self, path):
        return json.loads(self.get_unicode(path))

    def put_json(self, path, content):
        return self.put_unicode(path, json.dumps(content))

    def get_unicode(self, path):
        return self.get_bytes(path).decode('utf8')

    def put_unicode(self, path, content):
        return self.put_bytes(path, content.encode('utf8'))

    def get_bytes(self, path):
        return self.get_content(path)

    def put_bytes(self, path, content):
        return self.put_content(path, content)

    def get_content(self, path):
        """Method to get content."""
        raise NotImplementedError(
            "You must implement get_content(self, path) on your storage %s" %
            self.__class__.__name__)

    def put_content(self, path, content):
        """Method to put content."""
        raise NotImplementedError(
            "You must implement put_content(self, path, content) on %s" %
            self.__class__.__name__)

    def stream_read(self, path, bytes_range=None):
        """Method to stream read."""
        raise NotImplementedError(
            "You must implement stream_read(self, path, , bytes_range=None) " +
            "on your storage %s" %
            self.__class__.__name__)

    def stream_write(self, path, fp):
        """Method to stream write."""
        raise NotImplementedError(
            "You must implement stream_write(self, path, fp) " +
            "on your storage %s" %
            self.__class__.__name__)

    def list_directory(self, path=None):
        """Method to list directory."""
        raise NotImplementedError(
            "You must implement list_directory(self, path=None) " +
            "on your storage %s" %
            self.__class__.__name__)

    def _exists(self, path):
        "Do we have 'path' in storage?  True/False."
        raise NotImplementedError(
            "You must implement exists(self, path) on your storage %s" %
            self.__class__.__name__)

    def image_exists(self, image):
        return self._exists(path=self._image_mark_path(image=image))

    def remove(self, path):
        """Method to remove."""
        raise NotImplementedError(
            "You must implement remove(self, path) on your storage %s" %
            self.__class__.__name__)

    def get_size(self, path):
        """Method to get the size."""
        raise NotImplementedError(
            "You must implement get_size(self, path) on your storage %s" %
            self.__class__.__name__)


def fetch(name):
    try:
        # XXX The noqa below is because of hacking being non-sensical on this
        module = __import__('docker_registry.drivers.%s' % name, globals(),
                            locals(), ['Storage'], 0)  # noqa
        logger.debug("Will return docker-registry.drivers.%s.Storage" % name)
    except ImportError as e:
        logger.warn("Got exception: %s" % e)
        raise NotImplementedError(
            """You requested storage driver docker_registry.drivers.%s
which is not installed. Try `pip install docker-registry-driver-%s`
or check your configuration. The following are currently
available on your system: %s. Exception was: %s"""
            % (name, name, available(), e)
        )
    module.Storage.scheme = name
    return module.Storage


def available():
    return [modname for importer, modname, ispkg
            in pkgutil.iter_modules(docker_registry.drivers.__path__)]
