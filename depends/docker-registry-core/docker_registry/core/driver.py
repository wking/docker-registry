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

import logging
import pkgutil

import docker_registry.drivers

from . import exceptions
from .compat import json


logger = logging.getLogger(__name__)


class Base(object):

    """Storage is a convenience class that describes methods that must be
    implemented by any backend.
    You should inherit (or duck type) this if you are implementing your own.

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

    # Set the IO buffer to 128kB
    buffer_size = 128 * 1024
    # By default no storage plugin supports it
    supports_bytes_range = False

    def __init__(self, path=None, config=None):
        pass

    def get_images(self):
        """Iterate through images in storage

        This helper is useful for upgrades and other storage
        maintenance.  Yields image ids.
        """
        try:
            image_paths = list(
                self.list_directory(path=self.images))
        except exceptions.FileNotFoundError:
            image_paths = []
        for image_path in image_paths:
            image_id = image_path.rsplit('/', 1)[-1]
            yield image_id

    def get_repositories(self):
        """Iterate through repositories in storage

        This helper is useful for upgrades and other storage
        maintenance.  Yields tuples:

          (namespace, repository)
        """
        try:
            namespace_paths = list(
                self.list_directory(path=self.repositories))
        except exceptions.FileNotFoundError:
            namespace_paths = []
        for namespace_path in namespace_paths:
            namespace = namespace_path.rsplit('/', 1)[-1]
            try:
                repository_paths = list(
                    self.list_directory(path=namespace_path))
            except exceptions.FileNotFoundError:
                repository_paths = []
            for path in repository_paths:
                repository = path.rsplit('/', 1)[-1]
                yield (namespace, repository)

    def tags(self, namespace, repository):
        """Iterate through a repository's tags

        This helper is useful for upgrades and other storage
        maintenance.  Yields tuples:

          (tag_name, image_id)
        """
        tag_path = self.tag_path(namespace, repository)
        for path in self.list_directory(tag_path):
            full_tag_name = path.split('/').pop()
            if not full_tag_name.startswith('tag_'):
                continue
            tag_name = full_tag_name[4:]
            tag_content = self.get_content(path=path)
            yield (tag_name, tag_content)

    # FIXME(samalba): Move all path resolver in each module (out of the base)
    def images_list_path(self, namespace, repository):
        repository_path = self.repository_path(
            namespace=namespace, repository=repository)
        return '{0}/_images_list'.format(repository_path)

    def image_path(self, image_id):
        return '{0}/{1}'.format(self.images, image_id)

    def image_json_path(self, image_id):
        image_path = self.image_path(image_id=image_id)
        return '{0}/json'.format(image_path)

    def image_mark_path(self, image_id):
        image_path = self.image_path(image_id=image_id)
        return '{0}/_inprogress'.format(image_path)

    def image_checksum_path(self, image_id):
        image_path = self.image_path(image_id=image_id)
        return '{0}/_checksum'.format(image_path)

    def image_layer_path(self, image_id):
        image_path = self.image_path(image_id=image_id)
        return '{0}/layer'.format(image_path)

    def image_ancestry_path(self, image_id):
        image_path = self.image_path(image_id=image_id)
        return '{0}/ancestry'.format(image_path)

    def image_files_path(self, image_id):
        image_path = self.image_path(image_id=image_id)
        return '{0}/_files'.format(image_path)

    def image_diff_path(self, image_id):
        image_path = self.image_path(image_id=image_id)
        return '{0}/_diff'.format(image_path)

    def repository_path(self, namespace, repository):
        return '{0}/{1}/{2}'.format(
            self.repositories, namespace, repository)

    def tag_path(self, namespace, repository, tagname=None):
        repository_path = self.repository_path(
            namespace=namespace, repository=repository)
        if not tagname:
            return repository_path
        return '{0}/tag_{1}'.format(repository_path, tagname)

    def repository_json_path(self, namespace, repository):
        repository_path = self.repository_path(
            namespace=namespace, repository=repository)
        return '{0}/json'.format(repository_path)

    def repository_tag_json_path(self, namespace, repository, tag):
        repository_path = self.repository_path(
            namespace=namespace, repository=repository)
        return '{0}/tag{1}_json'.format(repository_path, tag)

    def index_images_path(self, namespace, repository):
        repository_path = self.repository_path(
            namespace=namespace, repository=repository)
        return '{0}/_index_images'.format(repository_path)

    def private_flag_path(self, namespace, repository):
        repository_path = self.repository_path(
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
        """Method to get content
        """
        raise exceptions.NotImplementedError(
            "You must implement get_content(self, path) on your storage %s" %
            self.__class__.__name__)

    def put_content(self, path, content):
        """Method to put content
        """
        raise exceptions.NotImplementedError(
            "You must implement put_content(self, path, content) on %s" %
            self.__class__.__name__)

    def stream_read(self, path, bytes_range=None):
        """Method to stream read
        """
        raise exceptions.NotImplementedError(
            "You must implement stream_read(self, path, , bytes_range=None) " +
            "on your storage %s" %
            self.__class__.__name__)

    def stream_write(self, path, fp):
        """Method to stream write
        """
        raise exceptions.NotImplementedError(
            "You must implement stream_write(self, path, fp) " +
            "on your storage %s" %
            self.__class__.__name__)

    def list_directory(self, path=None):
        """Method to list directory
        """
        raise exceptions.NotImplementedError(
            "You must implement list_directory(self, path=None) " +
            "on your storage %s" %
            self.__class__.__name__)

    def exists(self, path):
        """Method to test exists
        """
        raise exceptions.NotImplementedError(
            "You must implement exists(self, path) on your storage %s" %
            self.__class__.__name__)

    def remove(self, path):
        """Method to remove
        """
        raise exceptions.NotImplementedError(
            "You must implement remove(self, path) on your storage %s" %
            self.__class__.__name__)

    def get_size(self, path):
        """Method to get the size
        """
        raise exceptions.NotImplementedError(
            "You must implement get_size(self, path) on your storage %s" %
            self.__class__.__name__)


def fetch(name):
    """The only public method you should access if you are not implementing
    your own driver. - use this to get a backend
    instance to which you can delegate actual requests.

    :param host: host name
    :type host: unicode
    :param port: port number
    :type port: int
    :param basepath: base path (will be prepended to actual requests)
    :type basepath: unicode
    :returns: a docker connection instance usable for the requested scheme
    :rtype: DockerConnection
    """
    try:
        # XXX The noqa below is because of hacking being non-sensical on this
        module = __import__('docker_registry.drivers.%s' % name, globals(),
                            locals(), ['Storage'], 0)  # noqa
        logger.debug("Will return docker-registry.drivers.%s.Storage" % name)
    except ImportError as e:
        logger.warn("Got exception: %s" % e)
        raise exceptions.NotImplementedError(
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
