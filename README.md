This registry mocks up my [reverse-proxy auth
proposal][reverse-proxy-auth].  Launch the cluster with [Fig][].

    $ fig up -d

and test it with:

    $ PYTHONPATH=. python -m unittest test

[reverse-proxy-auth]: https://github.com/docker/docker-registry/issues/623
[Fig]: http://www.fig.sh/
