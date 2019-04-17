import ssl as ssl_lib
from distutils.version import LooseVersion

from ansible.module_utils.basic import AnsibleModule, to_native

try:
    from pymongo import version as PyMongoVersion
    from pymongo.errors import OperationFailure, ServerSelectionTimeoutError

    if LooseVersion(PyMongoVersion) >= LooseVersion('2.3'):
        from pymongo import MongoClient
    else:
        from pymongo import Connection as MongoClient
    HAS_PYMONGO = True
    HAS_PYMONGO_ERROR = None
except ImportError as e:
    HAS_PYMONGO_ERROR = to_native(e)
    HAS_PYMONGO = False


MONGODB_AUTH_ARG_SPEC = dict(
    host=dict(default="localhost"),
    port=dict(default=27017, type='int'),
    username=dict(default=None),
    password=dict(default=None, no_log=True),
    database=dict(default='admin'),
    ssl=dict(default=False, type='bool'),
    ssl_cert_reqs=dict(default='CERT_REQUIRED', choices=['CERT_NONE', 'CERT_OPTIONAL', 'CERT_REQUIRED'])
)


class AnsibleModuleMongodb(object):
    def __init__(self, argument_spec=None, supports_check_mode=False,
                 mutually_exclusive=None, required_together=None,
                 required_if=None, required_one_of=None):

        merged_argument_spec = MONGODB_AUTH_ARG_SPEC
        if argument_spec:
            merged_argument_spec.update(argument_spec)

        self.module = AnsibleModule(
            argument_spec=merged_argument_spec,
            supports_check_mode=supports_check_mode,
            mutually_exclusive=mutually_exclusive,
            required_together=required_together,
            required_if=required_if,
            required_one_of=required_one_of
        )

        if not HAS_PYMONGO:
            msg = "This module requires the pymongo. Try `pip install pymongo"
            self.fail_json(msg=msg)

        self._mongodb_auth_arg_spec = None

    @property
    def mongodb_auth_arg_spec(self):
        if not self._mongodb_auth_arg_spec:
            auth_arg_spec = dict(
                name=self.module.params.get('username'),
                password=self.module.params.get('password'),
                database=self.module.params.get('database')
            )

            if self.module.params.get('ssl'):
                auth_arg_spec['ssl'] = self.module.params.get('ssl')
                auth_arg_spec['ssl_cert_reqs'] = getattr(ssl_lib, self.module.params.get('ssl_cert_reqs'))

            self._mongodb_auth_arg_spec = auth_arg_spec

        return self._mongodb_auth_arg_spec

    def fail_json(self, **kwargs):
        return self.module.fail_json(**kwargs)

    def exit_json(self, **kwargs):
        return self.module.exit_json(**kwargs)

    def warn(self, msg):
        return self.module.warn(warning=msg)

    def client(self):
        host = self.module.params.get('host')
        port = self.module.params.get('port')

        client = self._client(host, port)

        if self.mongodb_auth_arg_spec['name'] and \
                self.mongodb_auth_arg_spec['password']:

            try:
                client.admin.authenticate(**self.mongodb_auth_arg_spec)
            except OperationFailure as e:
                msg = to_native(e)
                self.fail_json(msg=msg)

        self.check_compatibility(client)

        return client

    @staticmethod
    def _client(*args, **kwargs):
        return MongoClient(*args, **kwargs)

    def check_compatibility(self, client):

        try:
            server_version = client.server_info()['version']
        except ServerSelectionTimeoutError:
            msg = "Connection refused, please check host and port are valid"
            self.fail_json(msg=msg)

        server_version = LooseVersion(server_version)
        client_version = LooseVersion(PyMongoVersion)

        # Compatibility list between the driver and the database
        # See: https://docs.mongodb.com/ecosystem/drivers/driver-compatibility-reference/#python-driver-compatibility

        versions = [
            {'client': '3.7', 'server': '4.0'},
            {'client': '3.6', 'server': '3.6'},
            {'client': '3.4', 'server': '3.4'},
            {'client': '3.2', 'server': '3.2'},
            {'client': '2.8', 'server': '3.0'},
            {'client': '2.7', 'server': '2.6'}
        ]

        for version in versions:
            if server_version >= LooseVersion(version['server']) and \
                    client_version < LooseVersion(version['client']):

                msg = "You must use Pymongo {}+ with Mongodb {}".format(
                    version['client'], server_version)
                self.fail_json(msg=msg)

        if server_version < LooseVersion('2.6') or \
                client_version < LooseVersion('2.6'):
            msg = "This module doesn't supported Mongodb and Pymongo older 2.6"
            self.fail_json(msg=msg)
