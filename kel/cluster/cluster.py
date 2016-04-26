import base64
import collections
import importlib
import logging

from cryptography import x509
from jinja2 import Template

from .keykeeper import KeyKeeper


logger = logging.getLogger(__name__)


class Cluster:

    components = [
        "network",
        "etcd",
        "master",
        "nodes",
    ]

    def __init__(self, config):
        self.config = config
        self.resources = collections.OrderedDict()
        self.provider_module = importlib.import_module(
            "kel.cluster.providers.{}".format(
                config["layer-0"]["provider"]["kind"],
            ),
        )
        self.provider = self.provider_module.setup(**config["layer-0"]["provider"])

    def get_provider_resource(self, name):
        mapping = {
            "network": self.provider_module.Network,
            "etcd": self.provider_module.EtcdCluster,
            "master": self.provider_module.MasterGroup,
            "nodes": ClusterNodes(self.provider_module.NodeGroup),
        }
        return mapping[name](self.provider, self, self.config["layer-0"]["resources"][name])

    def get_etcd_endpoints(self):
        return self.get_provider_resource("etcd").get_initial_endpoints()

    def get_default_cert_opts(self, name):
        opts = {"sans": []}
        if name == "apiserver":
            opts["sans"].append(
                x509.DNSName("kubernetes"),
                x509.DNSName("kubernetes.default"),
            )
        return opts

    def decode_manifest(self, data, ctx=None):
        if ctx is None:
            ctx = {}
        ctx.update({
            "cluster": self,
            "pem": self.get_pem,
            "b64": lambda s: base64.b64encode(s.encode("utf-8")).decode("ascii"),
        })
        return Template(base64.b64decode(data).decode("utf-8")).render(ctx)

    def get_pem(self, name, raw=False):
        if name == "ca-key":
            data = KeyKeeper.encode_key_to_pem(
                self.key_keeper.get_certificate_authority_key(),
            )
        elif name == "ca":
            data = KeyKeeper.encode_certificate_to_pem(
                self.key_keeper.get_certificate_authority_certificate(),
            )
        elif name.endswith("-key"):
            data = KeyKeeper.encode_key_to_pem(self.key_keeper.get_key(name[:-4]))
        else:
            if raw:
                data = self.key_keeper.get_raw_certificate(name)
            else:
                data = KeyKeeper.encode_certificate_to_pem(
                    self.key_keeper.get_certificate(name, self.get_default_cert_opts(name)),
                )
        return base64.b64encode(data).decode("utf-8")

    def create(self):
        for c in self.components:
            self.get_provider_resource(c).create()

    def destroy(self):
        for c in reversed(self.components):
            self.get_provider_resource(c).destroy()


class ClusterNodes:

    def __init__(self, NodeGroup):
        self.NodeGroup = NodeGroup
        self.node_groups = []

    def __call__(self, provider, cluster, config):
        for node_config in config:
            self.node_groups.append(self.NodeGroup(provider, cluster, node_config))
        return self

    def create(self):
        for node_group in self.node_groups:
            node_group.create()

    def destroy(self):
        for node_group in self.node_groups:
            node_group.destroy()
