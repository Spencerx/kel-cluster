import base64
import collections
import importlib
import json
import logging
import os

import requests
import yaml

from cryptography import x509
from jinja2 import Template

from .keykeeper import KeyKeeper
from .utils import dict_merge


logger = logging.getLogger(__name__)


class Cluster:

    components = [
        "network",
        "etcd",
        "master",
        "nodes",
    ]

    @classmethod
    def from_config(cls, filename):
        root = os.path.join(os.path.expanduser("~"), ".config", "kel", "clusters")
        with open(os.path.join(root, filename)) as fp:
            config = yaml.load(fp.read())
        cluster = cls(config)
        cluster.key_keeper = KeyKeeper(cluster.config.get("key-dir"))
        return cluster

    def __init__(self, config):
        self.config = {}
        self.resources = collections.OrderedDict()
        self.provider_module = importlib.import_module(
            "kel.cluster.providers.{}".format(
                config["provider"]["kind"],
            ),
        )
        self.provider = self.provider_module.setup(**config["provider"])
        self.set_version(config["kel-version"])
        dict_merge(self.config, config)

    def load_layers(self, version):
        cache_file = os.path.expanduser("~/.cache/kel/versions.json")
        if not os.path.exists(os.path.dirname(cache_file)):
            os.makedirs(os.path.dirname(cache_file))
        cached_raw, cached = None, {}
        if os.path.exists(cache_file):
            with open(cache_file) as fp:
                cached_raw = fp.read().strip()
        if cached_raw:
            cached = json.loads(cached_raw)
        if version not in cached:
            url = "https://storage.googleapis.com/release.kelproject.com/distro/{}/manifest.json"
            r = requests.get(url.format(version))
            r.raise_for_status()
            cached[version] = json.loads(r.content)
        with open(cache_file, "w") as fp:
            fp.write(json.dumps(cached))
        return cached[version]

    def set_version(self, version):
        self.config.update(self.load_layers(version))
        self.config["layers"]["kel"]["version"] = version
        self.version = version

    def get_provider_resource(self, name):
        mapping = {
            "network": self.provider_module.Network,
            "etcd": self.provider_module.EtcdCluster,
            "master": self.provider_module.MasterGroup,
            "nodes": ClusterNodes(self.provider_module.NodeGroup),
        }
        return mapping[name](self.provider, self, self.config["resources"][name])

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
