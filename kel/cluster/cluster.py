import base64
import collections
import concurrent.futures
import importlib
import logging

from jinja2 import Template


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

    @property
    def node_token(self):
        return self.config["layer-0"].get("node-token")

    @node_token.setter
    def node_token(self, value):
        self.config["layer-0"]["node-token"] = value

    @property
    def master_ip(self):
        return self.config["layer-0"]["resources"].get("master-ip")

    @master_ip.setter
    def master_ip(self, value):
        self.config["layer-0"]["resources"]["master-ip"] = value

    @property
    def router_ip(self):
        return self.config["layer-1"]["resources"].get("router-ip")

    @router_ip.setter
    def router_ip(self, value):
        self.config["layer-1"]["resources"]["router-ip"] = value

    def get_etcd_endpoints(self):
        return self.get_provider_resource("etcd").get_initial_endpoints()

    def decode_manifest(self, data, ctx=None):
        if ctx is None:
            ctx = {}
        ctx.update({
            "cluster": self,
            "b64": lambda s: base64.b64encode(s.encode("utf-8")).decode("ascii"),
        })
        return Template(base64.b64decode(data).decode("utf-8")).render(ctx)

    def create(self):
        with concurrent.futures.ThreadPoolExecutor() as executor:
            for c in self.components:
                self.get_provider_resource(c).create(executor)

    def destroy(self):
        with concurrent.futures.ThreadPoolExecutor() as executor:
            for c in reversed(self.components):
                self.get_provider_resource(c).destroy(executor)


class ClusterNodes:

    def __init__(self, NodeGroup):
        self.NodeGroup = NodeGroup
        self.node_groups = []

    def __call__(self, provider, cluster, config):
        for node_config in config:
            self.node_groups.append(self.NodeGroup(provider, cluster, node_config))
        return self

    def create(self, executor):
        fs = []
        for node_group in self.node_groups:
            fs.append(executor.submit(node_group.create, executor))
        concurrent.futures.wait(fs)

    def destroy(self, executor):
        fs = []
        for node_group in self.node_groups:
            fs.append(executor.submit(node_group.destroy, executor))
        concurrent.futures.wait(fs)
