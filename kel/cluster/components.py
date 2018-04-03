import collections
import hashlib
import json
import logging
import time

import pykube
import pykube.objects
import yaml


logger = logging.getLogger(__name__)


class KubernetesResource:

    def __init__(self, cluster):
        self.cluster = cluster
        self.kubeconfig = self.build_kube_config()
        self.api = pykube.HTTPClient(self.kubeconfig)

    def build_kube_config(self):
        return pykube.KubeConfig({
            "clusters": [
                {
                    "name": self.cluster.config["name"],
                    "cluster": {
                        "server": "https://{}".format(self.cluster.master_ip),
                    },
                },
            ],
            "users": [
                {
                    "name": self.cluster.config["name"],
                    "user": {},  # @@@ kubeadm
                },
            ],
            "contexts": [
                {
                    "name": self.cluster.config["name"],
                    "context": {
                        "cluster": self.cluster.config["name"],
                        "user": self.cluster.config["name"],
                    },
                }
            ],
            "current-context": self.cluster.config["name"],
        })

    def get_api_objs(self, group, manifest, ctx=None):
        if ctx is None:
            ctx = {}
        ctx = self.get_manifest_ctx(group, manifest, **ctx)
        docs = yaml.load_all(
            self.cluster.decode_manifest(
                self.cluster.config["release"][group]["manifests"][manifest],
                ctx,
            )
        )
        objs = collections.defaultdict(list)
        for doc in docs:
            obj = getattr(pykube.objects, doc["kind"])(self.api, doc)
            if obj.exists():
                obj.reload()
                # set the shadow object to the original doc enabling proper
                # update handling if the object has changed in the manifest
                obj.obj = doc
            objs[doc["kind"]].append(obj)
        return objs

    def generate_deployment_key(self, objs):
        serialized = "".join([json.dumps(r.obj, sort_keys=True) for r in objs])
        return hashlib.sha1(serialized.encode("ascii")).hexdigest()[:8]

    def delete_namespace(self, obj):
        obj.delete()
        while obj.exists():
            time.sleep(1)

    def get_manifest_ctx(self, group, manifest, **ctx):
        image = self.cluster.config["release"][group].get("images", {}).get(manifest)
        if image:
            ctx["image"] = image
        return ctx


class ComponentResource(KubernetesResource):

    requires_disk = False
    bundle = None

    @property
    def disk(self):
        if self.requires_disk:
            disk_config = self.cluster.config[self.layer]["resources"].get("{}-disk".format(self.manifest))
            if disk_config is None:
                raise Exception('"{}" requires disk configuration'.format(self.manifest))
            return disk_config

    def get_manifest_ctx(self, group, manifest, **ctx):
        ctx = super(ComponentResource, self).get_manifest_ctx(group, manifest, **ctx)
        ctx.update({
            "version": self.cluster.config["release"]["version"].replace(".", "-"),
            "replicas": self.replicas,
        })
        if self.bundle:
            ctx["bundle"] = self.cluster.config["release"][group]["bundles"][self.bundle]
        return ctx

    def generate_deployment_key(self):
        deployment = self.get_api_objs(self.group, self.manifest)["Deployment"][0]
        secrets = self.get_api_objs(self.group, self.manifest)["Secret"]
        objs = [deployment]
        for volume in deployment.obj["spec"]["template"]["spec"].get("volumes", []):
            if "secret" in volume:
                secret = next((s for s in secrets if s.name == volume["secret"]["secretName"]), None)
                if secret is None:
                    continue
                objs.append(secret)
        return super(ComponentResource, self).generate_deployment_key(objs)

    def get_deployment(self):
        deployment = self.get_api_objs(self.group, self.manifest)["Deployment"][0]
        key = self.generate_deployment_key()
        deployment.obj["metadata"]["labels"]["deployment"] = key
        return deployment

    def create_service(self):
        objs = self.get_api_objs(self.group, self.manifest)["Service"]
        for obj in objs:
            if not obj.exists():
                obj.create()
                logger.info('created "{}" service'.format(obj.name))

    def create_deployment(self):
        deployment = self.get_deployment()
        if not deployment.exists():
            deployment.create()
            logger.info('created "{}" deployment'.format(deployment.name))

    def create_secrets(self):
        objs = self.get_api_objs(self.group, self.manifest)["Secret"]
        for obj in objs:
            if not obj.exists():
                obj.create()
                logger.info('created "{}" secret'.format(obj.name))

    def has_secrets(self):
        return bool(self.get_api_objs(self.group, self.manifest)["Secret"])

    def has_service(self):
        return bool(self.get_api_objs(self.group, self.manifest)["Service"])

    @property
    def current_deployment(self):
        if not hasattr(self, "_current_deployment"):
            deployment = self.get_api_objs(self.group, self.manifest)["Deployment"][0]
            self._current_deployment = (
                pykube.Deployment
                .objects(self.api)
                .filter(
                    namespace=deployment.namespace,
                    selector={
                        "kelproject.com/name": deployment.obj["metadata"]["labels"]["kelproject.com/name"]
                    }
                )
                .get()
            )
        return self._current_deployment

    def can_upgrade(self):
        key = self.generate_deployment_key()
        return key != self.current_deployment.obj["metadata"]["labels"]["deployment"]

    def create(self):
        if self.disk:
            self.cluster.provider.create_disk(
                "{}-{}".format(self.cluster.config["name"], self.disk.get("name", self.manifest)),
                self.disk["size"],
                self.disk["type"],
            )
        if self.has_secrets():
            self.create_secrets()
        if self.has_service():
            self.create_service()
        self.create_deployment()

    def upgrade(self):
        if not self.can_upgrade():
            return
        self.update_secrets()
        deployment = self.current_deployment
        deployment.obj = self.get_deployment().obj
        deployment.update()

    def update_secrets(self):
        if self.has_secrets():
            secrets = self.get_api_objs(self.group, self.manifest)["Secret"]
            for secret in secrets:
                secret.update()

    def destroy(self):
        self.destroy_deployment()
        if self.has_service():
            self.destroy_service()
        if self.has_secrets():
            self.destroy_secrets()
        # @@@ leave disk around and let this be a cluster admin concern
        # we will want this to be an option eventually (think testing)
        # if self.disk:
        #     self.cluster.provider.destroy_disk("{}-{}".format("{}-{}".format(self.cluster.config["name"], self.disk.get("name", self.manifest))))

    def destroy_service(self):
        objs = self.get_api_objs(self.group, self.manifest)["Service"]
        for obj in objs:
            obj.delete()
            logger.info('destroyed "{}" service'.format(obj.name))

    def destroy_deployment(self):
        obj = self.get_api_objs(self.group, self.manifest)["Deployment"][0]
        self.delete_deployment(obj)
        logger.info('destroyed "{}" deployment'.format(obj.name))

    def destroy_secrets(self):
        objs = self.get_api_objs(self.group, self.manifest)["Secret"]
        for obj in objs:
            obj.delete()
            logger.info('destroyed "{}" secret'.format(obj.name))


class KubeDNS(ComponentResource):

    layer = "layer-0"
    group = "kubernetes"
    manifest = "kube-dns"
    replicas = 1


class KelSystem(KubernetesResource):

    layer = "layer-1"

    def create(self):
        obj = self.get_api_objs("kel", "kel-system")["Namespace"][0]
        if not obj.exists():
            obj.create()
            logger.info('created "{}" namespace'.format(obj.name))

    def destroy(self):
        obj = self.get_api_objs("kel", "kel-system")["Namespace"][0]
        self.delete_namespace(obj)
        logger.info('destroyed "{}" namespace'.format(obj.name))


class KelBuilds(KubernetesResource):

    layer = "layer-1"

    def create(self):
        obj = self.get_api_objs("kel", "kel-builds")["Namespace"][0]
        if not obj.exists():
            obj.create()
            logger.info('created "{}" namespace'.format(obj.name))

    def destroy(self):
        obj = self.get_api_objs("kel", "kel-builds")["Namespace"][0]
        self.delete_namespace(obj)
        logger.info('destroyed "{}" namespace'.format(obj.name))


class Router(ComponentResource):

    layer = "layer-1"
    group = "kel"
    manifest = "router"
    replicas = 1

    @property
    def loadbalancer_name(self):
        return "{}-router".format(self.cluster.config["name"])

    def create_loadbalancer(self):
        self.cluster.provider.create_loadbalancer(
            self.loadbalancer_name,
            [80, 443],
            ip=self.cluster.router_ip,
            attached_ig="{}-node-1x-nodes".format(self.cluster.config["name"])  # @@@ fix hardcoded value
        )

    def create(self):
        self.create_loadbalancer()
        super(Router, self).create()

    def destroy(self):
        super(Router, self).destroy()
        self.destroy_loadbalancer()

    def destroy_loadbalancer(self):
        self.cluster.provider.destroy_loadbalancer(self.loadbalancer_name)


class ApiCache(ComponentResource):

    layer = "layer-1"
    group = "kel"
    manifest = "api-cache"
    replicas = 1
    requires_disk = True


class ApiDatabase(ComponentResource):

    layer = "layer-1"
    group = "kel"
    manifest = "api-database"
    replicas = 1
    requires_disk = True


class ApiWeb(ComponentResource):

    layer = "layer-1"
    group = "kel"
    manifest = "api-web"
    bundle = "api"
    replicas = 1
