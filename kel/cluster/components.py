import collections
import hashlib
import json
import logging
import time

import pykube
import pykube.objects
import yaml

from pykube.rolling_updater import RollingUpdater


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
                        "server": "https://{}".format(self.cluster.config["master-ip"]),
                        "certificate-authority-data": self.cluster.get_pem("ca"),
                    },
                },
            ],
            "users": [
                {
                    "name": self.cluster.config["name"],
                    "user": {
                        "client-key-data": self.cluster.get_pem("admin-key"),
                        "client-certificate-data": self.cluster.get_pem("admin"),
                    },
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

    def get_api_objs(self, layer, manifest, ctx=None):
        if ctx is None:
            ctx = {}
        ctx = self.get_manifest_ctx(layer, manifest, **ctx)
        docs = yaml.load_all(
            self.cluster.decode_manifest(
                self.cluster.config["layers"][layer]["manifests"][manifest],
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

    def delete_rc(self, obj):
        obj.scale(0)
        obj.delete()

    def get_manifest_ctx(self, layer, manifest, **ctx):
        image = self.cluster.config["layers"][layer].get("images", {}).get(manifest)
        if image:
            ctx["image"] = image
        return ctx


class ComponentResource(KubernetesResource):

    requires_disk = False
    bundle = None

    @property
    def disk(self):
        if self.requires_disk:
            disk_config = self.cluster.config["resources"].get("{}-disk".format(self.manifest))
            if disk_config is None:
                raise Exception('"{}" requires disk configuration'.format(self.manifest))
            return disk_config

    def get_manifest_ctx(self, layer, manifest, **ctx):
        ctx = super(ComponentResource, self).get_manifest_ctx(layer, manifest, **ctx)
        ctx.update({
            "version": self.cluster.config["layers"][layer]["version"].replace(".", "-"),
            "replicas": self.replicas,
        })
        if self.bundle:
            ctx["bundle"] = self.cluster.config["layers"][layer]["bundles"][self.bundle]
        return ctx

    def generate_deployment_key(self):
        rc = self.get_api_objs(self.layer, self.manifest)["ReplicationController"][0]
        secrets = self.get_api_objs(self.layer, self.manifest)["Secret"]
        objs = [rc]
        for volume in rc.obj["spec"]["template"]["spec"].get("volumes", []):
            if "secret" in volume:
                secret = next((s for s in secrets if s.name == volume["secret"]["secretName"]), None)
                if secret is None:
                    continue
                objs.append(secret)
        return super(ComponentResource, self).generate_deployment_key(objs)

    def get_rc(self):
        rc = self.get_api_objs(self.layer, self.manifest)["ReplicationController"][0]
        key = self.generate_deployment_key()
        rc.obj["metadata"]["name"] = "{}-{}".format(rc.name, key)
        rc.obj["metadata"]["labels"]["deployment"] = key
        rc.obj["spec"]["selector"]["deployment"] = key
        rc.obj["spec"]["template"]["metadata"]["labels"]["deployment"] = key
        return rc

    def create_service(self):
        objs = self.get_api_objs(self.layer, self.manifest)["Service"]
        for obj in objs:
            obj.create()
            logger.info('created "{}" service'.format(obj.name))

    def create_replication_controller(self):
        rc = self.get_rc()
        rc.create()
        logger.info('created "{}" replication controller'.format(rc.name))

    def create_secrets(self):
        objs = self.get_api_objs(self.layer, self.manifest)["Secret"]
        for obj in objs:
            obj.create()
            logger.info('created "{}" secret'.format(obj.name))

    def has_secrets(self):
        return bool(self.get_api_objs(self.layer, self.manifest)["Secret"])

    def has_service(self):
        return bool(self.get_api_objs(self.layer, self.manifest)["Service"])

    @property
    def running_rc(self):
        if not hasattr(self, "_running_rc"):
            rc = self.get_api_objs(self.layer, self.manifest)["ReplicationController"][0]
            self._running_rc = (
                pykube.ReplicationController
                .objects(self.api)
                .filter(
                    namespace=rc.namespace,
                    selector={
                        "kelproject.com/name": rc.obj["metadata"]["labels"]["kelproject.com/name"]
                    }
                )
                .get()
            )
        return self._running_rc

    def can_upgrade(self):
        key = self.generate_deployment_key()
        return key != self.running_rc.obj["metadata"]["labels"]["deployment"]

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
        self.create_replication_controller()

    def upgrade(self):
        if not self.can_upgrade():
            return
        self.update_secrets()
        RollingUpdater(self.api, self.running_rc, self.get_rc()).update()

    def update_secrets(self):
        if self.has_secrets():
            secrets = self.get_api_objs(self.layer, self.manifest)["Secret"]
            for secret in secrets:
                secret.update()

    def destroy(self):
        self.destroy_replication_controller()
        if self.has_service():
            self.destroy_service()
        if self.has_secrets():
            self.destroy_secrets()
        # @@@ leave disk around and let this be a cluster admin concern
        # we will want this to be an option eventually (think testing)
        # if self.disk:
        #     self.cluster.provider.destroy_disk("{}-{}".format("{}-{}".format(self.cluster.config["name"], self.disk.get("name", self.manifest))))

    def destroy_service(self):
        objs = self.get_api_objs(self.layer, self.manifest)["Service"]
        for obj in objs:
            obj.delete()
            logger.info('destroyed "{}" service'.format(obj.name))

    def destroy_replication_controller(self):
        obj = self.get_api_objs(self.layer, self.manifest)["ReplicationController"][0]
        self.delete_rc(obj)
        logger.info('destroyed "{}" replication controller'.format(obj.name))

    def destroy_secrets(self):
        objs = self.get_api_objs(self.layer, self.manifest)["Secret"]
        for obj in objs:
            obj.delete()
            logger.info('destroyed "{}" secret'.format(obj.name))


class KubeDNS(ComponentResource):

    layer = "kubernetes"
    manifest = "kube-dns"
    replicas = 1


class KelSystem(KubernetesResource):

    def create(self):
        obj = self.get_api_objs("kel", "kel-system")["Namespace"][0]
        obj.create()
        logger.info('created "{}" namespace'.format(obj.name))

    def destroy(self):
        obj = self.get_api_objs("kel", "kel-system")["Namespace"][0]
        self.delete_namespace(obj)
        logger.info('destroyed "{}" namespace'.format(obj.name))


class KelBuilds(KubernetesResource):

    def create(self):
        obj = self.get_api_objs("kel", "kel-builds")["Namespace"][0]
        obj.create()
        logger.info('created "{}" namespace'.format(obj.name))

    def destroy(self):
        obj = self.get_api_objs("kel", "kel-builds")["Namespace"][0]
        self.delete_namespace(obj)
        logger.info('destroyed "{}" namespace'.format(obj.name))


class Router(ComponentResource):

    layer = "kel"
    manifest = "router"
    bundle = "router"
    replicas = 3

    @property
    def loadbalancer_name(self):
        return "{}-router".format(self.cluster.config["name"])

    def create_loadbalancer(self):
        self.cluster.provider.create_loadbalancer(
            self.loadbalancer_name,
            [80, 443],
            ip=self.cluster.config.get("router-ip"),
        )

    def create(self):
        self.create_loadbalancer()
        super(Router, self).create()

    def destroy(self):
        super(Router, self).destroy()
        self.destroy_loadbalancer()

    def destroy_loadbalancer(self):
        self.cluster.provider.destroy_loadbalancer(self.loadbalancer_name)


class BlobstoreData(ComponentResource):

    layer = "kel"
    manifest = "blobstore-data"
    replicas = 1
    requires_disk = True


class Blobstore(ComponentResource):

    layer = "kel"
    manifest = "blobstore"
    bundle = "blobstore"
    replicas = 3


class ApiCache(ComponentResource):

    layer = "kel"
    manifest = "api-cache"
    replicas = 1
    requires_disk = True


class ApiDatabase(ComponentResource):

    layer = "kel"
    manifest = "api-database"
    replicas = 1
    requires_disk = True


class ApiWeb(ComponentResource):

    layer = "kel"
    manifest = "api-web"
    bundle = "api"
    replicas = 3


class ApiWorker(ComponentResource):

    layer = "kel"
    manifest = "api-worker"
    bundle = "api"
    replicas = 4


class LogAgent(KubernetesResource):

    def create(self):
        obj = self.get_api_objs("kel", "log-agent")["DaemonSet"][0]
        obj.create()
        logger.info('created "{}" daemonset'.format(obj.name))

    def destroy(self):
        obj = self.get_api_objs("kel", "log-agent")["DaemonSet"][0]
        self.delete_namespace(obj)
        logger.info('destroyed "{}" daemonset'.format(obj.name))


class Logstash(ComponentResource):

    layer = "kel"
    manifest = "logstash"
    replicas = 1


class LogStore(ComponentResource):

    layer = "kel"
    manifest = "log-store"
    replicas = 3
