import collections
import datetime
import functools
import os
import uuid

from cryptography import x509
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import rsa


class KeyKeeper:

    def __init__(self, path):
        self.path = path
        self.ca = {"key": None, "certificate": None}
        self.keypairs = collections.defaultdict(functools.partial(dict, key=None, certificate=None))

    @staticmethod
    def encode_key_to_pem(key):
        return key.private_bytes(
            encoding=serialization.Encoding.PEM,
            format=serialization.PrivateFormat.TraditionalOpenSSL,
            encryption_algorithm=serialization.NoEncryption(),
        )

    @staticmethod
    def encode_certificate_to_pem(certificate):
        return certificate.public_bytes(serialization.Encoding.PEM)

    def get_key(self, name):
        if self.keypairs[name]["key"]:
            return self.keypairs[name]["key"]
        else:
            if self.path:
                with open(os.path.join(self.path, "{}-key.pem".format(name)), "rb") as fp:
                    key = serialization.load_pem_private_key(fp.read(), None, default_backend())
            else:
                key = rsa.generate_private_key(
                    public_exponent=65537,
                    key_size=2048,
                    backend=default_backend()
                )
        self.keypairs[name]["key"] = key
        return key

    def get_certificate_authority_key(self):
        return self.get_key("ca")

    def get_certificate_authority_certificate(self):
        if self.keypairs["ca"]["certificate"]:
            return self.keypairs["ca"]["certificate"]
        else:
            if self.path:
                with open(os.path.join(self.path, "ca.pem"), "rb") as fp:
                    certificate = x509.load_pem_x509_certificate(fp.read(), default_backend())
            else:
                ca_key = self.get_certificate_authority_key()
                builder = x509.CertificateBuilder()
                builder = builder.serial_number(int(uuid.uuid4()))
                builder = builder.not_valid_before(datetime.datetime.today() - datetime.timedelta(1, 0, 0))
                builder = builder.not_valid_after(datetime.datetime(2018, 8, 2))
                builder = builder.public_key(ca_key.public_key())
                builder = builder.subject_name(x509.Name([
                    x509.NameAttribute(x509.NameOID.COUNTRY_NAME, "US"),
                    x509.NameAttribute(x509.NameOID.STATE_OR_PROVINCE_NAME, "CO"),
                    x509.NameAttribute(x509.NameOID.LOCALITY_NAME, "Denver"),
                    x509.NameAttribute(x509.NameOID.ORGANIZATION_NAME, "Eldarion, Inc."),
                    x509.NameAttribute(x509.NameOID.COMMON_NAME, "eldarion.com"),
                ]))
                builder = builder.issuer_name(x509.Name([
                    x509.NameAttribute(x509.NameOID.COUNTRY_NAME, "US"),
                    x509.NameAttribute(x509.NameOID.STATE_OR_PROVINCE_NAME, "CO"),
                    x509.NameAttribute(x509.NameOID.LOCALITY_NAME, "Denver"),
                    x509.NameAttribute(x509.NameOID.ORGANIZATION_NAME, "Eldarion, Inc."),
                    x509.NameAttribute(x509.NameOID.COMMON_NAME, "eldarion.com"),
                ]))
                builder = builder.add_extension(
                    x509.BasicConstraints(
                        ca=True,
                        path_length=None
                    ),
                    critical=False,
                )
                certificate = builder.sign(
                    private_key=ca_key,
                    algorithm=hashes.SHA256(),
                    backend=default_backend(),
                )
        self.keypairs["ca"]["certificate"] = certificate
        return certificate

    def get_raw_certificate(self, name):
        assert self.path, "raw certificate (name={}) requested, must be present in key dir".format(name)
        with open(os.path.join(self.path, "{}.pem".format(name)), "rb") as fp:
            return fp.read()

    def get_certificate(self, name, opts):
        if self.keypairs[name]["certificate"]:
            return self.keypairs[name]["certificate"]
        else:
            if self.path:
                with open(os.path.join(self.path, "{}.pem".format(name)), "rb") as fp:
                    certificate = x509.load_pem_x509_certificate(fp.read(), default_backend())
            else:
                ca_key = self.get_certificate_authority_key()
                ca_certificate = self.get_certificate_authority_certificate()
                builder = x509.CertificateBuilder()
                builder = builder.serial_number(int(uuid.uuid4()))
                builder = builder.not_valid_before(datetime.datetime.today() - datetime.timedelta(1, 0, 0))
                builder = builder.not_valid_after(datetime.datetime(2018, 8, 2))
                builder = builder.public_key(ca_key.public_key())
                builder = builder.subject_name(x509.Name([
                    x509.NameAttribute(x509.NameOID.COUNTRY_NAME, "US"),
                    x509.NameAttribute(x509.NameOID.STATE_OR_PROVINCE_NAME, "CO"),
                    x509.NameAttribute(x509.NameOID.LOCALITY_NAME, "Denver"),
                    x509.NameAttribute(x509.NameOID.ORGANIZATION_NAME, "Eldarion, Inc."),
                    x509.NameAttribute(x509.NameOID.COMMON_NAME, "kube-{}".format(name)),
                ]))
                builder = builder.issuer_name(ca_certificate.issuer)
                if opts.get("sans"):
                    builder = builder.add_extension(
                        x509.SubjectAlternativeName(opts["sans"]),
                        critical=False,
                    )
                builder = builder.add_extension(
                    x509.BasicConstraints(
                        ca=False,
                        path_length=None
                    ),
                    critical=False,
                )
                certificate = builder.sign(
                    private_key=ca_key,
                    algorithm=hashes.SHA256(),
                    backend=default_backend(),
                )
        self.keypairs[name]["certificate"] = certificate
        return certificate
