from __future__ import annotations

import json
import os
import urllib.parse
import urllib.request
from dataclasses import dataclass, field

import boto3
from botocore.config import Config


@dataclass
class STSAuth:
    """Credentials for the OIDC → STS → temp-credentials flow.

    Used when the S3 endpoint is a reverse proxy that validates JWT session tokens
    (e.g. s3sentinel). Set all fields to enable; leave unset for standard AWS auth.

    Supports two OIDC grant types:
    - "password"            — Keycloak ROPC (username + password required)
    - "client_credentials"  — ZITADEL / any IdP that supports machine-to-machine auth
                              (client_id + client_secret required; username/password ignored)
    """

    keycloak_url: str
    sts_endpoint: str
    client_id: str = "s3sentinel"
    role_arn: str = "arn:aws:iam::000000000000:role/s3sentinel"
    role_session_name: str = "benchmark"
    grant_type: str = "password"        # "password" or "client_credentials"
    username: str | None = None         # required for password grant
    password: str | None = None         # required for password grant
    client_secret: str | None = None    # required for client_credentials grant
    # Extra scopes appended to the token request (space-separated).
    # ZITADEL requires urn:zitadel:iam:org:project:id:<project_id>:aud
    # to include the project audience in the JWT.
    oidc_scope: str = "openid"

    @classmethod
    def from_env(cls) -> STSAuth | None:
        """Return an STSAuth from env vars, or None if not configured."""
        keycloak_url = os.getenv("KEYCLOAK_URL")
        sts_endpoint = os.getenv("STS_ENDPOINT_URL")
        if not all([keycloak_url, sts_endpoint]):
            return None
        grant_type = os.getenv("OIDC_GRANT_TYPE", "password")
        # Validate we have enough credentials for the requested grant type
        if grant_type == "client_credentials":
            if not os.getenv("OIDC_CLIENT_SECRET"):
                return None
        else:
            if not all([os.getenv("OIDC_USERNAME"), os.getenv("OIDC_PASSWORD")]):
                return None
        return cls(
            keycloak_url=keycloak_url,
            sts_endpoint=sts_endpoint,
            client_id=os.getenv("OIDC_CLIENT_ID", "s3sentinel"),
            role_arn=os.getenv("ROLE_ARN", "arn:aws:iam::000000000000:role/s3sentinel"),
            role_session_name=os.getenv("ROLE_SESSION_NAME", "benchmark"),
            grant_type=grant_type,
            username=os.getenv("OIDC_USERNAME"),
            password=os.getenv("OIDC_PASSWORD"),
            client_secret=os.getenv("OIDC_CLIENT_SECRET"),
            oidc_scope=os.getenv("OIDC_SCOPE", "openid"),
        )

    def resolve(self) -> dict:
        """Fetch an OIDC token and exchange it for S3 credentials via STS.

        Returns a dict with AccessKeyId, SecretAccessKey, SessionToken.
        """
        jwt_token = self._get_jwt_token()
        return self._assume_role(jwt_token)

    def _get_jwt_token(self) -> str:
        if self.grant_type == "client_credentials":
            params = {
                "grant_type": "client_credentials",
                "scope": self.oidc_scope,
            }
            # client_secret_basic: credentials in Authorization header
            credentials = urllib.parse.quote(self.client_id, safe="") + ":" + urllib.parse.quote(self.client_secret or "", safe="")
            import base64
            auth_header = "Basic " + base64.b64encode(credentials.encode()).decode()
            req = urllib.request.Request(
                self.keycloak_url,
                data=urllib.parse.urlencode(params).encode(),
                headers={"Authorization": auth_header, "Content-Type": "application/x-www-form-urlencoded"},
            )
            with urllib.request.urlopen(req) as r:
                return json.load(r)["access_token"]
        else:
            data = urllib.parse.urlencode(
                {
                    "grant_type": "password",
                    "client_id": self.client_id,
                    "username": self.username,
                    "password": self.password,
                    "scope": self.oidc_scope,
                }
            ).encode()
            with urllib.request.urlopen(self.keycloak_url, data) as r:
                return json.load(r)["access_token"]

    def _assume_role(self, jwt_token: str) -> dict:
        # Set DEBUG_STS=1 to dump the full STS request/response (incl. bodies)
        # to stdout — useful for seeing why a proxy rejects the web-identity call.
        if os.getenv("DEBUG_STS"):
            import logging

            boto3.set_stream_logger("botocore", logging.DEBUG)

        sts = boto3.client(
            "sts",
            endpoint_url=self.sts_endpoint,
            aws_access_key_id="placeholder",
            aws_secret_access_key="placeholder",
            region_name="us-east-1",
        )

        from botocore.exceptions import ClientError

        try:
            resp = sts.assume_role_with_web_identity(
                RoleArn=self.role_arn,
                RoleSessionName=self.role_session_name,
                WebIdentityToken=jwt_token,
            )
        except ClientError as e:
            meta = e.response.get("ResponseMetadata", {})
            raise RuntimeError(
                "AssumeRoleWithWebIdentity rejected by "
                f"{self.sts_endpoint}: HTTP {meta.get('HTTPStatusCode')} "
                f"error={e.response.get('Error')} headers={meta.get('HTTPHeaders')}"
            ) from e
        return resp["Credentials"]


@dataclass
class S3Config:
    """Connection settings for an S3-compatible endpoint.

    Credentials follow standard boto3 precedence: explicit values here take
    priority, otherwise boto3 reads AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY
    from the environment (compatible with K8s Secrets and IAM roles).

    Set endpoint_url=None to target real AWS S3.
    """

    bucket: str
    endpoint_url: str | None = field(default_factory=lambda: os.getenv("S3_ENDPOINT_URL") or os.getenv("LOCALSTACK_ENDPOINT"))  # None → real AWS S3
    region: str = field(default_factory=lambda: os.getenv("AWS_DEFAULT_REGION", "us-east-1"))
    aws_access_key_id: str | None = field(default_factory=lambda: os.getenv("AWS_ACCESS_KEY_ID"))
    aws_secret_access_key: str | None = field(default_factory=lambda: os.getenv("AWS_SECRET_ACCESS_KEY"))
    aws_session_token: str | None = None  # populated from STS temp credentials

    @classmethod
    def from_sts(cls, bucket: str, endpoint_url: str, region: str, sts_auth: STSAuth) -> S3Config:
        """Build an S3Config by resolving credentials via the STS/OIDC flow."""
        creds = sts_auth.resolve()
        return cls(
            bucket=bucket,
            endpoint_url=endpoint_url,
            region=region,
            aws_access_key_id=creds["AccessKeyId"],
            aws_secret_access_key=creds["SecretAccessKey"],
            aws_session_token=creds["SessionToken"],
        )

    def make_client(self):
        """Create a boto3 S3 client from this config.

        Always call inside the worker process — never share clients across forks.
        Uses path-style addressing for custom endpoints (localstack / MinIO),
        and virtual-hosted style for real AWS S3.
        """
        addressing = "path" if self.endpoint_url else "auto"
        s3_config: dict = {"addressing_style": addressing}
        extra: dict = {}
        if self.endpoint_url:
            # S3-compatible endpoints (UpCloud, localstack, MinIO) don't support
            # boto3's default payload signing / checksum behaviour; disable both.
            s3_config["payload_signing_enabled"] = False
            extra["request_checksum_calculation"] = "when_required"
            extra["response_checksum_validation"] = "when_required"
        kwargs: dict = dict(
            region_name=self.region,
            config=Config(s3=s3_config, **extra),
        )
        if self.endpoint_url:
            kwargs["endpoint_url"] = self.endpoint_url
        if self.aws_access_key_id:
            kwargs["aws_access_key_id"] = self.aws_access_key_id
        if self.aws_secret_access_key:
            kwargs["aws_secret_access_key"] = self.aws_secret_access_key
        if self.aws_session_token:
            kwargs["aws_session_token"] = self.aws_session_token
        return boto3.client("s3", **kwargs)


@dataclass
class BenchmarkParams:
    """Tuning knobs for a benchmark run."""

    big_file_size_mb: int = 100
    small_file_size_kb: int = 10
    small_file_count: int = 100
    processes: int = 4
