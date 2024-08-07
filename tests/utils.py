import os
import uuid


def get_config():
    config = {}

    config["catalog_uri"] = "http://localhost:8181"
    config["catalog_name"] = "demo"
    config["catalog_type"] = "rest"
    config["database"] = uuid.uuid4().hex
    config["s3_endpoint"] = "http://localhost:9000"
    config["s3_access_key_id"] = "admin"
    config["s3_secret_access_key"] = "password"

    return config
