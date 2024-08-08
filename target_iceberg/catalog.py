def get_catalog_config(
    conf,
) -> dict:
    config = {
        "type": conf.get("catalog_type"),
        "uri": conf.get("catalog_uri"),
        "warehouse": conf.get("catalog_name"),
        "py-io-impl": "pyiceberg.io.pyarrow.PyArrowFileIO",
        "s3.endpoint": conf.get("s3_endpoint"),
    }

    if conf.get("credential"):
        config["credential"] = conf.get("credential")
    else:
        config["s3.access-key-id"] = conf.get("s3_access_key_id")
        config["s3.secret-access-key"] = conf.get("s3_secret_access_key")

    return config
