def get_catalog_config(
    config,
) -> dict:
    catalog_config = {
        "type": config.get("catalog_type"),
        "uri": config.get("catalog_uri"),
        "warehouse": config.get("warehouse"),
        "py-io-impl": "pyiceberg.io.pyarrow.PyArrowFileIO",
        "s3.endpoint": config.get("s3_endpoint"),
    }

    if config.get("oauth2_server_uri"):
        catalog_config["oauth2-server-uri"] = config.get("oauth2_server_uri")
        catalog_config["scope"] = config.get("scope")

    if config.get("credential"):
        catalog_config["credential"] = config.get("credential")
    else:
        catalog_config["s3.access-key-id"] = config.get("s3_access_key_id")
        catalog_config["s3.secret-access-key"] = config.get("s3_secret_access_key")

    return catalog_config
