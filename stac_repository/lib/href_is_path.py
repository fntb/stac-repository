import urllib.parse


def href_is_path(url: str) -> bool:
    return urllib.parse.urlparse(url, scheme="file").scheme == "file"
