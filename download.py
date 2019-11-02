import hashlib
import os
import traceback
from collections import namedtuple
from urllib.parse import urlparse

import requests
from requests import TooManyRedirects, ReadTimeout
from tqdm import tqdm


def download_urls(df, data_dir):
    


    tq = tqdm(df.iterrows(), total=count, smoothing=0.05)
    for index, row in tq:
        try:
            tq.set_postfix(url=row["url"], refresh=False)
            # print("Downloading %s" % (item.url,))
            download_item(row, data_dir)
        except (KeyboardInterrupt, SystemExit):
            raise
        except KeyError as e:
            traceback.print_exc()
            item.success = 0
            print("Things went wrong with %s" % (row,))

        except (requests.exceptions.ConnectionError, TooManyRedirects, ReadTimeout) as e:
            # traceback.print_exc()
            print("Failed", type(e), e)
            # item.success = 0
        except (ValueError,) as e:
            pass
            # print("Failed", type(e), e)
            # item.success = 0


def download_item(item, dir):
    # download file
    skip_types = [".mp4", ".gif"]
    url = item["url"]

    # test against skipping URLs.
    parsed_url = urlparse(url)
    filename = os.path.basename(parsed_url.path)
    if len(filename) > 20:
        filename = filename[-20:]
    # print("Filename: %s" % (filename,))
    extension = os.path.splitext(filename)[1]
    if extension in skip_types:
        raise ValueError(extension)
    file_path = os.path.join(dir, filename)

    r = requests.get(url, stream=True, timeout=5.0)
    # print("Headers: %s" % (r.headers,))
    mime_type = r.headers["Content-Type"]
    mime_info = MIME_TYPES.get(mime_type)
    if not mime_info or mime_info.skip:
        r.close()
        raise ValueError("Skipping Mime type: %s url %s" % (mime_type, item.url,))

    hasher = get_hasher()

    chunk_size = 1024 * 1024

    with open(file_path, 'wb') as fd:
        for chunk in r.iter_content(chunk_size=chunk_size):
            fd.write(chunk)
            # hash file
            hasher.update(chunk)

    digest = hasher.hexdigest()

    # check database to see if the hash exists
    exists = session.query(Item).filter_by(hash=digest).first()
    if not exists:
        # new image, save and move it
        # compute path
        path, new_name = hash_path(digest, 2)
        new_filename = new_name + mime_info.extension

        # move file
        new_path = os.path.join(".", path, new_filename)
        new_dir = os.path.join(dir, path)
        new_location = os.path.join(dir, new_path)

        os.makedirs(new_dir, exist_ok=True)
        os.rename(file_path, new_location)

        item.hash = digest
        item.path = new_path
    else:
        os.remove(file_path)
        # image already exists, set it as an alias
        item.alias = exists.id


MimeData = namedtuple("MimeData", ["skip", "extension"])

MIME_TYPES = {
    "video/mp4": MimeData(True, ".mp4"),
    "image/bmp": MimeData(False, ".bmp"),
    "image/gif": MimeData(True, ".gif"),
    "image/jpeg": MimeData(False, ".jpg"),
    "image/png": MimeData(False, ".png"),
    "video/webm": MimeData(True, ".webm"),
}

"""
image/*
"""

URL_BLACKLIST = [
    "i.minus.com",
    "anonmgur.com",
    "redditmirror.cc",
    "rule34-data-000.paheal.net",
    "artstation.com",
]


def hash_file(path):
    hasher = get_hasher()
    with open(path, mode='rb') as file:
        for chunk in file:
            hasher.update(chunk)
    return hasher

def get_hasher():
    salt = b"cat"
    hasher = hashlib.md5()
    hasher.update(salt)  # this is not secure, but just because I want to
    return hasher