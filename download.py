import hashlib
import os
import traceback
from collections import namedtuple
from urllib.parse import urlparse

import requests
from requests import TooManyRedirects, ReadTimeout
from tqdm import tqdm


def hash_path(digest, dir_len=2, subdirs=4):
    """
    Break a hash digest into a path.
    """
    dirs = [digest[i:i + dir_len] for i in range(0, len(digest), dir_len)]
    for i in range(subdirs):
        dirs.append(digest[i*4:(i+1)*4])
    # print(dirs)
    return (os.path.join(*dirs[:subdirs]), "".join(dirs[subdirs:]))


def download_urls(df, data_dir):
    count = df.shape[0]

    tq = tqdm(df.iterrows(), total=count, smoothing=0.05)
    for index, row in tq:
        url = row["url"]
        try:
            tq.set_postfix(url=url, refresh=False)
            # print("Downloading %s" % (item.url,))
            download_item(url, data_dir)
        except (KeyboardInterrupt, SystemExit):
            raise
        except KeyError as e:
            traceback.print_exc()
            print("Things went wrong with %s" % (row,))

        except (requests.exceptions.ConnectionError, TooManyRedirects, ReadTimeout) as e:
            # traceback.print_exc()
            print("Failed", type(e), e)
            # item.success = 0
        except (ValueError,) as e:
            # raise e
            pass
            print("Failed", type(e), e)
            # item.success = 0


def download_item(url, download_dir):
    # download file
    skip_types = [".mp4", ".gif"]

    # test against skipping URLs.
    parsed_url = urlparse(url)
    filename = os.path.basename(parsed_url.path)
    if len(filename) > 20:
        filename = filename[-20:]
    # print("Filename: %s" % (filename,))
    extension = os.path.splitext(filename)[1]
    if extension in skip_types:
        raise ValueError(extension)
    file_path = os.path.join(download_dir, filename)

    r = requests.get(url, stream=True, timeout=5.0)
    # print("Headers: %s" % (r.headers,))
    mime_type = r.headers["Content-Type"]
    mime_info = MIME_TYPES.get(mime_type)
    if not mime_info or mime_info.skip:
        r.close()
        raise ValueError("Skipping Mime type: %s url %s" % (mime_type, url,))

    hasher = get_hasher()

    chunk_size = 1024 * 1024

    with open(file_path, 'wb') as fd:
        for chunk in r.iter_content(chunk_size=chunk_size):
            fd.write(chunk)
            # hash file
            hasher.update(chunk)

    digest = hasher.hexdigest()

    # new image, save and move it
    # compute path
    path, new_name = hash_path(digest, 2)
    new_filename = new_name + mime_info.extension

    # move file to new path
    new_dir = os.path.join(download_dir, path)
    new_location = os.path.join(new_dir, new_filename)

    os.makedirs(new_dir, exist_ok=True)
    os.rename(file_path, new_location)


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