import unittest

import pandas

from download import *


class TestDownload(unittest.TestCase):
    def test_hash_path(self):
        hasher = get_hasher()
        hasher.update(b"CAT")
        digest = hasher.hexdigest()
        dir_len = 2
        subdirs = 4
        path = hash_path(digest, dir_len, subdirs)
        print(path)

    def test_single_file(self):
        url = "https://www.gstatic.com/webp/gallery3/1.png"  # google static can handle my bandwidth, I'm pretty sure.
        data_dir = "./data/"
        download_item(url, data_dir)

    def test_multiple_files(self):
        url = "https://www.gstatic.com/webp/gallery3/1.png"  # google static can handle my bandwidth, I'm pretty sure.
        data_dir = "./data/"
        data = {"url": [url]}
        df = pandas.DataFrame(data)

        download_urls(df, data_dir)


if __name__ == '__main__':
    unittest.main()
