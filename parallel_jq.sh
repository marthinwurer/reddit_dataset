#!/bin/bash
</mnt/data/datasets/reddit_dumps/RS_2019-08 parallel --progress --pipe -j 10 jq -ca -f filter.jq > out.json

