#!/usr/bin/env python3

import os

os.system('rm ./atlashook.py')
os.system('hadoop fs -get hdfs://ns1018/user/jd_ad/ads_app/data_lineage/atlashook.py')