#!/usr/bin/env python3
# coding:utf-8
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import os
import sys

sys.path.insert(0, os.getcwd())
os.system('rm ./atlashook.py')
os.system('hadoop fs -get hdfs://ns1018/user/jd_ad/ads_app/data_lineage/atlashook.py')
