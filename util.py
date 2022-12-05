"""
******************
*    util.py     *
******************

其他工具函数
@author: seu_wxy
"""


import datetime


# 输出辅助函数
def log(*args):
    print(datetime.datetime.now().strftime("[%Y-%m-%d-%H:%M:%S]:"), end="")
    print(*args)