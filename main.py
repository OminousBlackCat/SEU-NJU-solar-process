"""
*****************
*    main.py    *
*****************

本py文件为程序入口, 包含了在并行过程中各种数据流的传输过程与子程序任务的分配过程
代码可以分为五段:
section 1. frame process pre(帧处理过程前的预处理, 包含读入参数, 分配全局数组等)
section 2. 并行分配TaskA
section 3. data process pre(图像预处理流程前的预操作, 包含寻找序列, reshape全局数组等)
section 4. 并行分配TaskB
section 5. 删除临时文件, 退出程序

子程序包含两项主要task
TaskA:  frame_task(start_byte: int)->(output: dict)
        用来对从start_byte的文件流开始处理, 隐式输出一个dict存放在全局数组中
TaskB:  sequence_task(sequence_index: int)->(output: file)

@author: seu_wxy
"""

import multiprocessing


def frame_task(start_byte: int):
    """
    依赖全局文件流工作, 显式输入开始比特位
    每个子程序在线的从开始比特位寻找合适的帧头部
    每寻找到一个帧头部, 就执行一次解压操作
    并根据头部信息将解压数据放入全局数组, 同时写入csv文件

    :param start_byte: 此block的开始比特位
    """
    pass


def sequence_task(sequence_index: int):
    """
    依赖全局光谱数组工作, 显示输入序列维度序号(global_array.shape[0])
    每个子程序连续地处理一个二维数组(shape: [光谱深度, 图像宽度])
    并缓存一个三维数组(shape: [光谱深度, 序列长度, 图像宽度])
    最后将三维数组保存为文件(HA.fits & FE.fits)

    :param sequence_index: 此序列的序号
    """
    pass

