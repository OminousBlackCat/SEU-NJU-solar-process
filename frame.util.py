"""
******************
*  frame.util.py *
******************

本py文件包含了解析帧所需的各类工具函数
@author: seu_lcl
@editor: seu_wxy
"""

import datetime
from copy import deepcopy
import util
import struct
from io import BytesIO
import struct
import config
import header
import struct

# 太阳空间望远镜科学载荷数据同步头[14,11,9,0,1,4,6,15]
# 图像帧同步头[5,5,10,10,5,5,10,10]
head_data = [14, 11, 9, 0, 1, 4, 6, 15]
head_check = [(14 << 4) + 11, (9 << 4), (1 << 4) + 4, (6 << 4) + 15]
head_pic = [5, 5, 10, 10, 5, 5, 10, 10, 1, 2, 3, 4, 5, 6, 7, 8]
pic_header = [15, 15, 4, 15, 15, 15, 5, 1]


def getNext(fread):
    """
    获取下一个字节数据
    """
    try:
        # 读取一个字节的内容
        data_raw = fread.read(1)
        # 转换为数字
        val_tuple = struct.unpack('B', data_raw)
        # 输出结果
        return int(val_tuple[0])
    except BaseException as exception:
        util.log(str(exception))
        # 文件读完返回-1
        util.log("文件已读完")
        return -1


def getData(fread, nums):
    """
    获取连续多个字节数据
    连续读取nums个字节
    """
    data = []
    error = True
    for i in range(nums):
        # 读取字符
        x = getNext(fread)
        if x == -1:
            # 无内容用0补齐
            error = False
            data.append(0)
            break
        else:
            # 输出提取内容
            data.append(x)
    # 不足补0
    while len(data) < nums:
        data.append(0)
    # 返回内容
    return data, error


def fileWrite(data, file):
    """
    写二进制文件
    filename为文件名
    """
    for x in data:
        # 打包回二进制
        a = struct.pack('B', x)
        # 写二进制文件
        file.write(a)


def getTarget(target):
    """
    获取目标同步头对应数值
    同步头一位代表四个二进制位
    """
    ans = 0
    for i in target:
        ans = (ans << 4) + i
    return ans


def getResult(target):
    """
    获取目标list对应值，按字节转换
    list头代表八个二进制位
    """
    ans = 0
    for i in target:
        ans = (ans << 8) + i
    return ans


def findFrameHead(fread, head_target):
    """
    寻找数据帧的探头
    """
    now = 0
    # 获取数据头的内容
    target = getTarget(head_target)

    while now != target:
        # 计算当前头内容
        now = now % (1 << (4 * (len(head_target) - 2)))
        X = getNext(fread)
        if X == -1:
            return "Error"
        now = (now << 8) + X
    return 1


def findPicHead(data):
    """
    寻找图片帧的开头
    输出图片帧开头所在的坐标第一个位置
    否则输出len(data)-7
    """
    n = len(data)
    # print(n)
    index = 0
    now = 0
    # 获取图片
    target = getTarget(head_pic)
    # print(target)
    while index < n:

        now = now % (1 << (4 * (len(head_pic) - 2)))
        now = (now << 8) + data[index]
        index = index + 1
        if now == target:
            break

    if now == target:
        return index - 8
    return n - 7


def processHeader(stream: BytesIO):
    """
    解析辅助数据并构造header(字典)
    """
    headDic = {}
    headList = []
    # stream代表输入的辅助数据流
    # 辅助数据格式    0~5(6)    时间码
    #               6~69(64)    定位数据
    #               70~127(58)  定轨数据
    #               128~183(56) 载荷仓数据
    #               184~211(28) 平台仓数据
    #               212~213(2)  温度量信息
    #               214~277(64) 望远镜工作参数
    #               278~493(216)填充数据

    # 处理定位数据
    # 定位数据需要的内容为
    stream.read(2)  # 跳过有的没的
    time_upper = int(struct.unpack('>H', stream.read(2))[0])
    time_lower = int(struct.unpack('>I', stream.read(4))[0])
    timeS = time_upper << 32
    timeS = timeS + time_lower  # 单位: 0.1 ms
    timeS = int(int(timeS) // int(10))  # 化为ms
    timeShort = timeS // 1000  # 化为S
    fileWriteTime = datetime.datetime(2000, 1, 1, 12, 0, 0, 0) + datetime.timedelta(
        days=float(timeShort // (3600 * 24)), seconds=float(timeShort % (3600 * 24)))  # 自2000年0点0秒起的累加秒
    headDic['STR_TIME'] = fileWriteTime.strftime("%Y-%m-%dT%H:%M:%S")
    headDic['TIME'] = fileWriteTime.strftime("%Y-%m-%dT%H-%M-%S")
    # stream.read(2 + 1 + 1 + 4 + 4 + 4 + 4 + 4 + 4 + 4 + 4 + 1)  # 跳过定位标志 可用星数 及其他数据 直到载荷舱工作模式
    signPosition = stream.read(1).hex()  # 定位标志
    starNum = stream.read(1).hex()  # 可用星数
    J2000 = struct.unpack('>I', stream.read(4))  # J2000时间整s
    timeMs = struct.unpack('>I', stream.read(4))  # J2000时间整ms
    xWPosition = struct.unpack('>f', stream.read(4))  # WGS-84坐标系X位置(J2000坐标系, 同下)
    yWPosition = struct.unpack('>f', stream.read(4))  # WGS-84坐标系Y位置
    zWPosition = struct.unpack('>f', stream.read(4))  # WGS-84坐标系Z位置
    xWVelocity = struct.unpack('>f', stream.read(4))  # WGS-84坐标系X速度
    yWVelocity = struct.unpack('>f', stream.read(4))  # WGS-84坐标系Y速度
    zWVelocity = struct.unpack('>f', stream.read(4))  # WGS-84坐标系Z速度
    sign = stream.read(1).hex()  # 定轨标志
    workMode = stream.read(1).hex()  # 获得载荷舱工作模式
    # headDic['workMode'] = workMode
    J2000Time = struct.unpack('>I', stream.read(4))  # J2000时间
    xPosition = struct.unpack('>f', stream.read(4))  # 星务计算X位置(J2000坐标系, 同下)
    yPosition = struct.unpack('>f', stream.read(4))  # 星务计算Y位置
    zPosition = struct.unpack('>f', stream.read(4))  # 星务计算Z位置
    xVelocity = struct.unpack('>f', stream.read(4))  # 星务计算X速度
    yVelocity = struct.unpack('>f', stream.read(4))  # 星务计算Y速度
    zVelocity = struct.unpack('>f', stream.read(4))  # 星务计算Z速度
    headDic['SAT_POS1'] = xPosition[0]
    headDic['SAT_POS2'] = yPosition[0]
    headDic['SAT_POS3'] = zPosition[0]
    headDic['SAT_VEL1'] = xVelocity[0]
    headDic['SAT_VEL2'] = yVelocity[0]
    headDic['SAT_VEL3'] = zVelocity[0]

    headList.append(signPosition)
    headList.append(starNum)
    headList.append(J2000[0])
    headList.append(timeMs[0])
    headList.append(xWPosition[0])
    headList.append(yWPosition[0])
    headList.append(zWPosition[0])
    headList.append(xWVelocity[0])
    headList.append(yWVelocity[0])
    headList.append(zWVelocity[0])
    headList.append(sign)
    headList.append(workMode)
    headList.append(J2000Time[0])
    headList.append(xPosition[0])
    headList.append(yPosition[0])
    headList.append(zPosition[0])
    headList.append(xVelocity[0])
    headList.append(yVelocity[0])
    headList.append(zVelocity[0])

    # 总读头移动数: 6 + 1 + 1 + 4 + 4 + 4 + 4 + 4 + 4 + 4 + 4 + 1 + 4 + 4 * 6 = 69

    # 定轨数据(58)
    # stream.read(58)
    headList.append(stream.read(1).hex())  # 定轨标志2
    headList.append(stream.read(1).hex())  # 可用星数2
    headList.append(struct.unpack('>I', stream.read(4))[0])  # 瞬根J2000时间整秒
    headList.append(struct.unpack('>I', stream.read(4))[0])  # 瞬根轨道半长轴(a)
    headList.append(struct.unpack('>I', stream.read(4))[0])  # 瞬根轨道半长轴(e)
    headList.append(struct.unpack('>I', stream.read(4))[0])  # 瞬根轨道半长轴(i)
    headList.append(struct.unpack('>I', stream.read(4))[0])  # 瞬根轨道半长轴(Ω)
    headList.append(struct.unpack('>I', stream.read(4))[0])  # 瞬根轨道半长轴(w)
    headList.append(struct.unpack('>I', stream.read(4))[0])  # 瞬根轨道半长轴(M)
    headList.append(struct.unpack('>I', stream.read(4))[0])  # 平根J2000时间整秒
    headList.append(struct.unpack('>I', stream.read(4))[0])  # 平根轨道半长轴(a)
    headList.append(struct.unpack('>I', stream.read(4))[0])  # 平根轨道半长轴(e)
    headList.append(struct.unpack('>I', stream.read(4))[0])  # 平根轨道半长轴(i)
    headList.append(struct.unpack('>I', stream.read(4))[0])  # 平根轨道半长轴(Ω)
    headList.append(struct.unpack('>I', stream.read(4))[0])  # 平根轨道半长轴(w)
    headList.append(struct.unpack('>I', stream.read(4))[0])  # 平根轨道半长轴(M)

    # 载荷舱姿态数据（56）
    # 0~7（8） 载荷舱惯性四元数Q0（8字节双精度）
    val_tuple = struct.unpack('>d', stream.read(8))
    headDic["Q0"] = val_tuple[0]
    headList.append(val_tuple[0])
    # 8~15（8） 载荷舱惯性四元数Q1（8字节双精度）
    val_tuple = struct.unpack('>d', stream.read(8))
    headDic["Q1"] = val_tuple[0]
    headList.append(val_tuple[0])
    # 16~23（8） 载荷舱惯性四元数Q2（8字节双精度）
    val_tuple = struct.unpack('>d', stream.read(8))
    headDic["Q2"] = val_tuple[0]
    headList.append(val_tuple[0])
    # 24~31（8） 载荷舱惯性四元数Q3（8字节双精度）
    val_tuple = struct.unpack('>d', stream.read(8))
    headDic["Q3"] = val_tuple[0]
    headList.append(val_tuple[0])
    # 角速率32~55(24)
    headList.append(struct.unpack('>d', stream.read(8))[0])  # 载荷舱角速率X
    headList.append(struct.unpack('>d', stream.read(8))[0])  # 载荷舱角速率y
    headList.append(struct.unpack('>d', stream.read(8))[0])  # 载荷舱角速率z

    # 平台舱姿态数据（28）
    # stream.read(28)
    headList.append(struct.unpack('>f', stream.read(4))[0])  # 载荷舱惯性四元数Q0
    headList.append(struct.unpack('>f', stream.read(4))[0])  # 载荷舱惯性四元数Q1
    headList.append(struct.unpack('>f', stream.read(4))[0])  # 载荷舱惯性四元数Q2
    headList.append(struct.unpack('>f', stream.read(4))[0])  # 载荷舱惯性四元数Q3
    headList.append(struct.unpack('>f', stream.read(4))[0])  # 载荷舱角速率X
    headList.append(struct.unpack('>f', stream.read(4))[0])  # 载荷舱角速率Y
    headList.append(struct.unpack('>f', stream.read(4))[0])  # 载荷舱角速率Z

    # 温度数据（2）
    headList.append(struct.unpack('>b', stream.read(1))[0])  # 全日面面阵成像单元焦平面组件温度
    headList.append(struct.unpack('>b', stream.read(1))[0])  # 光谱成像单元焦平面组件温度

    # 望远镜工作参数（64）
    # 0~12（13） 电机1参数
    # 0~3 电机1读出位置 无符号二进制整型
    val_tuple = struct.unpack('>I', stream.read(4))
    headList.append(val_tuple[0])
    # 4~5 电机1位置误差 无符号二进制整型
    val_tuple = struct.unpack('>H', stream.read(2))
    headList.append(val_tuple[0])
    # 6~7 电机1读出速度 无符号二进制整型
    val_tuple = struct.unpack('>H', stream.read(2))
    headList.append(val_tuple[0])
    # 8~9 电机1读出电流 无符号二进制整型
    val_tuple = struct.unpack('>H', stream.read(2))
    headList.append(val_tuple[0])
    # 10 电机1状态 无符号二进制整型
    val_tuple = struct.unpack('>B', stream.read(1))
    headList.append(val_tuple[0])
    # 11 电机1警报信息 无符号二进制整型
    val_tuple = struct.unpack('>B', stream.read(1))
    headList.append(val_tuple[0])
    # 12 电机1传感器使用和超限信息 无符号二进制整型
    val_tuple = struct.unpack('>B', stream.read(1))
    headList.append(val_tuple[0])

    # 13~25（13）   电机2参数
    # 13~16 电机2读出位置 无符号二进制整型
    val_tuple = struct.unpack('>I', stream.read(4))
    headList.append(val_tuple[0])
    # 17~18 电机2位置误差 无符号二进制整型
    val_tuple = struct.unpack('>H', stream.read(2))
    headList.append(val_tuple[0])
    # 19~20 电机2读出速度 无符号二进制整型
    val_tuple = struct.unpack('>H', stream.read(2))
    headList.append(val_tuple[0])
    # 21~22 电机2读出电流 无符号二进制整型
    val_tuple = struct.unpack('>H', stream.read(2))
    headList.append(val_tuple[0])
    # 23 电机2状态 无符号二进制整型
    val_tuple = struct.unpack('>B', stream.read(1))
    headList.append(val_tuple[0])
    # 24 电机2警报信息 无符号二进制整型
    val_tuple = struct.unpack('>B', stream.read(1))
    headList.append(val_tuple[0])
    # 25 电机2传感器使用和超限信息 无符号二进制整型
    val_tuple = struct.unpack('>B', stream.read(1))
    headList.append(val_tuple[0])

    # 26~29（4）    成像帧计数（无符号整型4位）
    val_tuple = struct.unpack('>I', stream.read(4))
    headDic["SCN_NUM"] = val_tuple[0]
    headList.append(val_tuple[0])
    # 30~33(4)      帧内计数
    val_tuple = struct.unpack('>I', stream.read(4))
    headDic["FRM_NUM"] = val_tuple[0]
    headList.append(val_tuple[0])

    headList.append(struct.unpack('>B', stream.read(1))[0])  # 图像像素位数
    headList.append(struct.unpack('>B', stream.read(1))[0])  # 扫描成像的数据维度
    headList.append(struct.unpack('>H', stream.read(2))[0])  # 空间维X方向上的像元数
    headList.append(struct.unpack('>H', stream.read(2))[0])  # 空间维Y方向上的像元数
    headList.append(struct.unpack('>H', stream.read(2))[0])  # 光谱维波长点数目
    headList.append(struct.unpack('>H', stream.read(2))[0])  # 开窗1行起始位置
    headList.append(struct.unpack('>H', stream.read(2))[0])  # 开窗1列起始位置
    headList.append(struct.unpack('>H', stream.read(2))[0])  # 开窗1行数
    headList.append(struct.unpack('>H', stream.read(2))[0])  # 开窗2行起始位置
    headList.append(struct.unpack('>H', stream.read(2))[0])  # 开窗2列起始位置
    headList.append(struct.unpack('>H', stream.read(2))[0])  # 开窗2行数
    headList.append(struct.unpack('>H', stream.read(2))[0])  # 曝光时间
    headList.append(struct.unpack('>B', stream.read(1))[0])  # 成像模式
    headList.append(struct.unpack('>B', stream.read(1))[0])  # 增益
    headList.append(struct.unpack('>H', stream.read(2))[0])  # 探测器中心位置波长
    lin_disp = struct.unpack('>H', stream.read(2))[0]
    headList.append(lin_disp)  # 像元光谱分辨率
    # headDic["LIN_DISP"] = lin_disp
    spec_res = struct.unpack('>H', stream.read(2))[0]
    headList.append(spec_res)  # 像元空间分辨率
    # headDic["SPEC_RES"] = spec_res

    stream.close()
    return headDic, headList


def processPicStream(data):
    """
    对图片帧文件进行处理
    """
    # 提取自定义数据区
    dataHead = data[8: 8 + 280]
    headStream = BytesIO()

    # 文件流回到开头
    fileWrite(dataHead, headStream)
    headStream.seek(0)
    # 去除自定义数据区
    data = data[8 + 278:]
    # 获取数据长度
    N = len(data)
    indexList = []
    target = getTarget(pic_header)
    # 统计所有头部分
    for i in range(N - 8):
        # 找图片的开头
        if target == getResult(data[i:i + 4]):
            indexList.append(i)
    M = len(indexList)
    # print(indexList)
    # print(M)
    # 不足8的倍数补0
    if M == 0:
        return [], [], []
    while len(data) % 8 > 0:
        data.append(0)
    # 输出所有文件
    FileList = []
    for i in range(M - 1):
        # 创建文件流
        FileList.append(BytesIO())
        # 写文件流
        fileWrite(data[indexList[i]:indexList[i + 1]], FileList[i])
        # 文件流回到开头
        FileList[i].seek(0)
    # 创建文件流
    FileList.append(BytesIO())
    # 写文件流
    fileWrite(data[indexList[M - 1]:], FileList[M - 1])
    # 文件流回到开头
    FileList[M - 1].seek(0)
    headDic, headList = processHeader(headStream)
    return headDic, headList, FileList


def check(data, Error_control):
    """
    对读取的数据做异或检查
    输出bool
    检查通过则为True 不通过为False
    """
    check_xor = deepcopy(Error_control)
    # 获取异或检查长度
    control_num = len(Error_control)

    # 标记数据对应校验位
    id = 0
    # 对数据做检验
    for data_list in data:
        # 对每部分数据做操作
        for x in data_list:
            check_xor[id] ^= x
            # 标记为转移
            id = (id + 1) % control_num
    # 检查是否所有位数都为0
    check_ans = 0
    # 对所有结果求或
    for x in check_xor:
        check_ans |= x
    return True


def dataWork(fread):
    """
    输入文件流 对文件流工作
    输出List
    格式 每个元素为一个文件流和一个list
    文件流为头文件信息 list为所有子图片的文件流
    """
    # num统计读取文件数量
    num = 0
    # 记录图片帧内容
    PicData = []
    # 记录数据
    Data = []
    # 存储所有图片信息
    header_dic_data = []
    header_list_data = []
    pic_data = []

    # 寻找数据帧的开头
    while findFrameHead(fread, head_data) == 1:
        # 记录头部份
        now = fread.tell()
        MainHead, not_error = getData(fread, 8)
        if not_error:
            break
        # if not check([MainHead[:6]],MainHead[6:]):
        #     fread.seek(now)
        #     continue

        # 提取数据部分
        Data, not_error = Data + getData(fread, 2032)
        if not_error:
            break
        # 记录错误控制内容
        ErrorControl, not_error = getData(fread, 4)
        if not_error:
            break
        if not check([head_check, MainHead, Data], ErrorControl):
            fread.seek(now)
            continue
        # 在数据帧中寻找图像帧开头，如果有输出图像帧开头的index
        index = findPicHead(Data)
        # 最终输出的头部信息 应该是个字典
        headerDic = None
        # 输出的csv row
        headerList = None
        # 需要拼接图像list
        picList = None

        # 判断有无出现数据帧开头
        if index < len(Data) - 7:
            # 记录新图像帧之前的内容
            PicData = PicData + Data[:index]
            # 去除提取的内容
            Data = Data[index + 8:]
            # 无用内容不处理
            if num > 0:
                # 处理图像帧内容
                headerDic, headerList, picList = processPicStream(PicData)
                # 存储图片头部信息
                if len(headerDic) > 0:
                    header_dic_data.append(deepcopy(headerDic))
                    header_list_data.append(deepcopy(headerList))
                    # 储存图片信息
                    pic_data.append(deepcopy(picList))
            # 情况图像帧
            PicData = []
            # 编号计数
            num = num + 1

        else:
            # 提取图像帧内容
            PicData = PicData + Data[:index]
            # 保留可能出现数据头的内容
            Data = Data[index:]
            # 无用内容不处理
    if num > 0 and len(PicData) > 1000:
        try:
            # 处理图像帧内容
            headerDic, headerList, picList = processPicStream(PicData)
            # 存储图片头部信息
            if len(headerDic) > 0:
                header_dic_data.append(deepcopy(headerDic))
                header_list_data.append(deepcopy(headerList))
                # 储存图片信息
                pic_data.append(deepcopy(picList))
        except BaseException as exception:
            # 文件读完返回-1
            util.log("剩余文件无法形成单独图像")
    # 输出处理信息
    util.log("无数据头，解压结束")
    util.log("发现图片帧：" + str(num))
    # 输出所有图片信息
    return header_dic_data, header_list_data, pic_data


def parallel_work(fread, start_byte):
    """
    用于并行工作
    输入为文件楼，起始字符地址，终止地址坐标
    """
    # 确定文件末尾位置
    end_byte = start_byte + config.iteration_chunk_size
    # 寻找文件的开头位置
    fread.seek(start_byte)
    # num统计读取文件数量
    num = 0
    # 记录图片帧内容
    PicData = []
    # 记录数据
    Data = []
    # 存储所有图片信息
    header_dic_data = []
    header_list_data = []
    pic_data = []
    # 寻找数据帧的开头
    while findFrameHead(fread, head_data) == 1:
        try:
            # 记录头部份
            now = fread.tell()
            MainHead, not_error = getData(fread, 8)
            if not not_error:
                util.log("头部解析错处")
                raise ValueError
            if not check([MainHead[:6]], MainHead[6:]):
                fread.seek(now)
                continue
            # 提取数据部分
            Data1, not_error = getData(fread, 2032)
            if not not_error:
                util.log("数据部分帧读取出错")
                raise ValueError
            Data = Data + deepcopy(Data1)
            # 记录错误控制内容
            ErrorControl, not_error = getData(fread, 4)
            if not not_error:
                util.log("差错校检出错")
                raise ValueError
            if not check([head_check, MainHead, Data], ErrorControl):
                fread.seek(now)
                continue
            # 在数据帧中寻找图像帧开头，如果有输出图像帧开头的index
            index = findPicHead(Data)
            # 最终输出的头部信息 应该是个字典
            headerDic = None
            # 输出的csv row
            headerList = None
            # 需要拼接图像list
            picList = None

            # 判断有无出现数据帧开头
            if index < len(Data) - 7:
                # 记录新图像帧之前的内容
                PicData = PicData + Data[:index]
                # 去除提取的内容
                Data = Data[index + 8:]
                # 无用内容不处理
                if num > 0:
                    # 处理图像帧内容
                    headerDic, headerList, picList = processPicStream(PicData)
                    if len(picList) == 6:
                        # 处理图像帧内容
                        # 存储图片头部信息
                        header_dic_data.append(deepcopy(headerDic))
                        header_list_data.append(deepcopy(headerList))
                        # 储存图片信息
                        pic_data.append(deepcopy(picList))
                    # 判断终止条件
                    if now > end_byte:
                        PicData = []
                        break
                # 情况图像帧
                PicData = []
                # 编号计数
                num = num + 1
                #
            else:
                # 提取图像帧内容
                PicData = PicData + Data[:index]
                # 保留可能出现数据头的内容
                Data = Data[index:]
                # 无用内容不处理
            # if 1.5 * (end_byte - start_byte) < fread.tell() - start_byte:
            #     break
        except ValueError as ve:
            util.log(str(ve))
            util.log("当前frame解析出错")
    if num > 0 and len(PicData) > 10000:
        try:
            # 处理图像帧内容
            headerDic, headerList, picList = processPicStream(PicData)
            if len(picList) == 6:
                # 存储图片头部信息
                header_dic_data.append(deepcopy(headerDic))
                header_list_data.append(deepcopy(headerList))
                # 储存图片信息
                pic_data.append(deepcopy(picList))
        except BaseException as exception:
            # 文件读完返回-1
            util.log("剩余文件无法形成单独图像")
    # 输出处理信息
    # print("无数据头，解压结束")
    # print("发现图片帧：" + str(num))
    # 输出所有图片信息
    return header_dic_data, header_list_data, pic_data
