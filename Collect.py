import urllib.request as rq
import gzip
import os.path


base_str = "https://raw.githubusercontent.com/thepanacealab/covid19_twitter/master/dailies/"
base_export_dir = "./sources/"
def get_original_data(year, month, day):
    '''
    获取给定日期的全部twitter的数据
    :param year: 目标年份
    :param month: 目标月份
    :param day: 目标日
    :return: 返回一个文件
    '''
    month_str = "0"+str(month) if month < 10 else str(month)
    day_str = "0"+str(day) if day < 10 else str(day)
    target_url = base_str + f"{year}-{month_str}-{day_str}/{year}-{month_str}-{day_str}_clean-dataset.tsv.gz"
    if os.path.exists(target_url):
        return target_url, None
    else:
        gz, message = rq.urlretrieve(target_url, base_export_dir+f"{year}-{month_str}-{day_str}.tsv.gz")
        return gz


def extract(file):
    '''
    解压缩目标.gz文件
    :param file: 目标文件的位置
    :return: 解压缩后的文件的位置
    '''
    data_file = open(file, 'rb')
    data = data_file.read()
    decompressed_data = gzip.decompress(data)
    if os.path.exists(base_export_dir+os.path.basename(file)[:-3]):
        return base_export_dir+os.path.basename(file)[:-3]
    else:
        decompressed_file = open(base_export_dir+os.path.basename(file)[:-3], "wb")
        decompressed_file.write(decompressed_data)
        decompressed_file.close()

        return base_export_dir+os.path.basename(file)[:-3]
