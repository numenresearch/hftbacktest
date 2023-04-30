import numpy as np
import pandas as pd
import glob
import os
import sys
import argparse
from datetime import datetime, timedelta

sys.path.append('..')

from hftbacktest.data.utils import binancefutures_mod, create_last_snapshot

# 设置浮点数的显示格式，保留10位有效数字
pd.set_option('display.float_format', '{:.10f}'.format)
np.set_printoptions(suppress=True, precision=10)


# 获取指定日期的文件列表
# symbol: bnbusdt
# date: 20230415
def get_files_by_date(symbol: str, date: str, directory: str = "examples/usdt/"):
    # 根据日期格式化文件名的通配符
    file_pattern = f"{directory}/{symbol}_{date}_*.txt.gz"

    # 使用glob来查找符合通配符的文件
    files = glob.glob(file_pattern)

    # # 提取文件名（不包括目录）
    # file_names = [os.path.basename(file) for file in files]

    # 对文件名列表进行排序
    sorted_files = sorted(files)

    return sorted_files



# 将数据转换为DataFrame
def convert_to_df(data):
    columns = ['event', 'exch_timestamp', 'local_timestamp', 'side', 'price', 'qty']
    df = pd.DataFrame(data, columns=columns)

    # print(df)

    # 转换数据类型
    df['event'] = df['event'].astype(int)
    df['exch_timestamp'] = df['exch_timestamp'].astype(np.int64)
    df['local_timestamp'] = df['local_timestamp'].astype(np.int64)
    df['side'] = df['side'].astype(int)

    return df


def process_date(symbol, tick_size: float, lot_size: float, date, data_dir, output_dir, save=True):
    npz_file = f'{output_dir}/{symbol}_{date}.npz'
    eod_file = f'{output_dir}/{symbol}_{date}_eod.npz'

    files = get_files_by_date(symbol, date, data_dir)
    data = binancefutures_mod.convert(files)

    if save:
        np.savez(npz_file, data=data)

    data = create_last_snapshot(data, tick_size=tick_size, lot_size=lot_size)

    if save:
        np.savez(eod_file, data=data)


def test():
    symbol = 'bnbusdt'
    date = '20230415'
    # date = '20230416'
    data_dir = 'C:/data/binancefutures/data'
    npz_file = f'../data/usdt/{symbol}_{date}.npz'
    eod_file = f'../data/usdt/{symbol}_{date}_eod.npz'
    save = True
    files = get_files_by_date(symbol, date, data_dir)
    # print(files)
    # files = files[0:1]
    # print(files)
    # files = ['examples/usdt/bnbusdt_20230415_0710-0720.txt.gz',
    #                                'examples/usdt/bnbusdt_20230415_0720-0730.txt.gz']
    data = binancefutures_mod.convert(files)
    # print(data)
    # print(type(data))
    if save:
        np.savez(npz_file, data=data)

    # tmp = np.load(npz_file)
    #
    # print('---tmp---')
    # print(tmp)

    # df = convert_to_df(data)
    # print(df)
    # print(df.dtypes)

    # Build 20230404 End of Day snapshot. It will be used for the initial snapshot for 20230405.
    # data = create_last_snapshot(npz_file, tick_size=0.01, lot_size=0.001)
    data = create_last_snapshot(data, tick_size=0.01, lot_size=0.001)
    # print(data)
    # df = convert_to_df(data)
    # print(df)
    if save:
        np.savez(eod_file, data=data)


def main(symbol, tick_size, lot_size, start_date, end_date, data_dir, output_dir):
    start_date_obj = datetime.strptime(start_date, "%Y%m%d")
    end_date_obj = datetime.strptime(end_date, "%Y%m%d")
    date_list = [start_date_obj + timedelta(days=x) for x in range((end_date_obj - start_date_obj).days + 1)]

    for date_obj in date_list:
        date = date_obj.strftime("%Y%m%d")
        # print(date)
        process_date(symbol, tick_size, lot_size, date, data_dir, output_dir)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Combine data by date or date range.')
    parser.add_argument('--symbol', type=str, required=True, help='Symbol, e.g. "bnbusdt"')
    parser.add_argument('--tick_size', type=float, required=True, help='Tick size for creating last snapshot')
    parser.add_argument('--lot_size', type=float, required=True, help='Lot size for creating last snapshot')
    parser.add_argument('--start_date', type=str, required=True, help='Start date in format YYYYMMDD')
    parser.add_argument('--end_date', type=str, required=True, help='End date in format YYYYMMDD')
    parser.add_argument('--data_dir', type=str, required=True, help='Path to the data directory')
    parser.add_argument('--output_dir', type=str, required=True, default='../data/usdt', help='Path to the output directory')

    args = parser.parse_args()

    main(args.symbol, args.tick_size, args.lot_size, args.start_date, args.end_date, args.data_dir, args.output_dir)
    # 命令行运行示例：
    # python .\convert.py --symbol bnbusdt --tick_size 0.01 --lot_size 0.01 --start_date 20230415 --end_date 20230417 --data_dir "C:/data/binancefutures/data" --output_dir "../data/usdt"
