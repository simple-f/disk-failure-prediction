#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
特征工程模块 - 时间窗口特征
"""

import pandas as pd
import numpy as np
from typing import List
import logging
from pathlib import Path
import yaml

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class FeatureEngineer:
    """特征工程师"""
    
    def __init__(self, config_path='config/config.yaml'):
        """初始化特征工程师"""
        with open(config_path, 'r', encoding='utf-8') as f:
            config = yaml.safe_load(f)
        
        self.smart_cols = config['feature_engineering']['smart_attributes']
        self.windows = config['feature_engineering']['windows']
        self.lags = config['feature_engineering']['lags']
        self.feature_cols = []
    
    def create_time_window_features(self, df: pd.DataFrame, 
                                     serial_col: str = 'serial_number',
                                     date_col: str = 'timestamp') -> pd.DataFrame:
        """
        创建时间窗口特征
        
        参数:
            df: 原始数据
            serial_col: 硬盘序列号列
            date_col: 时间列
        
        返回:
            带有时间窗口特征的数据框
        """
        df = df.copy()
        df[date_col] = pd.to_datetime(df[date_col])
        df = df.sort_values([serial_col, date_col])
        
        result_dfs = []
        
        for serial in df[serial_col].unique():
            disk_data = df[df[serial_col] == serial].copy()
            
            for smart in self.smart_cols:
                if smart not in disk_data.columns:
                    continue
                
                for window in self.windows:
                    # 滚动窗口特征
                    disk_data[f'{smart}_mean_{window}d'] = disk_data[smart].rolling(
                        window=window, min_periods=1
                    ).mean()
                    
                    disk_data[f'{smart}_max_{window}d'] = disk_data[smart].rolling(
                        window=window, min_periods=1
                    ).max()
                    
                    disk_data[f'{smart}_min_{window}d'] = disk_data[smart].rolling(
                        window=window, min_periods=1
                    ).min()
                    
                    disk_data[f'{smart}_std_{window}d'] = disk_data[smart].rolling(
                        window=window, min_periods=1
                    ).std().fillna(0)
                    
                    # 变化量（delta）
                    disk_data[f'{smart}_delta_{window}d'] = disk_data[smart].diff(window).fillna(0)
                    
                    # 变化次数（超过 90 分位数的次数）
                    threshold = disk_data[smart].quantile(0.9)
                    disk_data[f'{smart}_spike_{window}d'] = (
                        disk_data[smart] > threshold
                    ).rolling(window=window).sum().fillna(0)
            
            result_dfs.append(disk_data)
        
        result_df = pd.concat(result_dfs, ignore_index=True)
        
        # 填充 NaN
        result_df = result_df.ffill().fillna(0)
        
        logger.info(f"时间窗口特征完成：{len(df.columns)} 列 → {len(result_df.columns)} 列")
        
        return result_df
    
    def create_lag_features(self, df: pd.DataFrame,
                             serial_col: str = 'serial_number') -> pd.DataFrame:
        """
        创建滞后特征（前 N 天的值）
        """
        df = df.copy()
        df = df.sort_values([serial_col, 'timestamp'])
        
        for serial in df[serial_col].unique():
            mask = df[serial_col] == serial
            disk_data = df.loc[mask].copy()
            
            for smart in self.smart_cols:
                if smart not in disk_data.columns:
                    continue
                
                for lag in self.lags:
                    df.loc[mask, f'{smart}_lag_{lag}d'] = disk_data[smart].shift(lag)
        
        # 填充 NaN
        df = df.ffill().fillna(0)
        
        logger.info(f"滞后特征完成")
        
        return df
    
    def create_trend_features(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        创建趋势特征（斜率）
        """
        df = df.copy()
        
        for smart in self.smart_cols:
            if smart not in df.columns:
                continue
            
            # 7 天趋势（线性回归斜率）
            def calc_slope(y):
                if len(y) < 3:
                    return 0
                try:
                    return np.polyfit(range(len(y)), y, 1)[0]
                except:
                    return 0
            
            df[f'{smart}_trend_7d'] = df.groupby('serial_number')[smart].transform(
                lambda x: x.rolling(window=7, min_periods=3).apply(calc_slope, raw=True)
            ).fillna(0)
        
        logger.info(f"趋势特征完成")
        
        return df
    
    def get_all_features(self, df: pd.DataFrame) -> List[str]:
        """获取所有特征列名"""
        base_features = self.smart_cols.copy()
        
        window_features = []
        for smart in self.smart_cols:
            for window in self.windows:
                window_features.extend([
                    f'{smart}_mean_{window}d',
                    f'{smart}_max_{window}d',
                    f'{smart}_min_{window}d',
                    f'{smart}_std_{window}d',
                    f'{smart}_delta_{window}d',
                    f'{smart}_spike_{window}d'
                ])
        
        lag_features = []
        for smart in self.smart_cols:
            for lag in self.lags:
                lag_features.append(f'{smart}_lag_{lag}d')
        
        trend_features = [f'{smart}_trend_7d' for smart in self.smart_cols]
        
        return base_features + window_features + lag_features + trend_features
    
    def process(self, input_path: str = 'data/raw/smart_data.csv',
                output_path: str = 'data/features/smart_features.csv') -> pd.DataFrame:
        """
        完整特征工程流程
        """
        logger.info("开始特征工程")
        
        # 加载数据
        df = pd.read_csv(input_path)
        logger.info(f"加载数据：{len(df)} 条记录")
        
        # 特征工程
        df_features = self.create_time_window_features(df)
        df_features = self.create_lag_features(df_features)
        df_features = self.create_trend_features(df_features)
        
        # 保存
        Path(output_path).parent.mkdir(parents=True, exist_ok=True)
        df_features.to_csv(output_path, index=False, encoding='utf-8-sig')
        
        # 保存特征配置
        feature_cols = self.get_all_features(df_features)
        import joblib
        joblib.dump(feature_cols, 'models/feature_config.pkl')
        
        logger.info(f"特征工程完成：{len(df.columns)} 列 → {len(df_features.columns)} 列")
        logger.info(f"特征文件已保存到 {output_path}")
        
        return df_features


def main():
    """主函数"""
    logger.info("=" * 50)
    logger.info("开始特征工程")
    logger.info("=" * 50)
    
    fe = FeatureEngineer()
    df_features = fe.process()
    
    logger.info("=" * 50)
    logger.info(f"特征矩阵：{df_features.shape}")
    logger.info("=" * 50)


if __name__ == '__main__':
    main()
