#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
PySpark 特征工程 - OBS 数据源
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.ml.feature import VectorAssembler
from pyspark.ml import Pipeline
import logging
from typing import List
import yaml
from pathlib import Path

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class SparkFeatureEngineer:
    """PySpark 特征工程师"""
    
    def __init__(self, config_path='config/config.yaml'):
        """初始化"""
        with open(config_path, 'r', encoding='utf-8') as f:
            self.config = yaml.safe_load(f)
        
        self.obs_config = self.config['obs']
        self.spark_config = self.config['spark']
        self.smart_cols = self.config['feature_engineering']['smart_attributes']
        self.windows = self.config['feature_engineering']['windows']
        self.lags = self.config['feature_engineering']['lags']
        
        # 初始化 SparkSession
        self.spark = self._create_spark_session()
    
    def _create_spark_session(self) -> SparkSession:
        """创建 SparkSession"""
        spark = SparkSession.builder \
            .appName(self.spark_config['app_name']) \
            .master(self.spark_config['master']) \
            .config("spark.executor.cores", self.spark_config['executor_cores']) \
            .config("spark.executor.memory", self.spark_config['executor_memory']) \
            .config("spark.driver.memory", self.spark_config['driver_memory']) \
            .config("spark.num.executors", self.spark_config['num_executors']) \
            .config("spark.dynamicAllocation.enabled", 
                   self.spark_config['dynamic_allocation']['enabled']) \
            .config("spark.dynamicAllocation.minExecutors", 
                   self.spark_config['dynamic_allocation']['min_executors']) \
            .config("spark.dynamicAllocation.maxExecutors", 
                   self.spark_config['dynamic_allocation']['max_executors']) \
            .config("spark.serializer", self.spark_config['serializer']) \
            .config("spark.sql.shuffle.partitions", 
                   self.spark_config['sql']['shuffle_partitions']) \
            .enableHiveSupport() \
            .getOrCreate()
        
        spark.sparkContext.setLogLevel("WARN")
        logger.info("SparkSession 创建完成")
        
        return spark
    
    def load_data(self, path: str = None) -> DataFrame:
        """
        从 OBS 加载数据
        
        参数:
            path: OBS 路径，如果为 None 则使用配置文件中的路径
        """
        data_path = path or self.obs_config['paths']['raw_data']
        
        logger.info(f"从 OBS 加载数据：{data_path}")
        
        # 读取 Parquet 格式数据
        df = self.spark.read.parquet(data_path)
        
        logger.info(f"数据加载完成：{df.count()} 条记录")
        logger.info(f"Schema: {df.schema}")
        
        return df
    
    def create_time_window_features(self, df: DataFrame,
                                     serial_col: str = 'serial_number',
                                     date_col: str = 'timestamp') -> DataFrame:
        """
        创建时间窗口特征
        
        使用 Spark Window 函数进行滚动计算
        """
        logger.info("开始创建时间窗口特征")
        
        # 按硬盘序列号和时间排序
        window_spec = Window.partitionBy(serial_col).orderBy(date_col)
        
        result_df = df
        
        for smart in self.smart_cols:
            if smart not in df.columns:
                logger.warning(f"列 {smart} 不存在，跳过")
                continue
            
            for window in self.windows:
                # 创建滚动窗口
                window_rows = Window.partitionBy(serial_col) \
                    .orderBy(date_col) \
                    .rowsBetween(-window + 1, 0)
                
                # 滚动平均值
                result_df = result_df.withColumn(
                    f'{smart}_mean_{window}d',
                    F.avg(F.col(smart)).over(window_rows)
                )
                
                # 滚动最大值
                result_df = result_df.withColumn(
                    f'{smart}_max_{window}d',
                    F.max(F.col(smart)).over(window_rows)
                )
                
                # 滚动最小值
                result_df = result_df.withColumn(
                    f'{smart}_min_{window}d',
                    F.min(F.col(smart)).over(window_rows)
                )
                
                # 滚动标准差
                result_df = result_df.withColumn(
                    f'{smart}_std_{window}d',
                    F.stddev(F.col(smart)).over(window_rows)
                )
                
                # 变化量（delta）
                result_df = result_df.withColumn(
                    f'{smart}_delta_{window}d',
                    F.col(smart) - F.lag(F.col(smart), window).over(window_spec)
                )
        
        logger.info(f"时间窗口特征完成：{len(df.columns)} 列 → {len(result_df.columns)} 列")
        
        return result_df
    
    def create_lag_features(self, df: DataFrame,
                             serial_col: str = 'serial_number') -> DataFrame:
        """创建滞后特征"""
        logger.info("开始创建滞后特征")
        
        window_spec = Window.partitionBy(serial_col).orderBy('timestamp')
        
        result_df = df
        
        for smart in self.smart_cols:
            if smart not in df.columns:
                continue
            
            for lag in self.lags:
                result_df = result_df.withColumn(
                    f'{smart}_lag_{lag}d',
                    F.lag(F.col(smart), lag).over(window_spec)
                )
        
        # 填充 NaN
        result_df = result_df.fillna(0)
        
        logger.info("滞后特征完成")
        
        return result_df
    
    def create_trend_features(self, df: DataFrame) -> DataFrame:
        """创建趋势特征（使用近似线性回归）"""
        logger.info("开始创建趋势特征")
        
        # 简化版本：计算 7 天变化率
        window_spec = Window.partitionBy('serial_number').orderBy('timestamp')
        
        result_df = df
        
        for smart in self.smart_cols:
            if smart not in df.columns:
                continue
            
            # 7 天变化率
            result_df = result_df.withColumn(
                f'{smart}_trend_7d',
                (F.col(smart) - F.lag(F.col(smart), 7).over(window_spec)) / 7
            )
        
        result_df = result_df.fillna(0)
        
        logger.info("趋势特征完成")
        
        return result_df
    
    def save_features(self, df: DataFrame, path: str = None):
        """保存特征数据到 OBS"""
        output_path = path or self.obs_config['paths']['features']
        
        logger.info(f"保存特征数据到 OBS: {output_path}")
        
        # 添加分区列
        df = df.withColumn('year', F.year(F.col('timestamp')))
        df = df.withColumn('month', F.month(F.col('timestamp')))
        
        # 保存到 OBS（Parquet 格式，分区存储）
        df.write \
            .mode('overwrite') \
            .partitionBy('year', 'month') \
            .parquet(output_path)
        
        logger.info(f"特征数据已保存到 {output_path}")
    
    def process(self, input_path: str = None, output_path: str = None):
        """完整特征工程流程"""
        logger.info("=" * 60)
        logger.info("开始 PySpark 特征工程")
        logger.info("=" * 60)
        
        # 加载数据
        df = self.load_data(input_path)
        
        # 特征工程
        df_features = self.create_time_window_features(df)
        df_features = self.create_lag_features(df_features)
        df_features = self.create_trend_features(df_features)
        
        # 保存
        self.save_features(df_features, output_path)
        
        # 保存特征列表
        feature_cols = df_features.columns
        import joblib
        Path('models').mkdir(parents=True, exist_ok=True)
        joblib.dump(feature_cols, 'models/feature_config.pkl')
        
        logger.info("=" * 60)
        logger.info(f"特征工程完成：{len(df.columns)} 列 → {len(df_features.columns)} 列")
        logger.info("=" * 60)
        
        self.spark.stop()
        
        return df_features


def main():
    """主函数"""
    fe = SparkFeatureEngineer()
    fe.process()


if __name__ == '__main__':
    main()
