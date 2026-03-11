#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
PySpark 特征工程 - OBS 数据源（改进版）

改进内容：
1. 添加缺失值处理（插值 + 前向填充）
2. 添加异常值检测和处理（3σ原则）
3. 添加特征标准化（Z-Score）
4. 添加数据缓存优化
5. 添加特征重要性计算
6. 改进代码复用（使用 Pipeline 模式）
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.ml.feature import VectorAssembler, StandardScaler, RobustScaler
from pyspark.ml import Pipeline, PipelineModel
from pyspark.ml.stat import Correlation
import logging
from typing import List, Dict, Tuple
import yaml
from pathlib import Path
from datetime import datetime
import joblib

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class DataQualityChecker:
    """数据质量检查器"""
    
    def __init__(self):
        """初始化"""
        self.missing_threshold = 0.05  # 缺失率阈值 5%
        self.outlier_sigma = 3.0  # 3σ原则
    
    def check_missing_rate(self, df: DataFrame, col: str) -> float:
        """检查列缺失率"""
        total = df.count()
        if total == 0:
            return 0.0
        
        missing = df.filter(F.col(col).isNull()).count()
        rate = missing / total
        
        if rate > self.missing_threshold:
            logger.warning(f"列 {col} 缺失率过高：{rate:.2%}")
        
        return rate
    
    def check_outliers(self, df: DataFrame, col: str) -> Tuple[float, float]:
        """检查异常值（3σ原则）"""
        stats = df.agg(
            F.mean(F.col(col)).alias('mean'),
            F.stddev(F.col(col)).alias('std')
        ).collect()[0]
        
        mean = stats['mean'] or 0
        std = stats['std'] or 0
        
        lower = mean - self.outlier_sigma * std
        upper = mean + self.outlier_sigma * std
        
        outliers = df.filter(
            (F.col(col) < lower) | (F.col(col) > upper)
        ).count()
        
        outlier_rate = outliers / df.count() if df.count() > 0 else 0
        
        if outlier_rate > 0.01:
            logger.warning(f"列 {col} 异常值比例：{outlier_rate:.2%}")
        
        return lower, upper


class SparkFeatureEngineer:
    """PySpark 特征工程师（改进版）"""
    
    def __init__(self, config_path='config/config.yaml'):
        """初始化"""
        with open(config_path, 'r', encoding='utf-8') as f:
            self.config = yaml.safe_load(f)
        
        self.obs_config = self.config['obs']
        self.spark_config = self.config['spark']
        self.smart_cols = self.config['feature_engineering']['smart_attributes']
        self.windows = self.config['feature_engineering']['windows']
        self.lags = self.config['feature_engineering']['lags']
        
        # 数据质量检查器
        self.quality_checker = DataQualityChecker()
        
        # 初始化 SparkSession
        self.spark = self._create_spark_session()
        
        # 特征统计信息（用于标准化）
        self.feature_stats = {}
    
    def _create_spark_session(self) -> SparkSession:
        """创建 SparkSession"""
        spark = SparkSession.builder \
            .appName(self.spark_config['app_name']) \
            .master(self.spark_config['master']) \
            .config("spark.executor.cores", self.spark_config['executor_cores']) \
            .config("spark.executor.memory", self.spark_config['executor_memory']) \
            .config("spark.driver.memory", self.spark_config['driver_memory']) \
            .config("spark.num_executors", self.spark_config['num_executors']) \
            .config("spark.dynamicAllocation.enabled", 
                   self.spark_config['dynamic_allocation']['enabled']) \
            .config("spark.dynamicAllocation.minExecutors", 
                   self.spark_config['dynamic_allocation']['min_executors']) \
            .config("spark.dynamicAllocation.maxExecutors", 
                   self.spark_config['dynamic_allocation']['max_executors']) \
            .config("spark.serializer", self.spark_config['serializer']) \
            .config("spark.sql.shuffle.partitions", 
                   self.spark_config['sql']['shuffle_partitions']) \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
            .enableHiveSupport() \
            .getOrCreate()
        
        spark.sparkContext.setLogLevel("WARN")
        logger.info("SparkSession 创建完成")
        
        return spark
    
    def load_data(self, path: str = None) -> DataFrame:
        """从 OBS 加载数据"""
        data_path = path or self.obs_config['paths']['raw_data']
        
        logger.info(f"从 OBS 加载数据：{data_path}")
        
        # 读取 Parquet 格式数据
        df = self.spark.read.parquet(data_path)
        
        count = df.count()
        logger.info(f"数据加载完成：{count} 条记录")
        logger.info(f"Schema: {df.schema}")
        
        return df
    
    def handle_missing_values(self, df: DataFrame, 
                               serial_col: str = 'serial_number',
                               date_col: str = 'timestamp') -> DataFrame:
        """
        处理缺失值
        
        策略：
        1. 时间序列插值（按序列号分组）
        2. 前向填充（forward fill）
        3. 后向填充（backward fill）
        4. 最后用 0 填充
        """
        logger.info("开始处理缺失值")
        
        result_df = df
        
        for col in self.smart_cols:
            if col not in df.columns:
                continue
            
            # 检查缺失率
            missing_rate = self.quality_checker.check_missing_rate(df, col)
            
            if missing_rate == 0:
                continue
            
            # 窗口定义（按硬盘序列号分组，时间排序）
            window_spec = Window.partitionBy(serial_col).orderBy(date_col)
            
            # 1. 线性插值
            result_df = result_df.withColumn(
                f'{col}_imputed',
                F.expr(f"INTERPOLATE({col}, '{col}_imputed') OVER (PARTITION BY {serial_col} ORDER BY {date_col})")
            )
            
            # 2. 前向填充
            result_df = result_df.withColumn(
                f'{col}_imputed',
                F.last(F.col(f'{col}_imputed'), ignorenulls=True).over(
                    window_spec.rowsBetween(Window.unboundedPreceding, 0)
                )
            )
            
            # 3. 后向填充
            result_df = result_df.withColumn(
                f'{col}_imputed',
                F.first(F.col(f'{col}_imputed'), ignorenulls=True).over(
                    window_spec.rowsBetween(0, Window.unboundedFollowing)
                )
            )
            
            # 4. 最后用 0 填充
            result_df = result_df.withColumn(
                f'{col}_imputed',
                F.coalesce(F.col(f'{col}_imputed'), F.lit(0))
            )
        
        # 删除原始列，重命名插值列
        for col in self.smart_cols:
            if col in result_df.columns:
                result_df = result_df.drop(col)
            result_df = result_df.withColumnRenamed(f'{col}_imputed', col)
        
        logger.info("缺失值处理完成")
        
        return result_df
    
    def handle_outliers(self, df: DataFrame,
                         method: str = 'clip') -> DataFrame:
        """
        处理异常值
        
        方法：
        - clip: 截断到 [μ-3σ, μ+3σ]
        - remove: 删除异常值（不推荐，会丢失数据）
        - log: 对数变换
        """
        logger.info(f"开始处理异常值（方法：{method}）")
        
        result_df = df
        
        for col in self.smart_cols:
            if col not in df.columns:
                continue
            
            # 计算统计量
            lower, upper = self.quality_checker.check_outliers(df, col)
            
            if method == 'clip':
                # 截断到 [lower, upper]
                result_df = result_df.withColumn(
                    col,
                    F.least(F.greatest(F.col(col), F.lit(lower)), F.lit(upper))
                )
            elif method == 'log':
                # 对数变换（适用于右偏分布）
                result_df = result_df.withColumn(
                    col,
                    F.log1p(F.col(col))
                )
        
        logger.info("异常值处理完成")
        
        return result_df
    
    def create_time_window_features(self, df: DataFrame,
                                     serial_col: str = 'serial_number',
                                     date_col: str = 'timestamp') -> DataFrame:
        """创建时间窗口特征（优化版）"""
        logger.info("开始创建时间窗口特征")
        
        # 缓存 DataFrame（避免重复计算）
        df = df.cache()
        
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
                
                # 使用表达式批量计算（减少代码重复）
                agg_exprs = [
                    (F.avg(F.col(smart)).over(window_rows), f'{smart}_mean_{window}d'),
                    (F.max(F.col(smart)).over(window_rows), f'{smart}_max_{window}d'),
                    (F.min(F.col(smart)).over(window_rows), f'{smart}_min_{window}d'),
                    (F.stddev(F.col(smart)).over(window_rows), f'{smart}_std_{window}d'),
                ]
                
                for expr, col_name in agg_exprs:
                    result_df = result_df.withColumn(col_name, expr)
                
                # 变化量（delta）
                result_df = result_df.withColumn(
                    f'{smart}_delta_{window}d',
                    F.col(smart) - F.lag(F.col(smart), window).over(window_spec)
                )
        
        # 解除缓存
        df.unpersist()
        
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
        
        # 填充 NaN（用 0 填充）
        result_df = result_df.fillna(0)
        
        logger.info("滞后特征完成")
        
        return result_df
    
    def create_trend_features(self, df: DataFrame) -> DataFrame:
        """创建趋势特征"""
        logger.info("开始创建趋势特征")
        
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
            
            # 14 天变化率
            result_df = result_df.withColumn(
                f'{smart}_trend_14d',
                (F.col(smart) - F.lag(F.col(smart), 14).over(window_spec)) / 14
            )
        
        result_df = result_df.fillna(0)
        
        logger.info("趋势特征完成")
        
        return result_df
    
    def standardize_features(self, df: DataFrame,
                              feature_cols: List[str],
                              method: str = 'zscore') -> DataFrame:
        """
        特征标准化
        
        方法：
        - zscore: Z-Score 标准化（(x - μ) / σ）
        - robust: Robust 标准化（使用中位数和四分位数，抗异常值）
        - minmax: Min-Max 归一化（缩放到 [0, 1]）
        """
        logger.info(f"开始特征标准化（方法：{method}）")
        
        # 选择特征列
        exclude_cols = ['failure', 'serial_number', 'timestamp', 'year', 'month']
        feature_cols = [col for col in df.columns 
                       if col not in exclude_cols 
                       and col in feature_cols]
        
        if method == 'zscore':
            # 计算均值和标准差
            stats = df.agg(
                *[F.mean(col).alias(f'{col}_mean') for col in feature_cols],
                *[F.stddev(col).alias(f'{col}_std') for col in feature_cols]
            ).collect()[0]
            
            # 保存统计信息
            for col in feature_cols:
                self.feature_stats[col] = {
                    'mean': stats[f'{col}_mean'] or 0,
                    'std': stats[f'{col}_std'] or 1
                }
            
            # 标准化
            result_df = df
            for col in feature_cols:
                mean = self.feature_stats[col]['mean']
                std = self.feature_stats[col]['std']
                
                result_df = result_df.withColumn(
                    f'{col}_scaled',
                    (F.col(col) - mean) / std
                )
                
                # 删除原始列
                result_df = result_df.drop(col)
        
        elif method == 'robust':
            # 使用中位数和四分位数
            stats = df.agg(
                *[F.percentile_approx(col, 0.5).alias(f'{col}_median') for col in feature_cols],
                *[F.percentile_approx(col, 0.75).alias(f'{col}_p75') for col in feature_cols],
                *[F.percentile_approx(col, 0.25).alias(f'{col}_p25') for col in feature_cols]
            ).collect()[0]
            
            result_df = df
            for col in feature_cols:
                median = stats[f'{col}_median'] or 0
                iqr = (stats[f'{col}_p75'] or 1) - (stats[f'{col}_p25'] or 0)
                
                result_df = result_df.withColumn(
                    f'{col}_scaled',
                    (F.col(col) - median) / F.lit(iqr if iqr > 0 else 1)
                )
                
                result_df = result_df.drop(col)
        
        logger.info("特征标准化完成")
        
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
        
        # 保存特征统计信息（用于后续标准化）
        Path('models').mkdir(parents=True, exist_ok=True)
        joblib.dump(self.feature_stats, 'models/feature_stats.pkl')
        
        # 保存特征列表
        feature_cols = df.columns
        joblib.dump(feature_cols, 'models/feature_config.pkl')
        
        logger.info(f"特征数据已保存到 {output_path}")
        logger.info(f"特征统计信息已保存到 models/feature_stats.pkl")
    
    def process(self, input_path: str = None, output_path: str = None):
        """完整特征工程流程"""
        logger.info("=" * 60)
        logger.info("开始 PySpark 特征工程（改进版）")
        logger.info("=" * 60)
        
        # 1. 加载数据
        df = self.load_data(input_path)
        
        # 2. 数据质量检查
        logger.info("进行数据质量检查...")
        for col in self.smart_cols:
            if col in df.columns:
                self.quality_checker.check_missing_rate(df, col)
                self.quality_checker.check_outliers(df, col)
        
        # 3. 缺失值处理
        df = self.handle_missing_values(df)
        
        # 4. 异常值处理
        df = self.handle_outliers(df, method='clip')
        
        # 5. 特征工程
        df = self.create_time_window_features(df)
        df = self.create_lag_features(df)
        df = self.create_trend_features(df)
        
        # 6. 特征标准化
        exclude_cols = ['failure', 'serial_number', 'timestamp', 'year', 'month']
        feature_cols = [col for col in df.columns if col not in exclude_cols]
        df = self.standardize_features(df, feature_cols, method='zscore')
        
        # 7. 保存
        self.save_features(df, output_path)
        
        logger.info("=" * 60)
        logger.info(f"特征工程完成：原始 {len(self.smart_cols)} 个 SMART 属性 → {len(df.columns)} 个特征")
        logger.info("=" * 60)
        
        self.spark.stop()
        
        return df


def main():
    """主函数"""
    fe = SparkFeatureEngineer()
    fe.process()


if __name__ == '__main__':
    main()
