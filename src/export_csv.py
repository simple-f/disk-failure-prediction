#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
数据导出工具 - CSV 格式

输出格式：一行一个硬盘（最新一条记录）
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window
import logging
from pathlib import Path
import yaml

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class DataExporter:
    """数据导出器（CSV 格式）"""
    
    def __init__(self, config_path='config/config.yaml'):
        """初始化"""
        with open(config_path, 'r', encoding='utf-8') as f:
            self.config = yaml.safe_load(f)
        
        self.spark = SparkSession.builder \
            .appName("DiskFailurePrediction-Export") \
            .enableHiveSupport() \
            .getOrCreate()
        
        self.obs_config = self.config['obs']
    
    def load_features(self, path: str = None) -> DataFrame:
        """加载特征数据"""
        data_path = path or self.obs_config['paths']['features']
        logger.info(f"从 OBS 加载特征数据：{data_path}")
        
        df = self.spark.read.parquet(data_path)
        logger.info(f"加载完成：{df.count()} 条记录")
        
        return df
    
    def export_per_disk(self, df: DataFrame, output_path: str = 'data/disk_features.csv'):
        """
        导出每个硬盘的最新一条记录（一行一个盘）
        
        策略：
        1. 按序列号分组
        2. 取每个组的最新时间戳记录
        3. 导出为 CSV
        """
        logger.info("开始导出每个硬盘的最新记录")
        
        # 窗口函数：按序列号分组，时间倒序
        window_spec = Window.partitionBy('serial_number').orderBy(F.col('timestamp').desc())
        
        # 标记每个序列号的最新记录
        df_latest = df.withColumn('row_num', F.row_number().over(window_spec)) \
                      .filter(F.col('row_num') == 1) \
                      .drop('row_num')
        
        # 选择需要的列（排除分区列和中间列）
        exclude_cols = ['year', 'month', 'row_num']
        export_cols = [col for col in df_latest.columns if col not in exclude_cols]
        
        df_export = df_latest.select(*export_cols)
        
        # 转换为 Pandas 并保存 CSV
        logger.info(f"导出 {df_export.count()} 个硬盘数据")
        
        # 收集到 Driver（数据量不大时可行）
        pdf = df_export.toPandas()
        
        # 保存 CSV
        Path(output_path).parent.mkdir(parents=True, exist_ok=True)
        pdf.to_csv(output_path, index=False, encoding='utf-8-sig')
        
        logger.info(f"数据已导出到 {output_path}")
        logger.info(f"CSV 格式：{len(pdf)} 行 × {len(pdf.columns)} 列")
        
        return pdf
    
    def export_with_label(self, df: DataFrame, output_path: str = 'data/labeled_disks.csv'):
        """
        导出带标签的数据（用于训练）
        
        标签定义：
        - 0: 健康盘（最后一条记录 failure=0）
        - 1: 故障盘（最后一条记录 failure=1）
        """
        logger.info("开始导出带标签的数据")
        
        # 取每个盘的最后一条记录
        window_spec = Window.partitionBy('serial_number').orderBy(F.col('timestamp').desc())
        
        df_latest = df.withColumn('row_num', F.row_number().over(window_spec)) \
                      .filter(F.col('row_num') == 1) \
                      .drop('row_num')
        
        # 按标签分组统计
        stats = df_latest.groupBy('failure').count().collect()
        for row in stats:
            label = '故障' if row['failure'] == 1 else '健康'
            logger.info(f"{label}盘：{row['count']} 个")
        
        # 导出
        pdf = df_latest.toPandas()
        Path(output_path).parent.mkdir(parents=True, exist_ok=True)
        pdf.to_csv(output_path, index=False, encoding='utf-8-sig')
        
        logger.info(f"带标签数据已导出到 {output_path}")
        
        return pdf
    
    def export_time_series(self, df: DataFrame, serial_number: str, 
                           output_path: str = 'data/disk_{serial}.csv'):
        """
        导出单个硬盘的完整时间序列
        
        参数:
            serial_number: 硬盘序列号
            output_path: 输出路径（支持格式化）
        """
        logger.info(f"导出硬盘 {serial_number} 的时间序列")
        
        df_disk = df.filter(F.col('serial_number') == serial_number) \
                    .orderBy('timestamp')
        
        logger.info(f"共 {df_disk.count()} 条记录")
        
        pdf = df_disk.toPandas()
        
        output_file = output_path.format(serial=serial_number)
        Path(output_file).parent.mkdir(parents=True, exist_ok=True)
        pdf.to_csv(output_file, index=False, encoding='utf-8-sig')
        
        logger.info(f"时间序列已导出到 {output_file}")
        
        return pdf


def main():
    """主函数"""
    logger.info("=" * 60)
    logger.info("开始数据导出")
    logger.info("=" * 60)
    
    exporter = DataExporter()
    
    # 加载数据
    df = exporter.load_features()
    
    # 导出每个盘的最新记录
    exporter.export_per_disk(df)
    
    # 导出带标签的数据
    exporter.export_with_label(df)
    
    logger.info("=" * 60)
    logger.info("数据导出完成")
    logger.info("=" * 60)
    
    exporter.spark.stop()


if __name__ == '__main__':
    main()
