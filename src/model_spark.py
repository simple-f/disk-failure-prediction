#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
PySpark 模型训练 - LightGBM on Spark
"""

from pyspark.sql import SparkSession
from pyspark.ml import Pipeline
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder
import logging
from pathlib import Path
import yaml

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class SparkModelTrainer:
    """PySpark 模型训练器"""
    
    def __init__(self, config_path='config/config.yaml'):
        """初始化"""
        with open(config_path, 'r', encoding='utf-8') as f:
            self.config = yaml.safe_load(f)
        
        self.spark = SparkSession.builder \
            .appName("DiskFailurePrediction-Training") \
            .enableHiveSupport() \
            .getOrCreate()
        
        self.model_config = self.config['model']
        self.obs_config = self.config['obs']
    
    def load_features(self, path: str = None):
        """加载特征数据"""
        data_path = path or self.obs_config['paths']['features']
        logger.info(f"从 OBS 加载特征数据：{data_path}")
        
        df = self.spark.read.parquet(data_path)
        logger.info(f"加载完成：{df.count()} 条记录")
        
        return df
    
    def train(self, df, feature_cols: list, target_col: str = 'failure'):
        """训练模型"""
        logger.info("开始训练模型")
        
        # 准备数据
        assembler = VectorAssembler(
            inputCols=feature_cols,
            outputCol="features"
        )
        
        # LightGBM 分类器（用 RandomForest 替代，LightGBM on Spark 需要额外安装）
        classifier = RandomForestClassifier(
            labelCol=target_col,
            featuresCol="features",
            numTrees=200,
            maxDepth=10,
            maxBins=128,
            minInstancesPerNode=20,
            featureSubsetStrategy="sqrt",
            seed=42
        )
        
        # Pipeline
        pipeline = Pipeline(stages=[assembler, classifier])
        
        # 参数网格
        param_grid = ParamGridBuilder() \
            .addGrid(classifier.numTrees, [100, 200, 300]) \
            .addGrid(classifier.maxDepth, [10, 15, 20]) \
            .build()
        
        # 交叉验证
        cv = CrossValidator(
            estimator=pipeline,
            estimatorParamMaps=param_grid,
            evaluator=BinaryClassificationEvaluator(
                labelCol=target_col,
                rawPredictionCol="prediction",
                metricName="areaUnderROC"
            ),
            numFolds=5,
            seed=42,
            parallelism=4
        )
        
        # 训练
        logger.info("开始交叉验证训练...")
        model = cv.fit(df)
        
        logger.info(f"训练完成，最佳 AUC: {model.avgMetrics[0]:.4f}")
        
        return model
    
    def save_model(self, model, path: str = 'models/spark_model'):
        """保存模型"""
        Path(path).parent.mkdir(parents=True, exist_ok=True)
        model.write().overwrite().save(path)
        logger.info(f"模型已保存到 {path}")
    
    def evaluate(self, model, test_data, target_col: str = 'failure'):
        """评估模型"""
        predictions = model.transform(test_data)
        
        evaluator = BinaryClassificationEvaluator(
            labelCol=target_col,
            rawPredictionCol="prediction",
            metricName="areaUnderROC"
        )
        
        auc = evaluator.evaluate(predictions)
        logger.info(f"测试集 AUC: {auc:.4f}")
        
        return auc


def main():
    """主函数"""
    logger.info("=" * 60)
    logger.info("开始 PySpark 模型训练")
    logger.info("=" * 60)
    
    trainer = SparkModelTrainer()
    
    # 加载数据
    df = trainer.load_features()
    
    # 特征列（排除非特征列）
    exclude_cols = ['failure', 'serial_number', 'timestamp', 'year', 'month']
    feature_cols = [col for col in df.columns if col not in exclude_cols]
    
    logger.info(f"特征数量：{len(feature_cols)}")
    
    # 训练
    model = trainer.train(df, feature_cols)
    
    # 保存
    trainer.save_model(model)
    
    logger.info("=" * 60)
    logger.info("模型训练完成")
    logger.info("=" * 60)
    
    trainer.spark.stop()


if __name__ == '__main__':
    main()
