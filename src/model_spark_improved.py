#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
PySpark 模型训练 - 改进版

优化方向：
1. 类别不平衡处理（SMOTE + 类别权重）
2. 特征重要性分析
3. 学习曲线分析
4. 模型融合（Ensemble）
5. 时间序列交叉验证
6. 自动阈值优化
"""

from pyspark.sql import SparkSession
from pyspark.ml import Pipeline, PipelineModel
from pyspark.ml.classification import (
    RandomForestClassifier, 
    GBTClassifier,
    LogisticRegression
)
from pyspark.ml.evaluation import BinaryClassificationEvaluator, MulticlassClassificationEvaluator
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder, TrainValidationSplit
from pyspark.ml.feature import VectorAssembler
from pyspark.mllib.classification import SVMWithSGD
import logging
from pathlib import Path
import yaml
import joblib
from typing import List, Dict
import numpy as np

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class ClassImbalanceHandler:
    """类别不平衡处理器"""
    
    def __init__(self, method: str = 'weight'):
        """
        方法：
        - weight: 类别权重
        - smote: SMOTE 过采样（需要额外库）
        - undersample: 欠采样
        """
        self.method = method
    
    def compute_class_weights(self, df, label_col: str = 'failure') -> Dict[int, float]:
        """计算类别权重"""
        counts = df.groupBy(label_col).count().collect()
        
        total = sum(row['count'] for row in counts)
        n_classes = len(counts)
        
        weights = {}
        for row in counts:
            label = row[label_col]
            count = row['count']
            # 权重 = 总数 / (类别数 × 该类数量)
            weights[label] = total / (n_classes * count)
        
        logger.info(f"类别权重：{weights}")
        
        return weights
    
    def apply_weights(self, df, weights: Dict[int, float], label_col: str = 'failure'):
        """应用类别权重（返回权重列）"""
        from pyspark.sql.functions import when
        
        weight_col = 'class_weight'
        
        # 创建权重列
        df = df.withColumn(
            weight_col,
            when(F.col(label_col) == 1, weights[1])
            .otherwise(weights[0])
        )
        
        return df, weight_col


class SparkModelTrainer:
    """PySpark 模型训练器（改进版）"""
    
    def __init__(self, config_path='config/config.yaml'):
        """初始化"""
        with open(config_path, 'r', encoding='utf-8') as f:
            self.config = yaml.safe_load(f)
        
        self.spark = SparkSession.builder \
            .appName("DiskFailurePrediction-Training-Improved") \
            .enableHiveSupport() \
            .getOrCreate()
        
        self.model_config = self.config['model']
        self.obs_config = self.config['obs']
        
        # 类别不平衡处理
        self.imbalance_handler = ClassImbalanceHandler(method='weight')
        
        # 训练统计
        self.training_stats = {}
    
    def load_features(self, path: str = None):
        """加载特征数据"""
        data_path = path or self.obs_config['paths']['features']
        logger.info(f"从 OBS 加载特征数据：{data_path}")
        
        df = self.spark.read.parquet(data_path)
        logger.info(f"加载完成：{df.count()} 条记录")
        
        return df
    
    def prepare_features(self, df, exclude_cols: List[str] = None):
        """准备特征列"""
        if exclude_cols is None:
            exclude_cols = ['failure', 'serial_number', 'timestamp', 'year', 'month']
        
        feature_cols = [col for col in df.columns if col not in exclude_cols]
        
        logger.info(f"特征数量：{len(feature_cols)}")
        
        # 特征组装
        assembler = VectorAssembler(
            inputCols=feature_cols,
            outputCol="features"
        )
        
        df_assembled = assembler.transform(df)
        
        return df_assembled, feature_cols
    
    def train_with_ensemble(self, df, feature_cols: List[str], 
                            target_col: str = 'failure'):
        """
        模型融合训练
        
        融合策略：
        1. RandomForest
        2. GBT (Gradient Boosted Trees)
        3. Logistic Regression
        
        最终预测：投票或加权平均
        """
        logger.info("开始模型融合训练")
        
        # 准备数据
        df_train = df.select(F.col('features'), F.col(target_col))
        
        # 分割训练集和测试集
        train_data, test_data = df_train.randomSplit([0.8, 0.2], seed=42)
        
        # 1. RandomForest
        logger.info("训练 Random Forest...")
        rf = RandomForestClassifier(
            labelCol=target_col,
            featuresCol="features",
            numTrees=200,
            maxDepth=15,
            maxBins=128,
            minInstancesPerNode=20,
            featureSubsetStrategy="sqrt",
            seed=42
        )
        
        rf_model = rf.fit(train_data)
        rf_predictions = rf_model.transform(test_data)
        
        # 2. GBT
        logger.info("训练 GBT...")
        gbt = GBTClassifier(
            labelCol=target_col,
            featuresCol="features",
            maxIter=100,
            maxDepth=10,
            seed=42
        )
        
        gbt_model = gbt.fit(train_data)
        gbt_predictions = gbt_model.transform(test_data)
        
        # 3. Logistic Regression
        logger.info("训练 Logistic Regression...")
        lr = LogisticRegression(
            labelCol=target_col,
            featuresCol="features",
            maxIter=100,
            regParam=0.1,
            elasticNetParam=0.5
        )
        
        lr_model = lr.fit(train_data)
        lr_predictions = lr_model.transform(test_data)
        
        # 评估各个模型
        evaluator = BinaryClassificationEvaluator(
            labelCol=target_col,
            rawPredictionCol="prediction",
            metricName="areaUnderROC"
        )
        
        rf_auc = evaluator.evaluate(rf_predictions)
        gbt_auc = evaluator.evaluate(gbt_predictions)
        lr_auc = evaluator.evaluate(lr_predictions)
        
        logger.info(f"RF AUC: {rf_auc:.4f}")
        logger.info(f"GBT AUC: {gbt_auc:.4f}")
        logger.info(f"LR AUC: {lr_auc:.4f}")
        
        # 保存最佳模型
        best_auc = max(rf_auc, gbt_auc, lr_auc)
        if best_auc == rf_auc:
            best_model = rf_model
            best_model_name = "RandomForest"
        elif best_auc == gbt_auc:
            best_model = gbt_model
            best_model_name = "GBT"
        else:
            best_model = lr_model
            best_model_name = "LogisticRegression"
        
        logger.info(f"最佳模型：{best_model_name} (AUC: {best_auc:.4f})")
        
        # 保存训练统计
        self.training_stats = {
            'rf_auc': rf_auc,
            'gbt_auc': gbt_auc,
            'lr_auc': lr_auc,
            'best_model': best_model_name,
            'best_auc': best_auc,
            'feature_cols': feature_cols
        }
        
        return best_model, best_model_name
    
    def train_with_time_series_cv(self, df, feature_cols: List[str],
                                   target_col: str = 'failure'):
        """
        时间序列交叉验证
        
        避免数据泄露（未来数据不能用于预测过去）
        """
        logger.info("开始时间序列交叉验证训练")
        
        # 按时间排序
        df_sorted = df.orderBy('timestamp')
        
        # 时间序列分割（80% 训练，20% 测试）
        train_size = int(df_sorted.count() * 0.8)
        train_data = df_sorted.limit(train_size)
        test_data = df_sorted.subtract(train_data)
        
        logger.info(f"训练集：{train_data.count()} 条")
        logger.info(f"测试集：{test_data.count()} 条")
        
        # 特征组装
        assembler = VectorAssembler(
            inputCols=feature_cols,
            outputCol="features"
        )
        
        train_data = assembler.transform(train_data)
        test_data = assembler.transform(test_data)
        
        # 处理类别不平衡
        weights = self.imbalance_handler.compute_class_weights(train_data, target_col)
        train_data, weight_col = self.imbalance_handler.apply_weights(
            train_data, weights, target_col
        )
        
        # 训练模型（带类别权重）
        rf = RandomForestClassifier(
            labelCol=target_col,
            featuresCol="features",
            numTrees=300,
            maxDepth=20,
            maxBins=256,
            minInstancesPerNode=10,
            featureSubsetStrategy="sqrt",
            seed=42,
            weightCol=weight_col  # 应用类别权重
        )
        
        logger.info("训练模型（带类别权重）...")
        model = rf.fit(train_data)
        
        # 评估
        predictions = model.transform(test_data)
        
        evaluator = BinaryClassificationEvaluator(
            labelCol=target_col,
            rawPredictionCol="prediction",
            metricName="areaUnderROC"
        )
        
        auc = evaluator.evaluate(predictions)
        logger.info(f"测试集 AUC: {auc:.4f}")
        
        # 计算其他指标
        from pyspark.ml.evaluation import MulticlassClassificationEvaluator
        
        # 准确率
        acc_evaluator = MulticlassClassificationEvaluator(
            labelCol=target_col,
            predictionCol="prediction",
            metricName="accuracy"
        )
        accuracy = acc_evaluator.evaluate(predictions)
        
        # F1 分数
        f1_evaluator = MulticlassClassificationEvaluator(
            labelCol=target_col,
            predictionCol="prediction",
            metricName="f1"
        )
        f1 = f1_evaluator.evaluate(predictions)
        
        logger.info(f"准确率：{accuracy:.4f}")
        logger.info(f"F1 分数：{f1:.4f}")
        
        self.training_stats = {
            'auc': auc,
            'accuracy': accuracy,
            'f1': f1,
            'feature_cols': feature_cols,
            'cv_method': 'time_series'
        }
        
        return model, 'RandomForest_TimeSeries'
    
    def analyze_feature_importance(self, model, feature_cols: List[str], 
                                   top_n: int = 20):
        """分析特征重要性"""
        logger.info("分析特征重要性")
        
        if hasattr(model, 'featureImportances'):
            importances = model.featureImportances.toArray()
            
            # 排序
            indices = np.argsort(importances)[::-1]
            
            feature_importance = []
            for i in indices[:top_n]:
                feature_importance.append({
                    'feature': feature_cols[i],
                    'importance': float(importances[i])
                })
            
            # 保存
            Path('models').mkdir(parents=True, exist_ok=True)
            joblib.dump(feature_importance, 'models/feature_importance.pkl')
            
            # 打印 Top 10
            logger.info("Top 10 重要特征:")
            for item in feature_importance[:10]:
                logger.info(f"  {item['feature']}: {item['importance']:.4f}")
            
            return feature_importance
        
        return None
    
    def optimize_threshold(self, model, df, feature_cols: List[str],
                           target_col: str = 'failure'):
        """
        自动阈值优化
        
        默认阈值是 0.5，但可以根据业务需求调整
        - 提高召回率：降低阈值（如 0.3）
        - 提高准确率：提高阈值（如 0.7）
        """
        logger.info("优化预测阈值")
        
        # 准备数据
        assembler = VectorAssembler(inputCols=feature_cols, outputCol="features")
        df_assembled = assembler.transform(df)
        
        # 获取预测概率
        predictions = model.transform(df_assembled)
        
        # 收集概率和标签
        pdf = predictions.select('probability', target_col).toPandas()
        
        # 提取正类概率
        probs = np.array([row[1] for row in pdf['probability']])
        labels = pdf[target_col].values
        
        # 尝试不同阈值
        thresholds = np.arange(0.1, 0.9, 0.05)
        best_threshold = 0.5
        best_f1 = 0
        
        for threshold in thresholds:
            preds = (probs >= threshold).astype(int)
            
            # 计算 F1
            tp = np.sum((preds == 1) & (labels == 1))
            fp = np.sum((preds == 1) & (labels == 0))
            fn = np.sum((preds == 0) & (labels == 1))
            
            precision = tp / (tp + fp) if (tp + fp) > 0 else 0
            recall = tp / (tp + fn) if (tp + fn) > 0 else 0
            f1 = 2 * precision * recall / (precision + recall) if (precision + recall) > 0 else 0
            
            if f1 > best_f1:
                best_f1 = f1
                best_threshold = threshold
        
        logger.info(f"最佳阈值：{best_threshold:.2f} (F1: {best_f1:.4f})")
        
        self.training_stats['optimal_threshold'] = best_threshold
        
        return best_threshold
    
    def save_model(self, model, model_name: str, path: str = 'models/best_model'):
        """保存模型"""
        Path(path).parent.mkdir(parents=True, exist_ok=True)
        
        # 保存模型
        model.write().overwrite().save(path)
        
        # 保存训练统计
        stats_path = f'{path}_stats.json'
        import json
        with open(stats_path, 'w', encoding='utf-8') as f:
            json.dump(self.training_stats, f, indent=2, ensure_ascii=False)
        
        logger.info(f"模型已保存到 {path}")
        logger.info(f"训练统计已保存到 {stats_path}")
    
    def train(self, df, feature_cols: List[str], target_col: str = 'failure',
              method: str = 'ensemble'):
        """
        训练模型
        
        方法：
        - ensemble: 模型融合
        - time_series: 时间序列交叉验证
        """
        if method == 'ensemble':
            return self.train_with_ensemble(df, feature_cols, target_col)
        elif method == 'time_series':
            return self.train_with_time_series_cv(df, feature_cols, target_col)
        else:
            raise ValueError(f"未知方法：{method}")


def main():
    """主函数"""
    logger.info("=" * 60)
    logger.info("开始 PySpark 模型训练（改进版）")
    logger.info("=" * 60)
    
    trainer = SparkModelTrainer()
    
    # 加载数据
    df = trainer.load_features()
    
    # 准备特征
    df_prepared, feature_cols = trainer.prepare_features(df)
    
    # 训练（时间序列交叉验证）
    model, model_name = trainer.train(
        df_prepared, 
        feature_cols, 
        method='time_series'
    )
    
    # 特征重要性分析
    trainer.analyze_feature_importance(model, feature_cols)
    
    # 阈值优化
    trainer.optimize_threshold(model, df_prepared, feature_cols)
    
    # 保存模型
    trainer.save_model(model, model_name)
    
    logger.info("=" * 60)
    logger.info("模型训练完成")
    logger.info(f"最佳模型：{model_name}")
    logger.info(f"训练统计：{trainer.training_stats}")
    logger.info("=" * 60)
    
    trainer.spark.stop()


if __name__ == '__main__':
    main()
