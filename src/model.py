#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
模型训练模块 - LightGBM + 贝叶斯调优
"""

import lightgbm as lgb
import numpy as np
import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.metrics import roc_auc_score, f1_score, classification_report
from bayes_opt import BayesianOptimization
import joblib
import logging
from pathlib import Path
from typing import Dict, Tuple

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class DiskFailurePredictor:
    """硬盘故障预测器"""
    
    def __init__(self):
        """初始化预测器"""
        self.model = None
        self.best_params = None
    
    def prepare_data(self, df: pd.DataFrame,
                      feature_cols: list,
                      target_col: str = 'failure',
                      test_size: float = 0.2) -> Tuple:
        """准备训练数据"""
        X = df[feature_cols].fillna(0)
        y = df[target_col]
        
        X_train, X_test, y_train, y_test = train_test_split(
            X, y, 
            test_size=test_size, 
            stratify=y,
            random_state=42
        )
        
        logger.info(f"训练集：{len(X_train)} 样本，测试集：{len(X_test)} 样本")
        logger.info(f"正负样本比 - 训练：{y_train.value_counts().to_dict()}")
        
        return X_train, X_test, y_train, y_test
    
    def bayesian_optimization(self, X_train: pd.DataFrame,
                               y_train: pd.Series,
                               X_val: pd.DataFrame,
                               y_val: pd.Series,
                               n_iter: int = 50) -> Dict:
        """贝叶斯参数调优"""
        train_data = lgb.Dataset(X_train, label=y_train)
        val_data = lgb.Dataset(X_val, label=y_val, reference=train_data)
        
        # 参数空间
        param_bounds = {
            'num_leaves': (20, 100),
            'max_depth': (5, 15),
            'learning_rate': (0.01, 0.2),
            'min_child_samples': (10, 100),
            'subsample': (0.6, 1.0),
            'colsample_bytree': (0.6, 1.0),
            'reg_alpha': (0, 1),
            'reg_lambda': (0, 1)
        }
        
        # 目标函数
        def lgb_cv(**params):
            params['num_leaves'] = int(params['num_leaves'])
            params['max_depth'] = int(params['max_depth'])
            params['min_child_samples'] = int(params['min_child_samples'])
            
            model = lgb.train(
                params,
                train_data,
                valid_sets=[val_data],
                num_boost_round=1000,
                early_stopping_rounds=50,
                verbose_eval=False
            )
            
            y_pred = model.predict(X_val, num_iteration=model.best_iteration)
            score = roc_auc_score(y_val, y_pred)
            
            return score
        
        # 运行优化
        optimizer = BayesianOptimization(
            f=lgb_cv,
            pbounds=param_bounds,
            random_state=42,
            verbose=2
        )
        
        optimizer.maximize(init_points=10, n_iter=n_iter)
        
        # 提取最佳参数
        self.best_params = optimizer.max['params']
        self.best_params['num_leaves'] = int(self.best_params['num_leaves'])
        self.best_params['max_depth'] = int(self.best_params['max_depth'])
        self.best_params['min_child_samples'] = int(self.best_params['min_child_samples'])
        self.best_params['objective'] = 'binary'
        self.best_params['metric'] = 'auc'
        self.best_params['verbose'] = -1
        
        logger.info(f"最佳参数：{self.best_params}")
        logger.info(f"最佳 AUC: {optimizer.max['target']:.4f}")
        
        return self.best_params
    
    def train(self, X_train: pd.DataFrame,
               y_train: pd.Series,
               X_val: pd.DataFrame,
               y_val: pd.Series,
               params: Dict = None,
               num_boost_round: int = 1000) -> lgb.Booster:
        """训练模型"""
        if params is None:
            params = self.best_params or {
                'objective': 'binary',
                'metric': 'auc',
                'verbose': -1
            }
        
        train_data = lgb.Dataset(X_train, label=y_train)
        val_data = lgb.Dataset(X_val, label=y_val, reference=train_data)
        
        self.model = lgb.train(
            params,
            train_data,
            num_boost_round=num_boost_round,
            valid_sets=[val_data],
            early_stopping_rounds=50,
            verbose_eval=50
        )
        
        logger.info(f"训练完成，最佳迭代：{self.model.best_iteration}")
        
        return self.model
    
    def evaluate(self, X_test: pd.DataFrame,
                  y_test: pd.Series,
                  threshold: float = 0.5) -> Dict:
        """评估模型"""
        if self.model is None:
            raise ValueError("模型未训练")
        
        y_pred_proba = self.model.predict(X_test, num_iteration=self.model.best_iteration)
        y_pred = (y_pred_proba > threshold).astype(int)
        
        metrics = {
            'auc': roc_auc_score(y_test, y_pred_proba),
            'f1': f1_score(y_test, y_pred),
            'accuracy': (y_pred == y_test).mean(),
            'precision': classification_report(y_test, y_pred, output_dict=True)['1']['precision'],
            'recall': classification_report(y_test, y_pred, output_dict=True)['1']['recall']
        }
        
        logger.info("模型评估结果：")
        logger.info(f"  AUC: {metrics['auc']:.4f}")
        logger.info(f"  F1:  {metrics['f1']:.4f}")
        logger.info(f"  准确率：{metrics['accuracy']:.4f}")
        logger.info(f"  召回率：{metrics['recall']:.4f}")
        
        print(classification_report(y_test, y_pred, target_names=['健康', '故障']))
        
        return metrics
    
    def save(self, path: str = 'models/disk_failure_model.pkl'):
        """保存模型"""
        if self.model is None:
            raise ValueError("模型未训练")
        
        Path(path).parent.mkdir(parents=True, exist_ok=True)
        
        joblib.dump({
            'model': self.model,
            'best_params': self.best_params,
            'feature_importance': self.model.feature_importance(importance_type='gain')
        }, path)
        
        logger.info(f"模型已保存到 {path}")
    
    def load(self, path: str):
        """加载模型"""
        data = joblib.load(path)
        self.model = data['model']
        self.best_params = data['best_params']
        logger.info(f"模型已加载：{path}")
        
        return self


def main():
    """主函数"""
    logger.info("=" * 50)
    logger.info("开始模型训练")
    logger.info("=" * 50)
    
    # 加载特征数据
    df = pd.read_csv('data/features/smart_features.csv')
    
    # 特征列
    feature_cols = [col for col in df.columns if col not in ['failure', 'serial_number', 'timestamp']]
    
    # 如果还没有 failure 列，创建一个假的（实际应该有标注数据）
    if 'failure' not in df.columns:
        logger.warning("未找到 failure 列，创建假标签用于演示")
        df['failure'] = (df['smart_5_raw'] > 100).astype(int)
    
    # 初始化
    predictor = DiskFailurePredictor()
    
    # 准备数据
    X_train, X_test, y_train, y_test = predictor.prepare_data(df, feature_cols)
    
    # 分割验证集
    X_train, X_val, y_train, y_val = train_test_split(
        X_train, y_train, test_size=0.2, stratify=y_train, random_state=42
    )
    
    # 贝叶斯调优
    logger.info("开始贝叶斯参数调优（这可能需要几分钟）...")
    best_params = predictor.bayesian_optimization(X_train, y_train, X_val, y_val, n_iter=30)
    
    # 训练最终模型
    predictor.train(X_train, y_train, X_val, y_val, params=best_params)
    
    # 评估
    metrics = predictor.evaluate(X_test, y_test)
    
    # 保存
    predictor.save()
    
    # 保存特征配置
    joblib.dump(feature_cols, 'models/feature_config.pkl')
    
    # 特征重要性
    importance_df = pd.DataFrame({
        'feature': feature_cols,
        'importance': predictor.model.feature_importance(importance_type='gain')
    }).sort_values('importance', ascending=False)
    
    importance_df.to_csv('models/feature_importance.csv', index=False)
    print("\nTop 20 特征重要性：")
    print(importance_df.head(20))
    
    logger.info("=" * 50)
    logger.info("模型训练完成")
    logger.info("=" * 50)


if __name__ == '__main__':
    main()
