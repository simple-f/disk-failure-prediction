#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
FastAPI 预测服务
"""

from fastapi import FastAPI, UploadFile, File, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
import pandas as pd
import joblib
import numpy as np
from typing import List, Dict
import logging
from pathlib import Path

# 配置日志
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(title="硬盘故障预测 API", version="2.0")

# CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# 全局变量
model = None
feature_cols = None


@app.on_event("startup")
def load_model():
    """启动时加载模型"""
    global model, feature_cols
    
    model_path = Path('models/disk_failure_model.pkl')
    config_path = Path('models/feature_config.pkl')
    
    if model_path.exists() and config_path.exists():
        model_data = joblib.load(model_path)
        model = model_data['model']
        feature_cols = joblib.load(config_path)
        logger.info("模型加载完成")
    else:
        logger.warning("模型文件不存在，请先训练模型")


class PredictionRequest(BaseModel):
    serial_number: str
    smart_data: Dict[str, float]


class PredictionResponse(BaseModel):
    serial_number: str
    failure_probability: float
    prediction: int
    risk_level: str
    key_indicators: List[str]


@app.get("/")
def root():
    """根路径"""
    return {
        "message": "硬盘故障预测 API",
        "version": "2.0",
        "docs": "/docs"
    }


@app.get("/health")
def health_check():
    """健康检查"""
    return {
        "status": "ok",
        "model_loaded": model is not None
    }


@app.post("/predict", response_model=PredictionResponse)
async def predict_single(request: PredictionRequest):
    """单盘预测"""
    if model is None:
        raise HTTPException(status_code=503, detail="模型未加载")
    
    try:
        # 准备特征
        X = pd.DataFrame([request.smart_data])
        X = X.reindex(columns=feature_cols, fill_value=0)
        
        # 预测
        proba = model.predict(X)[0]
        pred = 1 if proba > 0.5 else 0
        
        # 风险等级
        if proba > 0.7:
            risk_level = "high"
        elif proba > 0.4:
            risk_level = "medium"
        else:
            risk_level = "low"
        
        # 关键指标
        importance = model.feature_importance(importance_type='gain')
        top_indices = np.argsort(importance)[-5:][::-1]
        key_indicators = [feature_cols[i] for i in top_indices]
        
        return PredictionResponse(
            serial_number=request.serial_number,
            failure_probability=float(proba),
            prediction=pred,
            risk_level=risk_level,
            key_indicators=key_indicators
        )
    
    except Exception as e:
        logger.error(f"预测失败：{e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/predict/batch")
async def predict_batch(file: UploadFile = File(...)):
    """批量预测（上传 CSV）"""
    if model is None:
        raise HTTPException(status_code=503, detail="模型未加载")
    
    try:
        # 读取 CSV
        df = pd.read_csv(file.file)
        
        # 特征处理
        X = df.reindex(columns=feature_cols, fill_value=0)
        
        # 预测
        probas = model.predict(X)
        preds = (probas > 0.5).astype(int)
        
        result = df.copy()
        result['failure_probability'] = probas
        result['prediction'] = preds
        result['risk_level'] = pd.cut(
            probas,
            bins=[0, 0.4, 0.7, 1.0],
            labels=['low', 'medium', 'high']
        )
        
        return {
            "total": len(result),
            "high_risk": int((probas > 0.7).sum()),
            "medium_risk": int(((probas > 0.4) & (probas <= 0.7)).sum()),
            "low_risk": int((probas <= 0.4).sum()),
            "predictions": result.to_dict('records')
        }
    
    except Exception as e:
        logger.error(f"批量预测失败：{e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/features")
def get_features():
    """获取特征列表"""
    if feature_cols is None:
        raise HTTPException(status_code=503, detail="特征配置未加载")
    
    return {
        "count": len(feature_cols),
        "features": feature_cols
    }


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
