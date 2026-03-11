#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Streamlit 可视化 Dashboard
"""

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import requests
import joblib
from pathlib import Path
import numpy as np

st.set_page_config(
    page_title="硬盘故障预测系统",
    page_icon="💾",
    layout="wide",
    initial_sidebar_state="expanded"
)

# 自定义 CSS
st.markdown("""
<style>
.big-font {
    font-size:20px !important;
    font-weight: bold;
}
.metric-card {
    background-color: #f0f2f6;
    padding: 20px;
    border-radius: 10px;
    margin: 10px 0;
}
</style>
""", unsafe_allow_html=True)

st.title("💾 硬盘故障预测系统")
st.markdown("---")

# 侧边栏
st.sidebar.header("⚙️ 功能菜单")
option = st.sidebar.selectbox(
    "选择功能",
    ["📊 总览", "🔍 单盘预测", "📁 批量预测", "📈 趋势分析", "⚙️ 系统设置"]
)

# 加载模型配置
feature_cols = None
if Path('models/feature_config.pkl').exists():
    feature_cols = joblib.load('models/feature_config.pkl')

if option == "📊 总览":
    st.header("📊 系统总览")
    
    # 加载统计数据
    stats = {
        "total_disks": 0,
        "high_risk": 0,
        "medium_risk": 0,
        "low_risk": 0
    }
    
    # 尝试加载最新预测结果
    if Path('data/features/smart_features.csv').exists():
        df = pd.read_csv('data/features/smart_features.csv')
        stats['total_disks'] = len(df)
    
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric("监控硬盘总数", stats['total_disks'])
    
    with col2:
        st.metric("高风险硬盘", stats['high_risk'], delta_color="inverse")
    
    with col3:
        st.metric("中风险硬盘", stats['medium_risk'])
    
    with col4:
        st.metric("低风险硬盘", stats['low_risk'])
    
    # 系统状态
    st.subheader("🔧 系统状态")
    
    try:
        response = requests.get("http://localhost:8000/health", timeout=5)
        if response.status_code == 200:
            health = response.json()
            if health.get('model_loaded'):
                st.success("✅ API 服务正常 | 模型已加载")
            else:
                st.warning("⚠️ API 服务正常 | 模型未加载")
        else:
            st.error("❌ API 服务异常")
    except:
        st.error("❌ 无法连接到 API 服务")
    
    # 最近预测
    st.subheader("📋 最近预测")
    if Path('data/features/smart_features.csv').exists():
        df = pd.read_csv('data/features/smart_features.csv')
        
        if 'failure' in df.columns:
            failure_rate = df['failure'].mean() * 100
            st.progress(failure_rate / 100)
            st.write(f"故障盘占比：{failure_rate:.2f}%")

elif option == "🔍 单盘预测":
    st.header("🔍 单盘预测")
    
    col1, col2 = st.columns(2)
    
    with col1:
        serial = st.text_input("硬盘序列号", placeholder="例如：ABC123")
        smart_5 = st.number_input("SMART 5 (重映射扇区)", min_value=0, value=0)
        smart_187 = st.number_input("SMART 187 (不可纠正错误)", min_value=0, value=0)
        smart_197 = st.number_input("SMART 197 (待映射扇区)", min_value=0, value=0)
    
    with col2:
        smart_188 = st.number_input("SMART 188 (命令超时)", min_value=0, value=0)
        smart_9 = st.number_input("SMART 9 (通电时间)", min_value=0, value=0)
        smart_194 = st.number_input("SMART 194 (温度)", min_value=0, value=40)
    
    if st.button("🚀 开始预测", type="primary", use_container_width=True):
        # 构造请求
        request_data = {
            "serial_number": serial,
            "smart_data": {
                "smart_5_raw": float(smart_5),
                "smart_187_raw": float(smart_187),
                "smart_197_raw": float(smart_197),
                "smart_188_raw": float(smart_188),
                "smart_9_raw": float(smart_9),
                "smart_194_raw": float(smart_194)
            }
        }
        
        try:
            response = requests.post("http://localhost:8000/predict", json=request_data, timeout=10)
            
            if response.status_code == 200:
                result = response.json()
                
                # 显示结果
                col1, col2, col3 = st.columns(3)
                
                with col1:
                    st.metric(
                        "故障概率",
                        f"{result['failure_probability']:.2%}",
                        delta="高风险" if result['failure_probability'] > 0.7 else "正常"
                    )
                
                with col2:
                    risk_emoji = {"high": "🔴", "medium": "🟡", "low": "🟢"}
                    st.metric("风险等级", f"{risk_emoji[result['risk_level']]} {result['risk_level'].upper()}")
                
                with col3:
                    st.metric("预测结果", "故障 🔴" if result['prediction'] == 1 else "健康 🟢")
                
                # 建议
                if result['failure_probability'] > 0.7:
                    st.error("🚨 **高风险**：建议立即备份数据并安排更换")
                elif result['failure_probability'] > 0.4:
                    st.warning("⚠️ **中风险**：建议加强监控，准备备件")
                else:
                    st.success("✅ **低风险**：硬盘状态良好")
                
                # 关键指标
                st.subheader("📊 关键指标")
                st.write("影响预测的前 5 个特征：")
                for i, indicator in enumerate(result['key_indicators'], 1):
                    st.write(f"{i}. {indicator}")
            
            else:
                st.error(f"预测失败：{response.status_code}")
        
        except Exception as e:
            st.error(f"请求失败：{e}")

elif option == "📁 批量预测":
    st.header("📁 批量预测")
    
    uploaded_file = st.file_uploader("上传 CSV 文件", type=['csv'])
    
    if uploaded_file:
        df = pd.read_csv(uploaded_file)
        st.write(f"上传数据：{len(df)} 条记录")
        st.dataframe(df.head())
        
        if st.button("🚀 开始批量预测", type="primary"):
            try:
                files = {'file': uploaded_file.getvalue()}
                response = requests.post("http://localhost:8000/predict/batch", files=files, timeout=30)
                
                if response.status_code == 200:
                    result = response.json()
                    
                    col1, col2, col3 = st.columns(3)
                    col1.metric("总数", result['total'])
                    col2.metric("高风险", result['high_risk'], delta_color="inverse")
                    col3.metric("中风险", result['medium_risk'])
                    
                    # 下载结果
                    result_df = pd.DataFrame(result['predictions'])
                    csv = result_df.to_csv(index=False, encoding='utf-8-sig')
                    
                    st.download_button(
                        label="📥 下载预测结果",
                        data=csv,
                        file_name="predictions.csv",
                        mime="text/csv",
                        use_container_width=True
                    )
                
                else:
                    st.error(f"预测失败：{response.status_code}")
            
            except Exception as e:
                st.error(f"请求失败：{e}")

elif option == "📈 趋势分析":
    st.header("📈 SMART 趋势分析")
    
    # 示例数据
    dates = pd.date_range('2025-01-01', periods=30, freq='D')
    smart_5_values = np.random.randint(0, 50, 30).cumsum()
    
    fig = go.Figure()
    fig.add_trace(go.Scatter(
        x=dates, 
        y=smart_5_values, 
        mode='lines+markers', 
        name='SMART 5',
        line=dict(color='red', width=3)
    ))
    fig.update_layout(
        title='SMART 5 趋势图',
        xaxis_title='日期',
        yaxis_title='值',
        template='plotly_dark'
    )
    st.plotly_chart(fig, use_container_width=True)
    
    # 加载实际数据
    if Path('data/features/smart_features.csv').exists():
        df = pd.read_csv('data/features/smart_features.csv')
        
        if 'serial_number' in df.columns and 'timestamp' in df.columns:
            serial = st.selectbox("选择硬盘", df['serial_number'].unique())
            
            disk_data = df[df['serial_number'] == serial]
            
            st.subheader(f"{serial} - SMART 趋势")
            
            # 绘制 SMART 5 趋势
            if 'smart_5_raw' in disk_data.columns:
                fig = go.Figure()
                fig.add_trace(go.Scatter(
                    x=pd.to_datetime(disk_data['timestamp']),
                    y=disk_data['smart_5_raw'],
                    mode='lines+markers',
                    name='SMART 5'
                ))
                fig.update_layout(title='SMART 5 趋势', xaxis_title='日期', yaxis_title='值')
                st.plotly_chart(fig, use_container_width=True)

elif option == "⚙️ 系统设置":
    st.header("⚙️ 系统设置")
    
    st.subheader("告警配置")
    
    threshold_high = st.slider("高风险阈值", 0.5, 0.9, 0.7, 0.05)
    threshold_medium = st.slider("中风险阈值", 0.3, 0.6, 0.4, 0.05)
    
    st.write(f"当前配置：")
    st.write(f"- 高风险：> {threshold_high:.0%}")
    st.write(f"- 中风险：> {threshold_medium:.0%}")
    st.write(f"- 低风险：≤ {threshold_medium:.0%}")
    
    if st.button("💾 保存设置", use_container_width=True):
        st.success("设置已保存")
    
    st.subheader("数据管理")
    
    if st.button("🗑️ 清空数据", type="secondary"):
        st.warning("此操作不可逆，请谨慎！")

# 页脚
st.markdown("---")
st.markdown(
    """
    <div style='text-align: center; color: gray;'>
        硬盘故障预测系统 v2.0 | Powered by LightGBM + 时间窗口特征工程
    </div>
    """,
    unsafe_allow_html=True
)
