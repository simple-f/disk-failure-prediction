#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
SMART 数据采集脚本
"""

import subprocess
import pandas as pd
import json
from datetime import datetime
import sqlite3
import logging
from pathlib import Path

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/data_collector.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


class SMARTCollector:
    """SMART 数据采集器"""
    
    def __init__(self, db_path='data/smart_data.db'):
        """初始化采集器"""
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self.init_db()
    
    def init_db(self):
        """初始化数据库"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS smart_data (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp TEXT NOT NULL,
                serial_number TEXT NOT NULL,
                model TEXT NOT NULL,
                capacity_bytes INTEGER,
                failure INTEGER DEFAULT 0,
                smart_1_raw REAL,
                smart_3_raw REAL,
                smart_4_raw REAL,
                smart_5_raw REAL,
                smart_7_raw REAL,
                smart_9_raw REAL,
                smart_10_raw REAL,
                smart_12_raw REAL,
                smart_187_raw REAL,
                smart_188_raw REAL,
                smart_194_raw REAL,
                smart_197_raw REAL,
                smart_198_raw REAL,
                smart_199_raw REAL,
                smart_201_raw REAL,
                smart_240_raw REAL,
                smart_241_raw REAL,
                smart_242_raw REAL
            )
        ''')
        
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_serial ON smart_data(serial_number)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_timestamp ON smart_data(timestamp)')
        
        conn.commit()
        conn.close()
        logger.info("数据库初始化完成")
    
    def list_disks(self):
        """列出所有硬盘"""
        try:
            result = subprocess.run(
                ['smartctl', '--scan'],
                capture_output=True,
                text=True,
                timeout=30
            )
            
            if result.returncode != 0:
                logger.error(f"获取硬盘列表失败：{result.stderr}")
                return []
            
            disks = []
            for line in result.stdout.strip().split('\n'):
                if line:
                    parts = line.split()
                    if len(parts) >= 2:
                        disks.append({
                            'device': parts[0],
                            'type': parts[2] if len(parts) > 2 else 'scsi'
                        })
            
            logger.info(f"发现 {len(disks)} 块硬盘")
            return disks
        
        except FileNotFoundError:
            logger.error("smartctl 未找到，请安装 smartmontools")
            return []
        except Exception as e:
            logger.error(f"列出硬盘失败：{e}")
            return []
    
    def get_smart_data(self, device):
        """获取单块硬盘的 SMART 数据"""
        try:
            result = subprocess.run(
                ['smartctl', '-j', '-a', device],
                capture_output=True,
                text=True,
                timeout=60
            )
            
            if result.returncode != 0:
                logger.warning(f"获取 {device} SMART 数据失败：{result.stderr}")
                return None
            
            try:
                smart_json = json.loads(result.stdout)
                return smart_json
            except json.JSONDecodeError:
                logger.error(f"解析 {device} SMART 数据失败")
                return None
        
        except Exception as e:
            logger.error(f"获取 SMART 数据失败：{e}")
            return None
    
    def parse_smart_attributes(self, smart_json):
        """解析 SMART 属性"""
        attributes = {}
        
        # 基本信息
        attributes['serial_number'] = smart_json.get('serial_number', 'UNKNOWN')
        attributes['model'] = smart_json.get('model_name', 'UNKNOWN')
        attributes['capacity_bytes'] = smart_json.get('user_capacity', {}).get('bytes', 0)
        
        # SMART 属性
        smart_attrs = smart_json.get('smart_table', [])
        for attr in smart_attrs:
            attr_id = attr.get('id')
            raw_value = attr.get('raw', {}).get('value', 0)
            
            # 映射关键属性
            attribute_map = {
                1: 'smart_1_raw',
                3: 'smart_3_raw',
                4: 'smart_4_raw',
                5: 'smart_5_raw',
                7: 'smart_7_raw',
                9: 'smart_9_raw',
                10: 'smart_10_raw',
                12: 'smart_12_raw',
                187: 'smart_187_raw',
                188: 'smart_188_raw',
                194: 'smart_194_raw',
                197: 'smart_197_raw',
                198: 'smart_198_raw',
                199: 'smart_199_raw',
                201: 'smart_201_raw',
                240: 'smart_240_raw',
                241: 'smart_241_raw',
                242: 'smart_242_raw'
            }
            
            if attr_id in attribute_map:
                attributes[attribute_map[attr_id]] = raw_value
        
        return attributes
    
    def collect(self):
        """执行一次完整采集"""
        timestamp = datetime.now().isoformat()
        disks = self.list_disks()
        
        if not disks:
            logger.warning("未发现硬盘，跳过采集")
            return 0
        
        collected_count = 0
        
        for disk in disks:
            device = disk['device']
            logger.info(f"采集 {device}...")
            
            smart_json = self.get_smart_data(device)
            if not smart_json:
                continue
            
            attrs = self.parse_smart_attributes(smart_json)
            attrs['timestamp'] = timestamp
            
            # 存入数据库
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            columns = ', '.join(attrs.keys())
            placeholders = ', '.join(['?' for _ in attrs])
            
            try:
                cursor.execute(
                    f'INSERT INTO smart_data ({columns}) VALUES ({placeholders})',
                    list(attrs.values())
                )
                conn.commit()
                collected_count += 1
                logger.debug(f"{device} 采集成功")
            except Exception as e:
                logger.error(f"保存 {device} 数据失败：{e}")
                conn.rollback()
            finally:
                conn.close()
        
        logger.info(f"本次采集完成：{collected_count}/{len(disks)}")
        
        # 导出 CSV
        if collected_count > 0:
            self.export_to_csv()
        
        return collected_count
    
    def export_to_csv(self, output_path='data/raw/smart_data.csv'):
        """导出为 CSV"""
        try:
            conn = sqlite3.connect(self.db_path)
            df = pd.read_sql_query('SELECT * FROM smart_data', conn)
            
            Path(output_path).parent.mkdir(parents=True, exist_ok=True)
            df.to_csv(output_path, index=False, encoding='utf-8-sig')
            
            conn.close()
            logger.info(f"数据已导出到 {output_path}")
            return df
        
        except Exception as e:
            logger.error(f"导出 CSV 失败：{e}")
            return None


def main():
    """主函数"""
    logger.info("=" * 50)
    logger.info("开始 SMART 数据采集")
    logger.info("=" * 50)
    
    collector = SMARTCollector()
    count = collector.collect()
    
    logger.info("=" * 50)
    logger.info(f"采集完成：{count} 块硬盘")
    logger.info("=" * 50)


if __name__ == '__main__':
    main()
