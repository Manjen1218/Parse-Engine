#!/usr/bin/env python3
"""
Batch Parser Validator with Multi-threading
用於批次檢查大量 CAP 檔案的解析結果，特別檢查 board_pn, board_sn, board_ver 欄位
"""

import json
import subprocess
import sys
import argparse
from pathlib import Path
from typing import Dict, List, Set, Tuple
import concurrent.futures
from datetime import datetime
import threading
import time
from collections import defaultdict
import os


class BatchParserValidator:
    def __init__(self, parser_script: str = "main.py", json_file: str = None, max_workers: int = 8):
        self.parser_script = parser_script
        self.json_file = json_file
        self.max_workers = max_workers
        self.required_fields = ['board_pn', 'board_sn', 'board_ver']
        
        # 統計資料
        self.stats_lock = threading.Lock()
        self.total_files = 0
        self.processed_files = 0
        self.success_files = 0
        self.failed_files = 0
        self.missing_fields_count = defaultdict(int)
        self.error_files = []
        self.missing_fields_files = defaultdict(list)
        
    def find_cap_files(self, directory: str) -> List[Path]:
        """遞迴尋找所有 .cap 檔案"""
        cap_files = []
        try:
            path = Path(directory)
            cap_files = list(path.rglob("*.cap"))
            print(f"Found {len(cap_files)} .cap files in {directory}")
        except Exception as e:
            print(f"Error scanning directory: {e}")
        return cap_files
    
    def parse_output(self, output: str) -> Dict[str, str]:
        """解析 parser 的輸出"""
        parsed_data = {}
        lines = output.split('\n')
        start_parsing = False
        
        for line in lines:
            if "Parsed data:" in line:
                start_parsing = True
                continue
            
            if start_parsing and line.strip():
                if ':' in line and line.startswith('  '):
                    key_value = line.strip().split(':', 1)
                    if len(key_value) == 2:
                        key = key_value[0].strip()
                        value = key_value[1].strip()
                        parsed_data[key] = value
        
        return parsed_data
    
    def process_single_file(self, cap_file: Path) -> Dict:
        """處理單一檔案並回傳結果"""
        result = {
            'file': str(cap_file),
            'success': False,
            'parsed_data': {},
            'missing_fields': [],
            'error': None,
            'has_all_required': False
        }
        
        try:
            # 建構命令
            cmd = [sys.executable, self.parser_script, "parse_single_file", str(cap_file)]
            if self.json_file:
                cmd.append(self.json_file)
            
            # 執行命令
            process_result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                encoding='utf-8',
                timeout=30  # 30秒超時
            )
            
            if process_result.returncode != 0:
                result['error'] = f"Parser failed: {process_result.stderr}"
                return result
            
            # 解析輸出
            parsed_data = self.parse_output(process_result.stdout)
            result['parsed_data'] = parsed_data
            result['success'] = True
            
            # 檢查必要欄位
            missing_fields = []
            for field in self.required_fields:
                if field not in parsed_data:
                    missing_fields.append(field)
            
            result['missing_fields'] = missing_fields
            result['has_all_required'] = len(missing_fields) == 0
            
        except subprocess.TimeoutExpired:
            result['error'] = "Parser timeout (30s)"
        except Exception as e:
            result['error'] = f"Error: {str(e)}"
        
        return result
    
    def update_stats(self, result: Dict):
        """更新統計資料（執行緒安全）"""
        with self.stats_lock:
            self.processed_files += 1
            
            if result['success']:
                if result['has_all_required']:
                    self.success_files += 1
                else:
                    for field in result['missing_fields']:
                        self.missing_fields_count[field] += 1
                        self.missing_fields_files[field].append(result['file'])
            else:
                self.failed_files += 1
                self.error_files.append({
                    'file': result['file'],
                    'error': result['error']
                })
            
            # 顯示進度
            if self.processed_files % 100 == 0:
                print(f"Progress: {self.processed_files}/{self.total_files} "
                      f"({self.processed_files/self.total_files*100:.1f}%)")
    
    def process_files_batch(self, cap_files: List[Path]):
        """使用多執行緒批次處理檔案"""
        self.total_files = len(cap_files)
        print(f"\nStarting batch processing with {self.max_workers} workers...")
        print(f"Checking for required fields: {', '.join(self.required_fields)}")
        
        start_time = time.time()
        
        with concurrent.futures.ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            # 提交所有任務
            future_to_file = {
                executor.submit(self.process_single_file, cap_file): cap_file 
                for cap_file in cap_files
            }
            
            # 處理完成的任務
            for future in concurrent.futures.as_completed(future_to_file):
                cap_file = future_to_file[future]
                try:
                    result = future.result()
                    self.update_stats(result)
                except Exception as e:
                    print(f"Error processing {cap_file}: {e}")
                    self.update_stats({
                        'file': str(cap_file),
                        'success': False,
                        'error': str(e),
                        'has_all_required': False,
                        'missing_fields': []
                    })
        
        elapsed_time = time.time() - start_time
        self.print_summary(elapsed_time)
        self.save_detailed_report()
    
    def print_summary(self, elapsed_time: float):
        """印出統計摘要"""
        print(f"\n{'='*80}")
        print("BATCH PROCESSING SUMMARY")
        print(f"{'='*80}")
        print(f"Total files processed: {self.processed_files}")
        print(f"Processing time: {elapsed_time:.2f} seconds")
        print(f"Average speed: {self.processed_files/elapsed_time:.2f} files/second")
        print(f"\n✓ Files with all required fields: {self.success_files} "
              f"({self.success_files/self.total_files*100:.1f}%)")
        print(f"✗ Files with parsing errors: {self.failed_files} "
              f"({self.failed_files/self.total_files*100:.1f}%)")
        
        files_missing_fields = self.total_files - self.success_files - self.failed_files
        print(f"⚠ Files missing some fields: {files_missing_fields} "
              f"({files_missing_fields/self.total_files*100:.1f}%)")
        
        print(f"\n{'='*80}")
        print("MISSING FIELDS STATISTICS")
        print(f"{'='*80}")
        for field in self.required_fields:
            count = self.missing_fields_count[field]
            percentage = count / self.total_files * 100 if self.total_files > 0 else 0
            print(f"{field}: missing in {count} files ({percentage:.1f}%)")
    
    def save_detailed_report(self):
        """儲存詳細報告"""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        report_filename = f"batch_validation_report_{timestamp}.txt"
        
        with open(report_filename, 'w', encoding='utf-8') as f:
            f.write("BATCH PARSER VALIDATION REPORT\n")
            f.write(f"Generated at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
            f.write(f"{'='*80}\n\n")
            
            f.write(f"Total files: {self.total_files}\n")
            f.write(f"Success (all fields): {self.success_files}\n")
            f.write(f"Failed to parse: {self.failed_files}\n")
            f.write(f"Missing some fields: {self.total_files - self.success_files - self.failed_files}\n\n")
            
            # 錯誤檔案列表
            if self.error_files:
                f.write(f"\nFILES WITH ERRORS ({len(self.error_files)}):\n")
                f.write("-" * 80 + "\n")
                for error_info in self.error_files[:100]:  # 只顯示前100個
                    f.write(f"File: {error_info['file']}\n")
                    f.write(f"Error: {error_info['error']}\n\n")
                if len(self.error_files) > 100:
                    f.write(f"... and {len(self.error_files) - 100} more error files\n")
            
            # 缺少欄位的檔案
            for field in self.required_fields:
                if field in self.missing_fields_files:
                    files = self.missing_fields_files[field]
                    f.write(f"\nFILES MISSING '{field}' ({len(files)}):\n")
                    f.write("-" * 80 + "\n")
                    for file in files[:50]:  # 只顯示前50個
                        f.write(f"  - {file}\n")
                    if len(files) > 50:
                        f.write(f"  ... and {len(files) - 50} more files\n")
        
        print(f"\nDetailed report saved to: {report_filename}")
        
        # 儲存缺少欄位的檔案清單（CSV格式）
        csv_filename = f"missing_fields_{timestamp}.csv"
        with open(csv_filename, 'w', encoding='utf-8') as f:
            f.write("file,missing_fields\n")
            for field, files in self.missing_fields_files.items():
                for file in files:
                    f.write(f'"{file}","{field}"\n')
        
        print(f"Missing fields CSV saved to: {csv_filename}")


def main():
    parser = argparse.ArgumentParser(description='Batch validate parser output for CAP files')
    parser.add_argument('directory', help='Directory containing CAP files')
    parser.add_argument('--json-file', help='Path to the JSON rules file (optional)')
    parser.add_argument('--parser-script', default='/home/gustave/dbWorker/ParseEngine/main.py', help='Path to the parser script')
    parser.add_argument('--workers', type=int, default=8, help='Number of worker threads (default: 8)')
    parser.add_argument('--required-fields', nargs='+', 
                       default=['board_pn', 'board_sn', 'board_ver'],
                       help='Required fields to check (default: board_pn board_sn board_ver)')
    
    args = parser.parse_args()
    
    # 檢查目錄是否存在
    if not Path(args.directory).exists():
        print(f"Error: Directory not found: {args.directory}")
        sys.exit(1)
    
    # 檢查 parser script
    if not Path(args.parser_script).exists():
        print(f"Error: Parser script not found: {args.parser_script}")
        sys.exit(1)
    
    # 建立驗證器
    validator = BatchParserValidator(
        parser_script=args.parser_script,
        json_file=args.json_file,
        max_workers=args.workers
    )
    
    # 設定要檢查的欄位
    validator.required_fields = args.required_fields
    
    # 尋找所有 CAP 檔案
    cap_files = validator.find_cap_files(args.directory)
    
    if not cap_files:
        print("No .cap files found in the specified directory")
        sys.exit(1)
    
    # 批次處理
    validator.process_files_batch(cap_files)


if __name__ == "__main__":
    main()