#!/usr/bin/env python3
"""
Enhanced Batch Parser Analyzer
批次分析 CAP 檔案的解析成功率，基於 JSON 規則檔案中定義的所有 keys
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
import statistics


class EnhancedBatchAnalyzer:
    def __init__(self, parser_script: str, json_file: str, max_workers: int = 8):
        self.parser_script = parser_script
        self.json_file = json_file
        self.max_workers = max_workers
        
        # 載入 JSON 規則以取得所有要檢查的 keys
        self.json_rules = self.load_json_rules(json_file)
        self.expected_keys = set(self.json_rules.keys())
        self.total_expected_keys = len(self.expected_keys)
        
        # 統計資料
        self.stats_lock = threading.Lock()
        self.total_files = 0
        self.processed_files = 0
        self.error_files = []
        
        # 每個檔案的解析結果
        self.file_results = []
        
        # 每個 key 的統計
        self.key_success_count = defaultdict(int)
        self.key_missing_files = defaultdict(list)
        
        # 成功率分佈
        self.success_rate_distribution = defaultdict(int)
        
    def load_json_rules(self, json_path: str) -> Dict:
        """載入 JSON 規則檔案"""
        try:
            with open(json_path, 'r', encoding='utf-8') as f:
                return json.load(f)
        except Exception as e:
            print(f"Error loading JSON file: {e}")
            sys.exit(1)
    
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
        """處理單一檔案並計算解析成功率"""
        result = {
            'file': str(cap_file),
            'success': False,
            'parsed_data': {},
            'parsed_keys': set(),
            'missing_keys': set(),
            'extra_keys': set(),
            'success_rate': 0.0,
            'parsed_count': 0,
            'error': None
        }
        
        try:
            # 建構命令
            cmd = [
                sys.executable,
                self.parser_script,
                "parse_single_file",
                str(cap_file),
                self.json_file
            ]
            
            # 執行命令
            process_result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                encoding='utf-8',
                timeout=30
            )
            
            if process_result.returncode != 0:
                result['error'] = f"Parser failed: {process_result.stderr}"
                return result
            
            # 解析輸出
            parsed_data = self.parse_output(process_result.stdout)
            result['parsed_data'] = parsed_data
            result['success'] = True
            
            # 分析解析結果
            parsed_keys = set(parsed_data.keys())
            result['parsed_keys'] = parsed_keys
            result['missing_keys'] = self.expected_keys - parsed_keys
            result['extra_keys'] = parsed_keys - self.expected_keys
            
            # 計算成功率（基於 JSON 中定義的 keys）
            result['parsed_count'] = len(self.expected_keys & parsed_keys)
            result['success_rate'] = (result['parsed_count'] / self.total_expected_keys * 100) if self.total_expected_keys > 0 else 0
            
        except subprocess.TimeoutExpired:
            result['error'] = "Parser timeout (30s)"
        except Exception as e:
            result['error'] = f"Error: {str(e)}"
        
        return result
    
    def update_stats(self, result: Dict):
        """更新統計資料"""
        with self.stats_lock:
            self.processed_files += 1
            self.file_results.append(result)
            
            if result['success']:
                # 更新每個 key 的成功統計
                for key in self.expected_keys:
                    if key in result['parsed_keys']:
                        self.key_success_count[key] += 1
                    else:
                        self.key_missing_files[key].append(result['file'])
                
                # 更新成功率分佈
                rate_bucket = int(result['success_rate'] / 10) * 10  # 0, 10, 20, ..., 90, 100
                self.success_rate_distribution[rate_bucket] += 1
            else:
                self.error_files.append({
                    'file': result['file'],
                    'error': result['error']
                })
            
            # 顯示進度
            if self.processed_files % 50 == 0 or self.processed_files == self.total_files:
                print(f"Progress: {self.processed_files}/{self.total_files} "
                      f"({self.processed_files/self.total_files*100:.1f}%)")
    
    def process_files_batch(self, cap_files: List[Path]):
        """批次處理檔案"""
        self.total_files = len(cap_files)
        print(f"\nStarting batch analysis with {self.max_workers} workers...")
        print(f"Total keys defined in JSON: {self.total_expected_keys}")
        print(f"Processing {self.total_files} CAP files...\n")
        
        start_time = time.time()
        
        with concurrent.futures.ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            future_to_file = {
                executor.submit(self.process_single_file, cap_file): cap_file 
                for cap_file in cap_files
            }
            
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
                        'success_rate': 0.0,
                        'parsed_count': 0,
                        'parsed_keys': set(),
                        'missing_keys': set(),
                        'extra_keys': set()
                    })
        
        elapsed_time = time.time() - start_time
        self.analyze_and_report(elapsed_time)
    
    def analyze_and_report(self, elapsed_time: float):
        """分析結果並生成報告"""
        # 計算整體統計
        successful_files = [r for r in self.file_results if r['success']]
        if successful_files:
            success_rates = [r['success_rate'] for r in successful_files]
            avg_success_rate = statistics.mean(success_rates)
            median_success_rate = statistics.median(success_rates)
            min_success_rate = min(success_rates)
            max_success_rate = max(success_rates)
            
            # 找出表現最好和最差的檔案
            best_files = sorted(successful_files, key=lambda x: x['success_rate'], reverse=True)[:10]
            worst_files = sorted(successful_files, key=lambda x: x['success_rate'])[:10]
        else:
            avg_success_rate = median_success_rate = min_success_rate = max_success_rate = 0
            best_files = worst_files = []
        
        # 印出摘要
        print(f"\n{'='*80}")
        print("ANALYSIS SUMMARY")
        print(f"{'='*80}")
        print(f"Total files processed: {self.processed_files}")
        print(f"Successful parsing: {len(successful_files)}")
        print(f"Failed parsing: {len(self.error_files)}")
        print(f"Processing time: {elapsed_time:.2f} seconds")
        print(f"Average speed: {self.processed_files/elapsed_time:.2f} files/second")
        
        print(f"\n{'='*80}")
        print("PARSING SUCCESS RATE STATISTICS")
        print(f"{'='*80}")
        print(f"Average success rate: {avg_success_rate:.1f}%")
        print(f"Median success rate: {median_success_rate:.1f}%")
        print(f"Min success rate: {min_success_rate:.1f}%")
        print(f"Max success rate: {max_success_rate:.1f}%")
        
        # 成功率分佈
        print(f"\n{'='*80}")
        print("SUCCESS RATE DISTRIBUTION")
        print(f"{'='*80}")
        for rate in sorted(self.success_rate_distribution.keys()):
            count = self.success_rate_distribution[rate]
            percentage = count / len(successful_files) * 100 if successful_files else 0
            bar = '█' * int(percentage / 2)
            print(f"{rate:3d}-{rate+9:3d}%: {count:4d} files ({percentage:5.1f}%) {bar}")
        
        # Key 統計
        print(f"\n{'='*80}")
        print("KEY PARSING STATISTICS (Top 20 Most Difficult)")
        print(f"{'='*80}")
        key_stats = []
        for key in self.expected_keys:
            success_count = self.key_success_count[key]
            success_rate = success_count / len(successful_files) * 100 if successful_files else 0
            key_stats.append((key, success_count, success_rate))
        
        # 按成功率排序，顯示最難解析的 keys
        key_stats.sort(key=lambda x: x[2])
        for i, (key, count, rate) in enumerate(key_stats[:20]):
            print(f"{i+1:2d}. {key:30s} - Success: {count:4d}/{len(successful_files)} ({rate:5.1f}%)")
        
        # 最佳和最差檔案
        if best_files:
            print(f"\n{'='*80}")
            print("TOP 10 BEST PERFORMING FILES")
            print(f"{'='*80}")
            for i, file_result in enumerate(best_files):
                print(f"{i+1:2d}. {Path(file_result['file']).name} - {file_result['success_rate']:.1f}% "
                      f"({file_result['parsed_count']}/{self.total_expected_keys})")
        
        if worst_files:
            print(f"\n{'='*80}")
            print("TOP 10 WORST PERFORMING FILES")
            print(f"{'='*80}")
            for i, file_result in enumerate(worst_files):
                print(f"{i+1:2d}. {Path(file_result['file']).name} - {file_result['success_rate']:.1f}% "
                      f"({file_result['parsed_count']}/{self.total_expected_keys})")
        
        # 儲存詳細報告
        self.save_detailed_report(successful_files, key_stats)
    
    def save_detailed_report(self, successful_files: List[Dict], key_stats: List[Tuple]):
        """儲存詳細報告到檔案"""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        # 主報告
        report_filename = f"parsing_analysis_report_{timestamp}.txt"
        with open(report_filename, 'w', encoding='utf-8') as f:
            f.write("CAP FILES PARSING ANALYSIS REPORT\n")
            f.write(f"Generated at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
            f.write(f"JSON Rules File: {self.json_file}\n")
            f.write(f"Total Keys Defined: {self.total_expected_keys}\n")
            f.write(f"{'='*80}\n\n")
            
            # 整體統計
            if successful_files:
                success_rates = [r['success_rate'] for r in successful_files]
                f.write(f"OVERALL STATISTICS\n")
                f.write(f"-"*80 + "\n")
                f.write(f"Total files: {self.total_files}\n")
                f.write(f"Successfully parsed: {len(successful_files)}\n")
                f.write(f"Failed to parse: {len(self.error_files)}\n")
                f.write(f"Average success rate: {statistics.mean(success_rates):.2f}%\n")
                f.write(f"Median success rate: {statistics.median(success_rates):.2f}%\n")
                f.write(f"Standard deviation: {statistics.stdev(success_rates):.2f}%\n" if len(success_rates) > 1 else "")
                f.write(f"Min success rate: {min(success_rates):.2f}%\n")
                f.write(f"Max success rate: {max(success_rates):.2f}%\n\n")
            
            # 完整的 key 統計
            f.write(f"\nCOMPLETE KEY STATISTICS\n")
            f.write(f"-"*80 + "\n")
            f.write(f"{'Key':<40} {'Success Count':>15} {'Success Rate':>15}\n")
            f.write(f"-"*80 + "\n")
            for key, count, rate in sorted(key_stats, key=lambda x: x[2]):
                f.write(f"{key:<40} {count:>15} {rate:>14.1f}%\n")
            
            # 錯誤檔案
            if self.error_files:
                f.write(f"\n\nERROR FILES ({len(self.error_files)})\n")
                f.write(f"-"*80 + "\n")
                for error_info in self.error_files:
                    f.write(f"File: {error_info['file']}\n")
                    f.write(f"Error: {error_info['error']}\n\n")
        
        print(f"\nDetailed report saved to: {report_filename}")
        
        # CSV 報告 - 每個檔案的解析結果
        csv_filename = f"file_parsing_results_{timestamp}.csv"
        with open(csv_filename, 'w', encoding='utf-8') as f:
            f.write("file,success_rate,parsed_count,total_keys,missing_count\n")
            for result in self.file_results:
                if result['success']:
                    missing_count = len(result['missing_keys'])
                    f.write(f'"{result["file"]}",{result["success_rate"]:.2f},{result["parsed_count"]},'
                           f'{self.total_expected_keys},{missing_count}\n')
        
        print(f"File results CSV saved to: {csv_filename}")
        
        # CSV 報告 - 每個 key 的缺失檔案
        key_missing_csv = f"key_missing_files_{timestamp}.csv"
        with open(key_missing_csv, 'w', encoding='utf-8') as f:
            f.write("key,missing_in_file\n")
            for key, files in self.key_missing_files.items():
                for file in files:
                    f.write(f'"{key}","{file}"\n')
        
        print(f"Key missing files CSV saved to: {key_missing_csv}")


def main():
    parser = argparse.ArgumentParser(
        description='Analyze CAP files parsing success rate based on JSON rules',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s /path/to/cap/files /path/to/rules.json
  %(prog)s /data/caps config.json --workers 16
  %(prog)s . rules.json --parser-script /home/user/parser/main.py
        """
    )
    
    parser.add_argument('directory', help='Directory containing CAP files')
    parser.add_argument('json_file', help='JSON rules file defining expected keys')
    parser.add_argument('--parser-script', default='/home/gustave/dbWorker/ParseEngine/main.py', 
                       help='Path to the parser script (default: main.py)')
    parser.add_argument('--workers', type=int, default=8, 
                       help='Number of worker threads (default: 8)')
    
    args = parser.parse_args()
    
    # 檢查輸入
    if not Path(args.directory).exists():
        print(f"Error: Directory not found: {args.directory}")
        sys.exit(1)
    
    if not Path(args.json_file).exists():
        print(f"Error: JSON file not found: {args.json_file}")
        sys.exit(1)
    
    if not Path(args.parser_script).exists():
        print(f"Error: Parser script not found: {args.parser_script}")
        sys.exit(1)
    
    # 建立分析器
    analyzer = EnhancedBatchAnalyzer(
        parser_script=args.parser_script,
        json_file=args.json_file,
        max_workers=args.workers
    )
    
    # 尋找所有 CAP 檔案
    cap_files = analyzer.find_cap_files(args.directory)
    
    if not cap_files:
        print("No .cap files found in the specified directory")
        sys.exit(1)
    
    # 開始批次分析
    analyzer.process_files_batch(cap_files)


if __name__ == "__main__":
    main()