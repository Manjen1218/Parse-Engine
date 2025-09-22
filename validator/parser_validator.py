#!/usr/bin/env python3
"""
Parser Output Validator
用於檢查 parser 輸出的資料與 JSON 規則檔案中定義的 key 之間的差異
"""

import json
import subprocess
import sys
import argparse
from pathlib import Path
from typing import Dict, List, Set, Tuple


class ParserValidator:
    def __init__(self, parser_script: str = "main.py"):
        self.parser_script = parser_script
        
    def load_json_rules(self, json_path: str) -> Dict:
        """載入 JSON 規則檔案"""
        try:
            with open(json_path, 'r', encoding='utf-8') as f:
                return json.load(f)
        except Exception as e:
            print(f"Error loading JSON file: {e}")
            return {}
    
    def parse_output(self, output: str) -> Dict[str, str]:
        """解析 parser 的輸出"""
        parsed_data = {}
        
        # 找到 "Parsed data:" 之後的內容
        lines = output.split('\n')
        start_parsing = False
        
        for line in lines:
            if "Parsed data:" in line:
                start_parsing = True
                continue
            
            if start_parsing and line.strip():
                # 解析格式: "  key: value"
                if ':' in line and line.startswith('  '):
                    key_value = line.strip().split(':', 1)
                    if len(key_value) == 2:
                        key = key_value[0].strip()
                        value = key_value[1].strip()
                        parsed_data[key] = value
        
        return parsed_data
    
    def run_parser(self, cap_file: str, json_file: str) -> Tuple[bool, str, Dict[str, str]]:
        """執行 parser 並取得輸出"""
        try:
            # 建構命令
            cmd = [
                sys.executable,
                self.parser_script,
                "parse_single_file",
                cap_file,
                json_file
            ]
            
            # 執行命令
            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                encoding='utf-8'
            )
            
            if result.returncode != 0:
                return False, f"Parser failed with error: {result.stderr}", {}
            
            # 解析輸出
            parsed_data = self.parse_output(result.stdout)
            
            return True, result.stdout, parsed_data
            
        except Exception as e:
            return False, f"Error running parser: {e}", {}
    
    def compare_keys(self, json_keys: Set[str], parsed_keys: Set[str]) -> Dict[str, List[str]]:
        """比較 JSON 定義的 keys 和實際解析出的 keys"""
        return {
            "missing_in_output": sorted(list(json_keys - parsed_keys)),
            "extra_in_output": sorted(list(parsed_keys - json_keys)),
            "successfully_parsed": sorted(list(json_keys & parsed_keys))
        }
    
    def generate_report(self, cap_file: str, json_file: str) -> None:
        """生成完整的驗證報告"""
        print(f"\n{'='*80}")
        print(f"Parser Validation Report")
        print(f"{'='*80}")
        print(f"CAP File: {cap_file}")
        print(f"JSON Rule File: {json_file}")
        print(f"{'='*80}\n")
        
        # 載入 JSON 規則
        json_rules = self.load_json_rules(json_file)
        if not json_rules:
            print("Failed to load JSON rules file!")
            return
        
        json_keys = set(json_rules.keys())
        print(f"Total keys defined in JSON: {len(json_keys)}")
        
        # 執行 parser
        print("\nRunning parser...")
        success, output, parsed_data = self.run_parser(cap_file, json_file)
        
        if not success:
            print(f"Parser execution failed: {output}")
            return
        
        parsed_keys = set(parsed_data.keys())
        print(f"Total keys parsed from CAP: {len(parsed_keys)}")
        
        # 比較結果
        comparison = self.compare_keys(json_keys, parsed_keys)
        
        # 顯示結果
        print(f"\n{'='*80}")
        print("COMPARISON RESULTS")
        print(f"{'='*80}")
        
        # 成功解析的 keys
        print(f"\n✓ Successfully parsed keys: {len(comparison['successfully_parsed'])}")
        if comparison['successfully_parsed']:
            for key in comparison['successfully_parsed']:
                print(f"  - {key}: {parsed_data.get(key, 'N/A')}")
        
        # 缺失的 keys
        print(f"\n✗ Missing keys (defined in JSON but not in output): {len(comparison['missing_in_output'])}")
        if comparison['missing_in_output']:
            for key in comparison['missing_in_output']:
                print(f"  - {key}")
        
        # 額外的 keys
        print(f"\n⚠ Extra keys (in output but not defined in JSON): {len(comparison['extra_in_output'])}")
        if comparison['extra_in_output']:
            for key in comparison['extra_in_output']:
                print(f"  - {key}: {parsed_data.get(key, 'N/A')}")
        
        # 統計摘要
        print(f"\n{'='*80}")
        print("SUMMARY")
        print(f"{'='*80}")
        total_json_keys = len(json_keys)
        successfully_parsed = len(comparison['successfully_parsed'])
        success_rate = (successfully_parsed / total_json_keys * 100) if total_json_keys > 0 else 0
        
        print(f"Success rate: {success_rate:.2f}% ({successfully_parsed}/{total_json_keys})")
        print(f"Missing keys: {len(comparison['missing_in_output'])}")
        print(f"Extra keys: {len(comparison['extra_in_output'])}")
        
        # 輸出詳細報告到檔案
        report_filename = f"validation_report_{Path(cap_file).stem}.txt"
        with open(report_filename, 'w', encoding='utf-8') as f:
            f.write(f"Parser Validation Report\n")
            f.write(f"{'='*80}\n")
            f.write(f"CAP File: {cap_file}\n")
            f.write(f"JSON Rule File: {json_file}\n")
            f.write(f"{'='*80}\n\n")
            
            f.write(f"Success rate: {success_rate:.2f}% ({successfully_parsed}/{total_json_keys})\n\n")
            
            f.write("Missing keys:\n")
            for key in comparison['missing_in_output']:
                f.write(f"  - {key}\n")
            
            f.write("\nExtra keys:\n")
            for key in comparison['extra_in_output']:
                f.write(f"  - {key}: {parsed_data.get(key, 'N/A')}\n")
            
            f.write("\nSuccessfully parsed keys:\n")
            for key in comparison['successfully_parsed']:
                f.write(f"  - {key}: {parsed_data.get(key, 'N/A')}\n")
        
        print(f"\nDetailed report saved to: {report_filename}")


def main():
    parser = argparse.ArgumentParser(description='Validate parser output against JSON rules')
    parser.add_argument('cap_file', help='Path to the CAP file')
    parser.add_argument('json_file', help='Path to the JSON rules file')
    parser.add_argument('--parser-script', default='/home/gustave/dbWorker/ParseEngine/main.py', help='Path to the parser script (default: main.py)')
    
    args = parser.parse_args()
    
    # 檢查檔案是否存在
    if not Path(args.cap_file).exists():
        print(f"Error: CAP file not found: {args.cap_file}")
        sys.exit(1)
    
    if not Path(args.json_file).exists():
        print(f"Error: JSON file not found: {args.json_file}")
        sys.exit(1)
    
    if not Path(args.parser_script).exists():
        print(f"Error: Parser script not found: {args.parser_script}")
        sys.exit(1)
    
    # 執行驗證
    validator = ParserValidator(args.parser_script)
    validator.generate_report(args.cap_file, args.json_file)


if __name__ == "__main__":
    main()