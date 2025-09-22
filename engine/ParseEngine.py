"""
Batch parsing class for processing multiple log files.
"""

import os
import json
import logging
import threading
from concurrent.futures import ThreadPoolExecutor
from typing import Dict, List, Optional, Tuple
from .ParseEngineData import ParseEngineData

class ParseEngine:
    """
    Batch parser that processes multiple log files using ParseEngineData.
    Handles parallel processing and CSV output.
    """
    def __init__(self):
        """
        Initialize batch parser with empty statistics.
        """
        self.stats = {
            'total_files': 0,
            'successful_parses': 0,
            'failed_parses': 0
        }
        self.logger = logging.getLogger(__name__)
        self._csv_lock = threading.Lock()

    def find_all_colunms(self, json_rules: dict) -> List[str]:
        """
        Extract column definitions from JSON settings.

        Args:
            json_rules (dict): JSON configuration containing column definitions

        Returns:
            List[str]: List of column names
        """
        columns = []
        for key in json_rules.keys():
            if key not in ['source', 'type', 'begPairs', 'endPairs', 'actions', 'parseInTheEnd']:
                columns.append(key)
        return columns

    def write_to_csv_title(self, fp, columns: List[str]):
        """
        Write CSV header row.

        Args:
            fp: File pointer to write to
            columns (List[str]): List of column names
        """
        with self._csv_lock:
            fp.write(','.join(columns) + '\n')

    def write_to_csv_data(self, fp, data: Dict[str, any], columns: List[str]):
        """
        Write parsed data as CSV row.

        Args:
            fp: File pointer to write to
            data (Dict[str, Any]): Parsed data dictionary
            columns (List[str]): List of column names
        """
        with self._csv_lock:
            row = []
            for col in columns:
                value = data.get(col, '')
                if isinstance(value, (int, float)):
                    row.append(str(value))
                elif isinstance(value, str):
                    row.append(f'"{value}"')
                else:
                    row.append(str(value))
            fp.write(','.join(row) + '\n')

    def parse_single_file(self, file_path: str, json_rules: dict) -> Tuple[bool, Optional[ParseEngineData]]:
        """
        Parse a single log file.

        Args:
            file_path (str): Path to the log file
            json_rules (dict): Parsing rules in JSON format

        Returns:
            Tuple[bool, Optional[ParseEngineData]]: Success status and parser instance
        """
        try:
            parser = ParseEngineData()
            success = parser.parse_engine(file_path, json_rules)
            return success, parser if success else None
        except Exception as e:
            self.logger.error(f"Error processing {file_path}: {str(e)}")
            return False, None

    def parse_folder_to_csv(self, folder_path: str, json_rules: dict, output_csv: str):
        """
        Parse all .cap files in folder and subfolders and write results to CSV.

        Args:
            folder_path (str): Path to folder containing log files
            json_rules (dict): Parsing rules in JSON format
            output_csv (str): Path to output CSV file
        """
        # Collect all .cap files recursively
        cap_files = []
        for root, _, files in os.walk(folder_path):
            for file in files:
                if file.endswith('.cap'):
                    cap_files.append(os.path.join(root, file))
                    
        self.stats['total_files'] = len(cap_files)
        
        if not cap_files:
            self.logger.warning(f"No .cap files found in {folder_path} or its subfolders")
            return self.stats

        # Determine number of workers based on CPU count
        num_workers = min(32, os.cpu_count() * 2)
        self.logger.info(f"Found {len(cap_files)} files. Using {num_workers} workers for parallel processing")

        # Get column definitions
        columns = self.find_all_colunms(json_rules)

        # Process files in parallel
        with open(output_csv, 'w', encoding='utf-8') as fp:
            self.write_to_csv_title(fp, columns)
            
            with ThreadPoolExecutor(max_workers=num_workers) as executor:
                futures = []
                for file_path in cap_files:
                    future = executor.submit(self.parse_single_file, file_path, json_rules)
                    futures.append(future)

                # Process results as they complete
                for i, future in enumerate(futures):
                    try:
                        success, parser = future.result()
                        if success and parser:
                            self.write_to_csv_data(fp, parser.var_mgn, columns)
                            self.stats['successful_parses'] += 1
                        else:
                            self.stats['failed_parses'] += 1

                        # Log progress every 100 files
                        if (i + 1) % 100 == 0 or (i + 1) == len(cap_files):
                            progress = ((i + 1) / len(cap_files)) * 100
                            self.logger.info(
                                f"Progress: {i + 1}/{len(cap_files)} ({progress:.1f}%) files processed. "
                                f"Success: {self.stats['successful_parses']}, "
                                f"Failed: {self.stats['failed_parses']}"
                            )
                    except Exception as e:
                        self.logger.error(f"Error processing result: {str(e)}")
                        self.stats['failed_parses'] += 1

        # Log final statistics
        self.logger.info(
            f"Processing complete. Total: {self.stats['total_files']}, "
            f"Success: {self.stats['successful_parses']}, "
            f"Failed: {self.stats['failed_parses']}"
        )
        
        return self.stats 