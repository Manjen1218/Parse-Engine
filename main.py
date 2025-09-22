"""
Main entry point for the ParseEngine application.
Demonstrates both single file and folder parsing capabilities.
"""

import json
import logging
import os
import time
import sys
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
from engine import ParseEngine, configure_logging

def load_json(json_path: str) -> dict:
    """
    Load JSON file and return its content.
    
    Args:
        json_path (str): Path to the JSON file
        
    Returns:
        dict: Parsed JSON content
    """
    try:
        with open(json_path, 'r', encoding='UTF-8') as f:
            return json.load(f)
    except Exception as e:
        logging.error(f"Failed to load JSON file: {str(e)}")
        return {}

def parse_files(filelist: list, parsing_rules: dict):
    """
    Parse multiple files using the parsing rules and output results to stdout.
    
    Args:
        filelist (list): List of file paths to parse
        parsing_rules (dict): Dictionary of parsing rules
    """
    engine = ParseEngine()
    
    num_workers = min(32, os.cpu_count() * 2)
    logging.info(f"Using {num_workers} workers for parallel processing")

    # Process files in parallel
    with ThreadPoolExecutor(max_workers=num_workers) as executor:
        futures = []

        for file_path in filelist:
            future = executor.submit(engine.parse_single_file, file_path, parsing_rules)
            futures.append((future, file_path))  # Store the file_path with the future

        tbeg = time.time()
        # Process results as they complete
        for i, (future, file_path) in enumerate(futures):
            try:
                success, parser = future.result()
                if success and parser:
                    # Print parsed data to standard output
                    print(f"\nSuccessfully parsed: {file_path}")
                    print("Parsed data:")
                    for key, value in parser.var_mgn.items():
                        print(f"  {key}: {value}")
                    engine.stats['successful_parses'] += 1
                else:
                    print(f"\nFailed to parse: {file_path}")
                    if parser:
                        print(f"Error: {parser.error}")
                    else:
                        print("Parser returned None")
                    engine.stats['failed_parses'] += 1

                # Log progress every 100 files
                if not (i + 1) % 100:
                    progress = ((i + 1) / len(filelist)) * 100
                    logging.info(
                        f"Progress: {i + 1}/{len(filelist)} ({progress:.1f}%) files processed. "
                        f"Duration: {time.time() - tbeg:.2f} s, "
                        f"Parse Success: {engine.stats['successful_parses']}, "
                        f"Parse Failed: {engine.stats['failed_parses']}"
                    )
            except Exception as e:
                logging.error(f"Error processing result: {str(e)}")
                print(f"\nError processing {file_path}: {str(e)}")
                engine.stats['failed_parses'] += 1
                
    # Log final statistics
    logging.info(
        f"Processing complete. Duration: {time.time() - tbeg:.2f} s, "
        f"Parse Success: {engine.stats['successful_parses']}, "
        f"Parse Failed: {engine.stats['failed_parses']}"
    )

def parse_single_file(file_path: str, json_rule_path: str):
    """
    Parse a single file using the specified rule and output results.
    
    Args:
        file_path (str): Path to the file to parse
        json_rule_path (str): Path to the JSON rule file
    """
    # Load JSON rules
    parsing_rules = load_json(json_rule_path)
    
    engine = ParseEngine()
    success, parser = engine.parse_single_file(file_path, parsing_rules)
    
    if success and parser:
        print(f"\nSuccessfully parsed: {file_path}")
        print("Parsed data:")
        for key, value in parser.var_mgn.items():
            print(f"  {key}: {value}")
    else:
        print(f"\nFailed to parse: {file_path}")
        if parser:
            print(f"Error: {parser.error}")
        else:
            print("Parser returned None")

def parse_files_in_directory(directory_path: str, rule_path: str):
    """
    Parse all .cap files in a directory using the specified rule file.
    
    Args:
        directory_path (str): Path to the directory containing .cap files
        rule_path (str): Path to the JSON rule file
    """
    # Load JSON rules
    parsing_rules = load_json(rule_path)
    if not parsing_rules:
        logging.error(f"Failed to load parsing rules from {rule_path}")
        return
    
    # Get all .cap files in the directory
    cap_files = []
    if os.path.isdir(directory_path):
        for file in os.listdir(directory_path):
            if file.endswith('.cap'):
                cap_files.append(os.path.join(directory_path, file))
    else:
        logging.error(f"{directory_path} is not a valid directory")
        return
    
    if not cap_files:
        logging.info(f"No .cap files found in {directory_path}")
        return
    
    logging.info(f"Found {len(cap_files)} .cap files in {directory_path}")
    
    # Parse the files
    parse_files(cap_files, parsing_rules)

def parse_folder_to_csv(folder_path: str, rule_path: str, output_csv: str):
    """
    Parse all .cap files in a folder (including subfolders) and save results to CSV.
    
    Args:
        folder_path (str): Path to the folder containing .cap files
        rule_path (str): Path to the JSON rule file
        output_csv (str): Path to the output CSV file
    """
    # Load JSON rules
    parsing_rules = load_json(rule_path)
    if not parsing_rules:
        logging.error(f"Failed to load parsing rules from {rule_path}")
        return
    
    output_dir = os.path.dirname(output_csv)
    if output_dir and not os.path.exists(output_dir):
        os.makedirs(output_dir)
        logging.info(f"Created output directory: {output_dir}")
    
    engine = ParseEngine()
    
    print(f"Starting to parse folder: {folder_path}")
    print(f"Using rule file: {rule_path}")
    print(f"Output CSV: {output_csv}")
    
    # output result to CSV
    stats = engine.parse_folder_to_csv(folder_path, parsing_rules, output_csv)
    
    # Statistics
    print(f"\nParsing completed!")
    print(f"Total files processed: {stats['total_files']}")
    print(f"Successfully parsed: {stats['successful_parses']}")
    print(f"Failed to parse: {stats['failed_parses']}")
    print(f"Results saved to: {output_csv}")

def main():
    """
    Main function to run the ParseEngine application.
    """
    # Configure logging with standard format and INFO level
    configure_logging(enable_logging=True, level=logging.INFO)
    
    if len(sys.argv) < 2:
        print("Usage:")
        print("  python main.py parse_all_folders <json_path>")
        print("  python main.py parse_recent_files <json_path>")
        print("  python main.py parse_single_file <file_path> <rule_path>")
        print("  python main.py parse_files <directory_path> <rule_path>")
        print("  python main.py parse_to_csv <folder_path> <rule_path> <output_csv>")
        return
        
    command = sys.argv[1]
    
    if len(sys.argv) < 2:
        print("Usage:")
        print("  python main.py parse_all_folders <json_path>")
        print("  python main.py parse_recent_files <json_path>")
        print("  python main.py parse_single_file <file_path> <rule_path>")
        print("  python main.py parse_files <directory_path> <rule_path>")
        print("  python main.py parse_to_csv <folder_path> <rule_path> <output_csv>")
        return
        
    command = sys.argv[1]
    
    if command == "parse_all_folders" and len(sys.argv) >= 3:
        # Parse all folders
        json_path = sys.argv[2]
        parse_all_folders(json_path)
    elif command == "parse_recent_files" and len(sys.argv) >= 3:
        # Parse folder with day-based filtering
        json_path = sys.argv[2]
        parse_recent_files(json_path)
    elif command == "parse_single_file" and len(sys.argv) >= 4:
        # Parse a single file
        file_path = sys.argv[2]
        rule_path = sys.argv[3]
        parse_single_file(file_path, rule_path)
    elif command == "parse_files" and len(sys.argv) >= 4:
        # Parse all .cap files in a directory
        directory_path = sys.argv[2]
        rule_path = sys.argv[3]
        parse_files_in_directory(directory_path, rule_path)
    elif command == "parse_to_csv" and len(sys.argv) >= 5:
        # Parse folder to CSV
        folder_path = sys.argv[2]
        rule_path = sys.argv[3]
        output_csv = sys.argv[4]
        parse_folder_to_csv(folder_path, rule_path, output_csv)
    else:
        print("Invalid command or missing arguments")
        print("\nFor CSV output, use:")
        print("  python main.py parse_to_csv <folder_path> <rule_path> <output_csv>")


if __name__ == '__main__':
    main()