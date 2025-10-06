# ParseEngine

[From Accton Technology Corporation]

ParseEngine is a Python tool for parsing `.cap` log files. It uses JSON-based rules for flexible configuration and can process many files at the same time using parallel processing.

## Key Features

- Flexible parsing rules using JSON config
- Searches all subfolders for `.cap` files
- Multi-threaded processing for better speed
- Outputs results to console or CSV file
- Detailed error logging
- Configurable logging levels
- Command-line interface for easy use

## Project Structure

```
ParseEngine/
├── engine/                # Engine module
│   ├── __init__.py        # Package init file
│   ├── ParseEngine.py     # Batch processing engine
│   ├── ParseEngineData.py # Single file parser
│   └── utils.py           # Helper functions
├── main.py               # Main program entry
├── sku_setting/          # JSON rules folder
│   └── k2v5_jrd03r_pt.json # Parsing rules file
└── README.md             # Documentation
```

## Requirements

- Python 3.6 or newer
- No extra packages needed

## How to Use

### 1. Parse Single File

```python
from engine import ParseEngine, configure_logging
import json
import logging

# Set up logging
configure_logging(enable_logging=True, level=logging.INFO)

# Create parser
parser = ParseEngineData()

# Load JSON rules
with open('sku_setting/k2v5_jrd03r_pt.json', 'r', encoding='UTF-8') as f:
    json_rules = json.load(f)

# Parse one file
success = parser.parse_engine('path/to/your/file.cap', json_rules)
if success:
    for key, value in parser.items():
        print(f'{key}: {value}')
```

### 2. Parse Multiple Files in Folder

```python
from engine import ParseEngine, configure_logging
import json
import logging
import os

# Set up logging
configure_logging(enable_logging=True, level=logging.INFO)

# Create engine
engine = ParseEngine()

# Load JSON rules
with open('sku_setting/k2v5_jrd03r_pt.json', 'r', encoding='UTF-8') as f:
    json_rules = json.load(f)

# Get list of files to parse
files_to_parse = []
for root, _, files in os.walk('path/to/your/folder'):
    for file in files:
        if file.endswith('.cap'):
            files_to_parse.append(os.path.join(root, file))

# Parse files and print results to stdout
for file_path in files_to_parse:
    success, parser = engine.parse_single_file(file_path, json_rules)
    if success and parser:
        print(f"\nSuccessfully parsed: {file_path}")
        print("Parsed data:")
        for key, value in parser.var_mgn.items():
            print(f"  {key}: {value}")
    else:
        print(f"\nFailed to parse: {file_path}")
```

### 3. Run Using Command Line

The main.py provides several command-line options for parsing files:

```bash
# Parse all .cap files in a directory and display results
python main.py parse_files <directory_path> <rule_path>

# Parse a single file and display results
python main.py parse_single_file <file_path> <rule_path>

# Parse all .cap files in a folder (including subfolders) and save to CSV
python main.py parse_to_csv <folder_path> <rule_path> <output_csv>

# Parse all folders specified in a JSON config
python main.py parse_all_folders <json_path>

# Parse files from the last N days
python main.py parse_recent_files <json_path>
```

#### Examples:

```bash
# Parse all .cap files in a directory and display results
python main.py parse_files "path/to/folder" "sku_setting/k2v5_jrd03r_pt.json"

# Parse a single file and display results
python main.py parse_single_file "path/to/file.cap" "sku_setting/k2v5_jrd03r_pt.json"

# Parse all .cap files in a folder and save results to CSV
python main.py parse_to_csv "path/to/folder" "sku_setting/k2v5_jrd03r_pt.json" "output/results.csv"

# Parse files and save to a specific location
python main.py parse_to_csv "./data/cap_files" "./rules/parsing_rule.json" "./reports/parsed_data.csv"
```

### 4. CSV Output Features

The `parse_to_csv` command provides powerful CSV export capabilities:

- **Recursive Processing**: Automatically scans all subfolders for `.cap` files
- **Multi-threaded**: Uses parallel processing for faster execution
- **Progress Tracking**: Shows real-time progress every 100 files
- **Thread-safe Writing**: Ensures data integrity when writing to CSV
- **Automatic Directory Creation**: Creates output directories if they don't exist
- **Comprehensive Statistics**: Reports total files, successful parses, and failures

#### CSV File Format

The generated CSV file contains:
- All parsed variables as columns (based on your JSON rules)
- Each row represents one parsed `.cap` file
- Column headers are automatically generated from your JSON configuration
- Numeric values are properly formatted
- String values are quoted for CSV compatibility

#### Usage Tips for CSV Output

1. **Large Datasets**: For processing thousands of files, the CSV output is much more efficient than console output
2. **Data Analysis**: CSV files can be easily imported into Excel, pandas, or other analysis tools
3. **Batch Processing**: Perfect for automated workflows and scheduled tasks
4. **Quality Control**: Review parsing success rates in the final statistics

## JSON Rules Format

The JSON configuration file is the core of ParseEngine. It defines how to extract and transform data from your `.cap` files.

### Basic Structure

```json
{
    "field_name": {
        "source": "scontext",
        "type": "str",
        "begPairs": ["start1", "start2"],
        "endPairs": ["end1", "end2"],
        "begIndexs": [1,1],
        "actions": {
            "action_name": {
                "parameters": "values"
            }
        },
        "parseInTheEnd": false
    }
}
```

### Field Descriptions

1. `source` (Required)
   - Usually "scontext" (the raw file content)
   - Can reference other parsed fields
   - Example: `"source": "scontext"`

2. `type` (Required)
   - Supported types: "str", "int", "float", "datetime"
   - For datetime, uses format: "%Y/%m/%d %H:%M:%S"
   - Example: `"type": "int"`

3. `begPairs` and `endPairs` (Required)
   - Lists of start and end markers that work in sequence
   - Each element in begPairs/endPairs can be either:
     - A single string: `"begPairs": ["START"]`
     - A list of strings: `"endPairs": [[" ", ","]]` (matches first occurrence of either marker)
   - Each pair progressively narrows down the text by clipping content between its markers
   - The parsing process works like this:
     1. First pair extracts text between its start and end markers
     2. Next pair searches within the previously extracted text
     3. This continues until all pairs are processed
   - This sequential clipping is useful when:
     - The target content appears multiple times in the file
     - You need to narrow down the context step by step
   - Example:
     ```json
     {
         "field_name": {
             "begPairs": ["TEST_SECTION", "RESULT:", "Value:"],
             "endPairs": ["END_SECTION", "\n", ","],
             "begIndexs": [1, 1, 1]
         }
     }
     ```
   - For a log file containing:
     ```
     TEST_SECTION
       RESULT: Test1
         Value: 123,
         Value: 456,
       RESULT: Test2
         Value: 789,
     END_SECTION
     ```
   - The parsing process would be:
     1. First pair ("TEST_SECTION" to "END_SECTION") extracts:
        ```
        RESULT: Test1
          Value: 123,
          Value: 456,
        RESULT: Test2
          Value: 789,
        ```
     2. Second pair ("RESULT:" to "\n") narrows it to:
        ```
        Test1
          Value: 123,
          Value: 456,
        ```
     3. Final pair ("Value:" to ",") gets: `123`

   This sequential clipping ensures you get the correct value even when similar patterns appear multiple times in the log.

4. `begIndexs` (Optional)
   - Array of integers specifying which occurrence to use for each start marker
   - Must have the same length as begPairs
   - Each number corresponds to the pair at the same position
   - Example:
     ```json
     {
         "field_name": {
             "begPairs": ["Error:", "Warning:"],
             "endPairs": ["\n", "\n"],
             "begIndexs": [2, 1]
         }
     }
     ```
   - In this example:
     - For "Error:" pair: looks for the 2nd occurrence
     - For "Warning:" pair: looks for the 1st occurrence
   - Default is [1] for all pairs if not specified

5. `actions` (Optional)
   - Transform extracted data
   - Supported actions:
     - `split`: Split text and take specific part
     - `trueFalse`: Convert to boolean by comparison
   - Example:
     ```json
     "actions": {
         "split": {
             "sep": ",",
             "index": 1
         },
         "trueFalse": {
             "compare": "PASS"
         }
     }
     ```

6. `parseInTheEnd` (Optional)
   - Boolean flag for parsing order
   - Set true if field depends on other parsed fields
   - Default is false
   - Example: `"parseInTheEnd": true`

### Example Configuration

Here's a complete example that shows different parsing scenarios:

```json
{
    "version": {
        "source": "scontext",
        "type": "str",
        "begPairs": ["Version:", "Ver:"],
        "endPairs": ["\n", ","]
    },
    "timestamp": {
        "source": "scontext",
        "type": "datetime",
        "begPairs": ["Time: "],
        "endPairs": ["\n"]
    },
    "status": {
        "source": "scontext",
        "type": "str",
        "begPairs": [["Status:", "status="]],
        "endPairs": ["\n"],
        "actions": {
            "trueFalse": {
                "compare": "PASS"
            }
        }
    },
    "error_code": {
        "source": "scontext",
        "type": "int",
        "begPairs": ["Error:", "Code:"],
        "endPairs": ["\n", ","],
        "begIndexs": [1, 1],
        "parseInTheEnd": true
    }
}
```

### Common Patterns

1. Sequential Clipping Example
   ```json
   {
       "error_details": {
           "begPairs": ["[ERROR]", "Details:", "Code:"],
           "endPairs": ["[END]", "Stack:", "\n"],
           "begIndexs": [1, 1, 1]
       }
   }
   ```
   - Each pair clips the content for the next pair
   - Useful for extracting nested information
   - Helps avoid matching wrong occurrences

2. Dependent Fields
   ```json
   "total_time": {
       "source": "scontext",
       "type": "int",
       "begPairs": ["Total Time:"],
       "endPairs": ["ms"],
       "parseInTheEnd": true
   }
   ```
   - Use `parseInTheEnd` when field depends on other parsed values
   - Ensures all required fields are parsed first

3. Data Transformations
   ```json
   "test_result": {
       "source": "scontext",
       "type": "str",
       "begPairs": ["Result:"],
       "endPairs": ["\n"],
       "actions": {
           "split": {
               "sep": ",",
               "index": 0
           },
           "trueFalse": {
               "compare": "PASS"
           }
       }
   }
   ```
   - Actions are applied after all pairs are processed
   - Multiple actions are executed in order

### Available Actions

The following actions can be used in the `actions` field to transform parsed values:

1. `split`
   - Splits a string by a separator and takes a specific index
   - Parameters:
     - `sep`: The separator string
     - `index`: The index of the split result to return
   - Example: `"split": {"sep": ",", "index": 0}`

2. `trueFalse`
   - Performs boolean comparisons
   - Parameters (use one of the following):
     - `compare`: Checks if source equals this value
     - `contains`: Checks if source contains this value
     - `contains_nocase`: Checks if source contains this value (case insensitive)
   - Example: `"trueFalse": {"compare": "PASS"}`

3. `findMaxPattern`
   - Finds the maximum value matching a pattern
   - Parameters:
     - `pattern`: Regular expression pattern to match
   - Example: `"findMaxPattern": {"pattern": "\\d+\\.\\d+"}`

4. `replace`
   - Replaces one substring with another
   - Parameters:
     - `from`: The substring to replace
     - `to`: The replacement substring
   - Example: `"replace": {"from": "_", "to": " "}`

5. `strip`
   - Removes leading and trailing whitespace
   - No parameters required
   - Example: `"strip": {}`

6. `delta`
   - Calculates the difference between two values
   - Parameters:
     - `from_keys`: Array of two keys from the parsed variables
   - Example: `"delta": {"from_keys": ["end_time", "start_time"]}`

### Tips for Writing Rules

1. Start Simple
   - Begin with a single pair to extract the general section
   - Add more pairs to narrow down to specific content
   - Test each step of the extraction process

2. Test Incrementally
   - Use logging to see intermediate results
   - Check what text is extracted after each pair
   - Verify the final output matches expectations

3. Consider Edge Cases
   - Handle missing sections with multiple start/end pairs
   - Consider text that might appear between your target sections
   - Test with different file formats and content variations

4. Optimize Performance
   - Use minimal pairs needed to extract data
   - Order pairs from larger sections to smaller details
   - Group related fields that use similar sections

## Features

1. Parallel Processing
   - Automatically uses the right number of threads
   - Shows progress in real-time
   - Handles multiple files simultaneously

2. Error Handling
   - Detailed error messages
   - Errors in one file don't stop others
   - Summary of results

3. Performance
   - Caches repeated searches
   - Processes files in batches
   - Efficient memory use

4. Flexible Output
   - Direct output to console for quick inspection
   - CSV export for data analysis and reporting
   - Thread-safe file writing for large datasets

5. CSV Output Benefits
   - Easy import into Excel, Python pandas, or databases
   - Structured data format for automated processing
   - Preserves all parsed variables in organized columns
   - Supports large-scale batch processing workflows

## Logging Options

Control logging with `configure_logging`:

```python
configure_logging(enable_logging=True, level=logging.DEBUG)  # All details
configure_logging(enable_logging=True, level=logging.INFO)   # Normal info
configure_logging(enable_logging=False)                      # No logging
```

## Results Summary

After processing, you get:

- Total number of files processed
- Number of successful parses
- Number of failed parses
- Total processing time
- Output location (for CSV exports)

## Important Notes

1. Make sure your JSON rules file is correct
2. Check disk space when processing large folders, especially for CSV output
3. Test with a few files first before processing many
4. For CSV output, ensure the output directory exists or the tool will create it
5. CSV files can become large with many files - monitor disk space

## Performance Tips

1. **For Console Output**: Use `parse_files` for quick inspection and debugging
2. **For Data Analysis**: Use `parse_to_csv` for large datasets and systematic analysis
3. **Testing Rules**: Start with `parse_single_file` to test your JSON configuration
4. **Large Datasets**: The CSV output is significantly faster for processing thousands of files

## License

MIT License
