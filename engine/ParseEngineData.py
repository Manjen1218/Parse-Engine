"""
Core parsing class for individual log files with selective parsing capability.
"""

import os
import logging
import re
from datetime import datetime, timezone, timedelta
from functools import lru_cache
from typing import Any, Dict, List, Tuple, Union, Optional
import xxhash 
from io import StringIO
import csv

class ParseEngineData:
    """
    Core parsing class that handles individual log file parsing.
    Extracts data based on predefined rules and maintains parsed variables.
    Supports selective parsing to only process specified fields.
    """
    def __init__(self, selective_fields: Optional[List[str]] = None):
        """
        Initialize parser with empty values for core variables.
        
        Args:
            selective_fields (Optional[List[str]]): List of field names to parse.
                                                  If None or empty, parse all fields.
        """
        self.fullpath = ''
        self.bname = ''
        self.is_y = True
        self.total_td = 0
        self.var_mgn = dict()
        self.selective_fields = set(selective_fields) if selective_fields else None
        self.logger = logging.getLogger(__name__)

    def __getitem__(self, key):
        """
        Allow accessing parsed variables using object['var'] syntax.

        Args:
            key (str): Variable name to access

        Returns:
            Any: Value of the variable or None if not found
        """
        return self.var_mgn.get(key, None)

    def __contains__(self, key):
        """
        Allow checking if a key exists using 'key in object' syntax.

        Args:
            key (str): Variable name to check

        Returns:
            bool: True if variable exists, False otherwise
        """
        return key in self.var_mgn

    def items(self):
        """
        Allow iterating over parsed variables.

        Returns:
            dict_items: Iterator over parsed variable key-value pairs
        """
        return self.var_mgn.items()

    def set_selective_fields(self, fields: Optional[List[str]]) -> None:
        """
        Set the fields to be selectively parsed.
        
        Args:
            fields (Optional[List[str]]): List of field names to parse.
                                        If None or empty, parse all fields.
        """
        self.selective_fields = set(fields) if fields else None

    def should_parse_field(self, field_name: str) -> bool:
        """
        Check if a field should be parsed based on selective parsing settings.
        
        Args:
            field_name (str): Name of the field to check
            
        Returns:
            bool: True if field should be parsed, False otherwise
        """
        # Always parse these critical fields regardless of selective_fields
        critical_fields = {'tbeg', 'tend', 'wo', 'scontext'}
        
        if field_name in critical_fields:
            return True
            
        # If no selective fields specified, parse everything
        if self.selective_fields is None:
            return True
            
        # Check if field is in the selective list
        return field_name in self.selective_fields

    def get_dependency_fields(self, json_rules: dict, target_fields: set) -> set:
        """
        Get all dependency fields needed to parse the target fields.
        
        Args:
            json_rules (dict): JSON rules configuration
            target_fields (set): Set of target field names
            
        Returns:
            set: Set of all required fields including dependencies
        """
        required_fields = set(target_fields)
        added_new = True
        
        # Iteratively find dependencies until no new fields are added
        while added_new:
            added_new = False
            current_size = len(required_fields)
            
            for field in list(required_fields):
                if field in json_rules:
                    rules = json_rules[field]
                    
                    # Check source field dependency
                    source = rules.get('source', '')
                    if source and source != field and source in json_rules:
                        required_fields.add(source)
                    
                    # Check action dependencies
                    if 'actions' in rules:
                        for action, pars in rules['actions'].items():
                            if action == 'delta' and isinstance(pars, dict) and 'from_keys' in pars:
                                required_fields.update(pars['from_keys'])
                            elif action in ['merge', 'r_merge'] and 'with_var' in pars:
                                required_fields.add(pars['with_var'])
            
            if len(required_fields) > current_size:
                added_new = True
        
        return required_fields

    @lru_cache(maxsize=1000)
    def find_nth(self, haystack: str, needle: str, n: int) -> int:
        """
        Find the nth occurrence of needle in haystack with caching for improved performance.

        Args:
            haystack (str): String to search in
            needle (str or list): String or list of strings to search for
            n (int): Occurrence number to find (-1 for last occurrence)

        Returns:
            int: Position of the nth occurrence, -1 if not found
        """
        # Preprocessing To avoid inefficiency operate.
        if not haystack or not needle or n == 0:
            return -1
        
        if isinstance(needle, list):
            # Only count the exist position.
            valid_positions = {}
            for nd in needle:
                if nd:  # Protect Program, Make sure needle not empty
                    pos = haystack.find(nd)
                    if pos != -1:
                        valid_positions[nd] = pos
            
            if not valid_positions:
                return -1
            
            # find the minimum point of needle
            needle = min(valid_positions, key=valid_positions.get)

        # process the last case
        if n == -1:
            return haystack.rfind(needle)

        # reduce some search, return faster
        pos = -1
        for _ in range(n):
            pos = haystack.find(needle, pos + 1)
            if pos == -1:
                return -1  # exit in advance
        return pos

    def cutStrWith2Str(self, source: str, begStr: str, endStr: str, n = 1):
        """
        Extract substring between two delimiter strings.

        Args:
            source (str): Source string to extract from
            begStr (str or list): Beginning delimiter(s)
            endStr (str or list): Ending delimiter(s)
            n (int): Occurrence number for beginning delimiter

        Returns:
            str: Extracted substring or None if not found
        """

        if not source:
            return None

        if isinstance(begStr, list) or isinstance(endStr, list):
            begStrs = begStr if isinstance(begStr, list) else [begStr]
            endStrs = endStr if isinstance(endStr, list) else [endStr]
            
            begPos, actual_begStr = self.findSmallestPositionAmongSustrings(begStrs, source)
            if begPos == -1:
                return None
            
            start_search_pos = begPos + len(actual_begStr)
            endPos, actual_endStr = self.findSmallestPositionAmongSustrings(endStrs, source[start_search_pos:])
            if endPos == -1:
                return None
            
            actual_endPos = start_search_pos + endPos
            return source[start_search_pos:actual_endPos]
        
        begPos = self.find_nth(source, begStr, n)
        if begPos == -1:
            return None
        
        start_pos = begPos + len(begStr)
        endPos = source.find(endStr, start_pos)
        
        if endPos == -1:
            return None
        
        return source[start_pos:endPos]
        
    def convert_to_utc(self, dt_str):
        """
        Convert timezone to UTC, default is UTC+8 to UTC.

        Args:
            dt_str: Timestamp to convert.
        
        Returns:
            Any: converted value of timestamp.
        """
        local_dt = datetime.strptime(dt_str, "%Y-%m-%d %H:%M:%S")
        # Set local timezone as +08:00
        local_dt = local_dt.replace(tzinfo=timezone(timedelta(hours=8)))
        utc_dt = local_dt.astimezone(timezone.utc)
        return utc_dt.strftime("%Y-%m-%d %H:%M:%S")

    def dataTypeTransform(self, source, typeTo: str, sourceName: str):
        """
        Convert string data to specified type.

        Args:
            source: Value to convert
            typeTo (str): Target type name ('str', 'int', 'datetime', etc.)
            sourceName (str): Name of the variable (for error reporting)

        Returns:
            Any: Converted value or None if conversion fails
        """
        try:
            if isinstance(source, eval(typeTo)):
                return source
            if typeTo == 'datetime':
                if isinstance(source, str):
                    if source.strip().upper() in ('NA', 'N/A', ''):
                        return None
                    # Try known formats with ',' as separator
                    for fmt_in in ("%Y/%m/%d %H:%M:%S", "%Y-%m-%d,%H:%M:%S"):
                        try:
                            dt = datetime.strptime(source, fmt_in)
                            # Convert to standard format for further processing
                            source = dt.strftime("%Y-%m-%d %H:%M:%S")
                            break
                        except Exception:
                            continue
                    else:
                        return None
                if isinstance(source, datetime):
                    result = source
                else:
                    result = datetime.strptime(source, "%Y-%m-%d %H:%M:%S")
                return result.strftime("%Y-%m-%d %H:%M:%S")
            else:
                result = eval(typeTo)(source)
            return result
        except Exception as e:
            print(f"[Warning] Failed to convert {sourceName} to {typeTo}: {source}")
            return None

    def findSmallestPositionAmongSustrings(self, substrings, s):
        """
        Find the leftmost occurrence among multiple substrings.

        Args:
            substrings (list): List of strings to search for
            s (str): String to search in

        Returns:
            tuple: (position, found_substring) or (-1, None) if not found
        """
        if not substrings or not s:
            return (-1, None)
        
        min_pos = float('inf')
        found_substring = None
        
        for sub in substrings:
            if not sub:
                continue
                
            pos = s.find(sub)
            if pos != -1 and pos < min_pos:
                min_pos = pos
                found_substring = sub
                if pos == 0:
                    break
        
        return (int(min_pos), found_substring) if found_substring else (-1, None)
        
    def isParseInTheEnd(self, rules:dict) -> bool:
        """
        Check if item should be parsed after all others.

        Args:
            rules (dict): Parsing rules for the item

        Returns:
            bool: True if should parse at end, False otherwise
        """
        if 'parseInTheEnd' in rules and rules['parseInTheEnd']:
            return True
        return False

    def rulesAugmentation(self, rules):
        """
        Add necessary default values to parsing rules.

        Args:
            rules (dict): Original parsing rules

        Returns:
            dict: Augmented rules with defaults
        """
        if not 'begIndexs' in rules:
            rules['begIndexs'] = [1 for i in rules['begPairs']]
        return rules

    def doActions(self, source, actions:dict):
        """
        Apply transformation actions to parsed value.

        Args:
            source: Value to transform
            actions (dict): Dictionary of actions to apply

        Returns:
            Any: Transformed value
        """
        for action, pars in actions.items():
            if action == 'split':
                source = source.split(pars["sep"])[pars["index"]]
            elif action == 'trueFalse':
                if "compare" in pars:
                    source = source == pars["compare"]
                elif "contains" in pars:
                    source = pars["contains"] in source
                elif "contains_nocase" in pars:
                    source = pars["contains_nocase"].lower() in source.lower()    
            elif action == 'findMaxPattern':
                matches = re.findall(pars["pattern"], source)
                if matches:
                    numbers = [float(m) for m in matches]
                    source = max(numbers)
                else:
                    return None   
            elif action == 'findMinPattern':
                matches = re.findall(pars["pattern"], source)
                if matches:
                    numbers = [float(m) for m in matches]
                    source = min(numbers)
                else:
                    return None         
            elif action == 'replace':
                if source:
                    # single replace
                    if isinstance(pars, dict):
                        source = source.replace(pars["from"], pars["to"])
                    # multiple replace
                    elif isinstance(pars, list):
                        for r in pars:
                            source = source.replace(r["from"], r["to"])
            elif action == 'strip':
                source = source.strip()
            elif action == 'delta':
                if isinstance(pars, dict) and 'from_keys' in pars:
                    k1, k2 = pars['from_keys']
                    v1 = self.var_mgn.get(k1)
                    v2 = self.var_mgn.get(k2)
                    if v1 is not None and v2 is not None:
                        source = float(v1) - float(v2)
                    else:
                        return None
            elif action == 'merge':
                # Merge source with another value (from var_mgn or direct value)
                if "with_var" in pars:
                    other = self.var_mgn.get(pars["with_var"], "")
                elif "with_value" in pars:
                    other = pars["with_value"]
                else:
                    other = ""
                sep = pars.get("sep", "")
                source = f"{source}{sep}{other}"
            elif action == 'r_merge':
                if "with_var" in pars:
                    other = self.var_mgn.get(pars["with_var"], "")
                    other = other.split(' ')[0]
                elif "with_value" in pars:
                    other = pars["with_value"]
                else:
                    other = ""
                sep = pars.get("sep", "")
                source = f"{other}{sep}{source}"
            elif action == 'hash':
                if source is not None and self.var_mgn['wo'] is not None:
                    csv_reader = csv.reader(StringIO(source))
                    first_column_values = sorted([row[0] for row in csv_reader if row])
                    combined = ''.join(first_column_values)
                    combined = self.var_mgn['wo'] + combined
                    hash_value = xxhash.xxh32(combined).hexdigest()
                    source = hash_value
                else: 
                    source = None
        return source

    def parse_item(self, rules:dict, source:str, sourceName: str, fullpath: str):
        """
        Parse a single variable according to its rules.

        Args:
            rules (dict): Parsing rules for the variable
            source (str): Source string to parse
            sourceName (str): Name of the variable being parsed

        Returns:
            Any: Parsed and transformed value, or None if parsing fails
        """
        try:

            begPairs = rules['begPairs']
            endPairs = rules['endPairs']
            begIndexs = rules.get('begIndexs', [1] * len(begPairs))

            # Sequential processing: each pair progressively narrows down the text
            closingPairs = zip(begPairs, endPairs, begIndexs)
            source_out = source
            for begStr, endStr, n in closingPairs:
                if isinstance(begStr, list) and isinstance(endStr, list):
                    source = self.cutStrWith2Str(source_out, begStr, endStr, n)
                    if source is not None:
                        break
                else:
                    source = self.cutStrWith2Str(source, begStr, endStr, n)
                    source_out = source
                    if not source:
                        self.logger.debug(f"Failed to extract {sourceName} using delimiters: beg='{begStr}', end='{endStr}'")
                        return None

            # Parse err_msg
            if sourceName == "err_msg" and source is not None:
                source = source.strip()
                source = source.replace("FAIL", "").replace("Fail!!", "").replace("Fail", "")
                remove_index = source.find("correct :")
                if remove_index != -1:
                    source = source[:remove_index]
                
                last_comma_index = source.rfind(",")
                if len(source) > 190:
                    if last_comma_index != -1:
                        source = source[:last_comma_index] # cut all string after ",..."
                else:
                    if last_comma_index != -1 and last_comma_index == (len(source) - 1):
                        source = source[:last_comma_index] # cut "," at the end


            if 'actions' in rules:
                source = self.doActions(source, rules['actions'])
                if source is None:
                    return None

            result = self.dataTypeTransform(source, rules['type'], sourceName)
            if result is None:
                self.logger.debug(f"Failed to transform {sourceName} to type {rules['type']}")
            return result
        except Exception as e:
            self.logger.error(f"Error parsing {sourceName}: {str(e)} , fullpath : {fullpath}")
            return None

    def validate_json_rules(self, rules: dict) -> bool:
        """
        Validate the format of JSON configuration file.

        Args:
            rules (dict): JSON rules configuration

        Returns:
            bool: Whether the configuration is valid
        """
        required_fields = {'source', 'type', 'begPairs', 'endPairs'}
        for var, rule in rules.items():
            if not all(field in rule for field in required_fields):
                self.logger.error(f"Missing required fields in rule for {var}")
                return False
        return True

    def validate_final_results(self) -> bool:
        """
        Final validation to ensure all required fields exist and are valid.
        
        Returns:
            bool: True if all validations pass, False otherwise
        """
        required_fields = ['tbeg', 'tend', 'wo' ] # Define required fields
        
        # Check if required fields exist
        for field in required_fields:
            if field not in self.var_mgn:
                self.logger.error(f"Required field '{field}' is missing")
                return False
            
            # Check if values are valid (not None, not empty strings, etc.)
            value = self.var_mgn[field]
            if value is None or (isinstance(value, str) and value.strip() == ''):
                self.logger.error(f"Required field '{field}' has invalid value: {value}")
                return False
        
        # Check if total_td was calculated successfully
        if self.total_td is None or self.total_td < 0:
            self.logger.error(f"Invalid total_td: {self.total_td}")
            return False
        
        return True

    def parse_engine(self, fullpath: str, json_rules: dict) -> bool:
        """
        Main parsing function for a single log file with selective parsing support.

        Args:
            fullpath (str): Path to the log file
            json_rules (dict): Parsing rules in JSON format

        Returns:
            bool: True if parsing successful, False otherwise
        """
        if not os.path.isfile(fullpath):
            self.logger.error(f"File not found: {fullpath}")
            return False

        if not self.validate_json_rules(json_rules):
            self.logger.error("Invalid JSON rules configuration")
            return False

        # Read file content
        with open(fullpath, 'r', encoding='UTF-8') as f:
            scontext = f.read()
        self.var_mgn['scontext'] = scontext

        # Determine which fields to parse
        if self.selective_fields is not None:
            # Get all dependency fields needed for selective parsing
            required_fields = self.get_dependency_fields(json_rules, self.selective_fields)
            self.logger.debug(f"Selective parsing: target fields {self.selective_fields}, "
                            f"required fields including dependencies: {required_fields}")
        else:
            required_fields = None

        # Filter rules based on selective parsing
        filtered_rules = {}
        rules_ParseInTheEnd = []
        
        for var, rules in json_rules.items():
            # Skip fields that are not needed for selective parsing
            if required_fields is not None and var not in required_fields:
                continue
                
            rules = self.rulesAugmentation(rules)
            filtered_rules[var] = rules
            
            if self.isParseInTheEnd(rules):
                rules_ParseInTheEnd.append((var, rules))
            else:
                source = eval(rules['source']) if var == rules['source'] else self.var_mgn[rules['source']]
                parseResult = self.parse_item(
                    rules=rules, 
                    source=source,
                    sourceName = var,
                    fullpath=fullpath
                )
                if parseResult is not None:
                    self.var_mgn[var] = parseResult

        # Log parsing statistics for selective parsing
        if self.selective_fields is not None:
            total_rules = len(json_rules)
            parsed_rules = len(filtered_rules)
            self.logger.info(f"Selective parsing: processed {parsed_rules}/{total_rules} fields "
                           f"({(parsed_rules/total_rules*100):.1f}% reduction)")

        # Convert timezone for timestamp fields
        for key, value in self.var_mgn.items():
            if isinstance(value, str):
                try:
                    # Try to parse as timestamp in expected format
                    datetime.strptime(value, "%Y-%m-%d %H:%M:%S")
                    self.var_mgn[key] = self.convert_to_utc(value)
                except Exception:
                    pass

        # Calculate total time if both tbeg and tend are available
        tbeg = self.var_mgn.get('tbeg')
        tend = self.var_mgn.get('tend')
        
        total_td = None
        try:
            if tbeg is None or tend is None:
                if self.selective_fields is None:  # Only log error if not selective parsing
                    self.logger.error(f"[Error] tbeg or tend is missing")
            elif isinstance(tbeg, (int, float)) and isinstance(tend, (int, float)):
                total_td = int(tend - tbeg)
            elif isinstance(tbeg, str) and isinstance(tend, str):
                dt_beg = datetime.strptime(tbeg, "%Y-%m-%d %H:%M:%S")
                dt_end = datetime.strptime(tend, "%Y-%m-%d %H:%M:%S")
                total_td = int((dt_end - dt_beg).total_seconds())
            elif isinstance(tbeg, datetime) and isinstance(tend, datetime):
                total_td = int((tend - tbeg).total_seconds())
            else:
                if self.selective_fields is None:  # Only log error if not selective parsing
                    self.logger.error(f"Unsupported types for tbeg/tend: {type(tbeg)}, {type(tend)}")
        except Exception as e:
            if self.selective_fields is None:  # Only log error if not selective parsing
                self.logger.error(f"Failed to calculate total_td: {e}")
        self.total_td = total_td

        # Process parseInTheEnd rules
        for var, rules in rules_ParseInTheEnd:
            source = eval(rules['source']) if var == rules['source'] else self.var_mgn[rules['source']]
            
            self.var_mgn[var] = self.parse_item(
                rules=rules, 
                source=source,
                sourceName = var,
                fullpath = fullpath
            )

        # Delete fields marked with delete: true
        for var, rules in filtered_rules.items():
            if 'delete' in rules and rules['delete']:
                if var in self.var_mgn:
                    del self.var_mgn[var]
        
        # Clean up scontext
        if 'scontext' in self.var_mgn:
            del self.var_mgn['scontext']

        return True