# log_timing.py
import time
import functools
import threading
from collections import defaultdict

log_lock = threading.Lock()
profile_stats = defaultdict(lambda: {
    "calls": 0,
    "total_time": 0.0,
    "max_time": 0.0,
    "min_time": float('inf'),
    "total_input_len": 0,
    "max_input_len": 0,
    "file_sizes": []
})

# LOG_FILE = "/home/gustave/test1.txt"

def log_timing(track_input_length=True):
    """
    
    Args:
        track_input_length (bool):
    """
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            start_time = time.perf_counter()
            result = func(*args, **kwargs)
            elapsed_time = time.perf_counter() - start_time

            input_length = 0
            max_single_input = 0
            
            if track_input_length:
                for a in args:
                    if isinstance(a, str):
                        input_length += len(a)
                        max_single_input = max(max_single_input, len(a))
                
                for v in kwargs.values():
                    if isinstance(v, str):
                        input_length += len(v)
                        max_single_input = max(max_single_input, len(v))

            func_name = func.__qualname__

            with log_lock:
                stats = profile_stats[func_name]
                stats["calls"] += 1
                stats["total_time"] += elapsed_time
                stats["max_time"] = max(stats["max_time"], elapsed_time)
                stats["min_time"] = min(stats["min_time"], elapsed_time)
                
                if track_input_length:
                    stats["total_input_len"] += input_length
                    stats["max_input_len"] = max(stats["max_input_len"], max_single_input)
                    if max_single_input > 0:
                        stats["file_sizes"].append(max_single_input)

                if stats["calls"] % 100 == 0 or elapsed_time > 1.0:
                    write_profile_stats()

            return result
        return wrapper
    return decorator

def write_profile_stats():
    try:
        with open(LOG_FILE, "w") as f:
            sorted_stats = sorted(profile_stats.items(), 
                                key=lambda x: x[1]["total_time"], reverse=True)
            
            f.write("=" * 50 + "\n")
            f.write("PERFORMANCE ANALYSIS REPORT\n")
            f.write("=" * 50 + "\n\n")
            
            total_overall_time = sum(stat["total_time"] for stat in profile_stats.values())
            
            for name, stat in sorted_stats:
                calls = stat["calls"]
                total_time = stat["total_time"]
                max_time = stat["max_time"]
                min_time = stat["min_time"] if stat["min_time"] != float('inf') else 0
                
                avg_time = total_time / calls if calls else 0
                time_percentage = (total_time / total_overall_time * 100) if total_overall_time else 0
                
                f.write(f"[{name}]\n")
                f.write(f"  Calls: {calls:,}\n")
                f.write(f"  Total Time: {total_time:.6f}s ({time_percentage:.1f}%)\n")
                f.write(f"  Avg Time: {avg_time:.6f}s\n")
                f.write(f"  Min Time: {min_time:.6f}s\n")
                f.write(f"  Max Time: {max_time:.6f}s\n")
                
                if stat["total_input_len"] > 0:
                    avg_len = stat["total_input_len"] / calls
                    f.write(f"  Total Input Length: {stat['total_input_len']:,} chars\n")
                    f.write(f"  Avg Input Length: {avg_len:.2f} chars\n")
                    f.write(f"  Max Single Input: {stat['max_input_len']:,} chars\n")
                    
                    if stat["file_sizes"]:
                        file_sizes = sorted(stat["file_sizes"])
                        f.write(f"  File Size Distribution:\n")
                        f.write(f"    Small files (<100KB): {sum(1 for s in file_sizes if s < 100000)}\n")
                        f.write(f"    Medium files (100KB-1MB): {sum(1 for s in file_sizes if 100000 <= s < 1000000)}\n")
                        f.write(f"    Large files (>1MB): {sum(1 for s in file_sizes if s >= 1000000)}\n")
                
                f.write("\n")
            f.write(f"Total Overall Execution Time: {total_overall_time:.6f}s\n")
            
    except Exception as e:
        print(f"[log_timing] Failed to write log: {e}")

def dump_stats():
    with log_lock:
        write_profile_stats()

def clear_stats():
    with log_lock:
        profile_stats.clear()