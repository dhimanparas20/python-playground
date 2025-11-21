import functools
import time
from typing import Callable, Any

# Option 1: Using ANSI escape codes (no external dependencies)
class Colors:
    HEADER = '\033[95m'
    OKBLUE = '\033[94m'
    OKCYAN = '\033[96m'
    OKGREEN = '\033[92m'
    WARNING = '\033[93m'
    FAIL = '\033[91m'
    ENDC = '\033[0m'
    BOLD = '\033[1m'
    UNDERLINE = '\033[4m'

def timeit(func: Callable) -> Callable:
    """
    Decorator to measure execution time of a function.
    Uses time.perf_counter() for high precision timing.
    """
    @functools.wraps(func)
    def wrapper(*args: Any, **kwargs: Any) -> Any:
        start_time = time.perf_counter()
        result = func(*args, **kwargs)
        end_time = time.perf_counter()
        
        execution_time = end_time - start_time
        
        # Color-code based on execution time
        if execution_time < 0.1:
            color = Colors.OKGREEN
            emoji = "⚡"
        elif execution_time < 1.0:
            color = Colors.OKCYAN
            emoji = "✓"
        elif execution_time < 5.0:
            color = Colors.WARNING
            emoji = "⚠"
        else:
            color = Colors.FAIL
            emoji = "🐌"
        
        print(f"{color}{emoji} Function '{Colors.BOLD}{func.__name__}{Colors.ENDC}{color}' "
              f"took {Colors.BOLD}{execution_time:.6f}{Colors.ENDC}{color} seconds{Colors.ENDC}")
        
        return result
    
    return wrapper


# Option 2: Enhanced version with statistics
def timeit_stats(threshold: float = 1.0, unit: str = "auto"):
    """
    Enhanced decorator with configurable threshold and unit.
    
    Args:
        threshold: Time threshold for warnings (in seconds)
        unit: Time unit ('auto', 's', 'ms', 'μs')
    """
    def decorator(func: Callable) -> Callable:
        @functools.wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            start_time = time.perf_counter()
            result = func(*args, **kwargs)
            end_time = time.perf_counter()
            
            execution_time = end_time - start_time
            
            # Auto-select appropriate unit
            if unit == "auto":
                if execution_time < 0.001:
                    time_value = execution_time * 1_000_000
                    time_unit = "μs"
                elif execution_time < 1.0:
                    time_value = execution_time * 1000
                    time_unit = "ms"
                else:
                    time_value = execution_time
                    time_unit = "s"
            else:
                time_value = execution_time
                time_unit = unit
            
            # Color based on threshold
            if execution_time < threshold * 0.5:
                color = Colors.OKGREEN
            elif execution_time < threshold:
                color = Colors.OKCYAN
            else:
                color = Colors.FAIL
            
            print(f"{color}⏱  {func.__name__}() → {time_value:.3f} {time_unit}{Colors.ENDC}")
            
            return result
        
        return wrapper
    
    return decorator
