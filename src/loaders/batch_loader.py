"""
Batch loader for ETL operations
Handles batch processing of data loading
"""

from typing import List, Callable, Any
import time


class BatchLoader:
    """Handles batch loading of data with progress tracking"""
    
    def __init__(self, batch_size: int = 1000):
        """
        Initialize batch loader
        
        Args:
            batch_size: Size of each batch
        """
        self.batch_size = batch_size
    
    def load_in_batches(self, data: List[Any], load_function: Callable[[List[Any]], int]) -> int:
        """
        Loads data in batches using a provided load function.
        
        Args:
            data: List of data items to load
            load_function: A callable that takes a list of data items and returns the number of items loaded
            
        Returns:
            Total number of items successfully loaded
        """
        total_loaded = 0
        num_batches = (len(data) + self.batch_size - 1) // self.batch_size
        
        print(f"Starting batch load of {len(data)} items in {num_batches} batches (batch size: {self.batch_size})...")
        
        for i in range(num_batches):
            start_index = i * self.batch_size
            end_index = min((i + 1) * self.batch_size, len(data))
            batch = data[start_index:end_index]
            
            try:
                loaded_count = load_function(batch)
                total_loaded += loaded_count
                print(f"  Batch {i+1}/{num_batches}: Loaded {loaded_count} items. Total loaded: {total_loaded}")
            except Exception as e:
                print(f"  Batch {i+1}/{num_batches}: Error loading batch: {e}")
                # Depending on requirements, you might want to log the failed batch or retry
                continue
            
            # Small delay between batches to avoid overwhelming the system
            time.sleep(0.01)
        
        print(f"Finished batch load. Total items loaded: {total_loaded}/{len(data)}")
        return total_loaded
    
    def load_with_retry(self, data: List[Any], load_function: Callable[[List[Any]], int], 
                       max_retries: int = 3) -> int:
        """
        Load data with retry logic for failed batches
        
        Args:
            data: List of data items to load
            load_function: Function to load data
            max_retries: Maximum number of retries for failed batches
            
        Returns:
            Total number of items successfully loaded
        """
        total_loaded = 0
        num_batches = (len(data) + self.batch_size - 1) // self.batch_size
        
        print(f"Starting batch load with retry logic: {len(data)} items in {num_batches} batches...")
        
        for i in range(num_batches):
            start_index = i * self.batch_size
            end_index = min((i + 1) * self.batch_size, len(data))
            batch = data[start_index:end_index]
            
            loaded_count = 0
            for attempt in range(max_retries + 1):
                try:
                    loaded_count = load_function(batch)
                    total_loaded += loaded_count
                    print(f"  Batch {i+1}/{num_batches}: Loaded {loaded_count} items (attempt {attempt + 1})")
                    break
                except Exception as e:
                    if attempt < max_retries:
                        print(f"  Batch {i+1}/{num_batches}: Error on attempt {attempt + 1}: {e}. Retrying...")
                        time.sleep(2 ** attempt)  # Exponential backoff
                    else:
                        print(f"  Batch {i+1}/{num_batches}: Failed after {max_retries + 1} attempts: {e}")
                        break
            
            time.sleep(0.01)
        
        print(f"Finished batch load with retry. Total items loaded: {total_loaded}/{len(data)}")
        return total_loaded
