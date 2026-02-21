import os
import unicodedata
import re
from time import perf_counter
from contextlib import contextmanager
import psutil

###############################
#         Parameters          #
###############################

process= psutil.Process(os.getpid())

###############################
#    Classes and functions    #
###############################

@contextmanager
def timer(block_name: str, processMeasures: list):

    print(f"============ {block_name} Ini ============")

    mem_ini = process.memory_info().rss / 1024**2  # MB
    t0 = perf_counter()
    
    yield block_name
    
    t1 = perf_counter()
    mem_end = process.memory_info().rss / 1024**2  # MB
    
    duration = t1 - t0
    mem_used = mem_end - mem_ini
    
    print(f"{duration:.4f}s | ΔMem: {mem_used:.2f} MB")
    print(f"============ {block_name} End ============")
    
    processMeasures.append({
        "block": block_name,
        "seconds": duration,
        "memory_MB": mem_used        
    })

# Minimal name normalizations

def normalize_name(name):
    if not isinstance(name, str):
        return ""
    name = unicodedata.normalize("NFKC", name)
    name = re.sub(r"[^\w\s]", " ", name)
    name = re.sub(r"\s+", " ", name).strip()
    return name