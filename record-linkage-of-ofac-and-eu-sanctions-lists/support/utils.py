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

    print(f"============ {block_name} ============")

    mem_ini = process.memory_info().rss / 1024**2  # MB
    t0 = perf_counter()
    
    yield block_name
    
    t1 = perf_counter()
    mem_end = process.memory_info().rss / 1024**2  # MB
    
    duration = t1 - t0
    mem_used = mem_end - mem_ini
    
    print(f"{duration:.4f}s | ΔMem: {mem_used:.2f} MB")
    print(f"============ {block_name} ============")
    
    processMeasures.append({
        "block": block_name,
        "seconds": duration,
        "memory_MB": mem_used        
    })

def confusion_matrix(
        dfAssestment,
        dfTot,
        entityType,
        simCol,
        inCol,
        threshold        
    ):

    df = dfAssestment[dfAssestment.type == entityType].copy()    

    TP = (
        (df[simCol] >= threshold) &
        (df["real"] == True) &
        (df[inCol] == True)
    ).sum()
    FP = (
        (df[simCol] >= threshold) &
        (df["real"] == False) &
        (df[inCol] == True)
    ).sum()
    FN_THRESHOLD = (
        (df[simCol] < threshold) &
        (df["real"] == True) &
        (df[inCol] == True)
    ).sum()
    FN_BLOCK = (        
        (df["real"] == True) &
        (df[inCol] == False)
    ).sum()
    FN_TOT = FN_THRESHOLD + FN_BLOCK
    totEU = (
        (dfTot.source == "EU") &
        (dfTot.type == entityType)
    ).sum()
    totOFAC = (
        (dfTot.source == "OFAC") &
        (dfTot.type == entityType)
    ).sum()    
    TN = (totEU * totOFAC) - TP - FP - FN_TOT

    precision = TP / (TP + FP) if (TP + FP) > 0 else 0
    recall = TP / (TP + FN_TOT) if (TP + FN_TOT) > 0 else 0
    accuracy = (TP + TN) / (TP + FP + FN_TOT + TN) if (TP + FP + FN_TOT + TN) > 0 else 0
    f1 = (
        2 * precision * recall / (precision + recall)
        if (precision + recall) > 0
        else 0
    )

    return {
        "type": entityType,
        "threshold": threshold,
        "TP": TP,
        "FP": FP,
        "FN_THRESHOLD": FN_THRESHOLD,
        f"FN_{inCol.upper()}": FN_BLOCK,
        "FN_TOT": FN_TOT,        
        "TN": TN,
        "precision": precision,
        "recall": recall,
        "accuracy": accuracy,
        "f1": f1
    }