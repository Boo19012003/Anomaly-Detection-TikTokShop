import asyncio
import sys
import os

sys.path.append(os.path.abspath(os.path.dirname(__file__)))

from pipeline.products import run_pipeline

if __name__ == "__main__":
    for i in range(12):
        asyncio.run(run_pipeline())