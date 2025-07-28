#!/usr/bin/env python3
import sys
import os
sys.path.insert(0, '.')
print(os.getcwd())
# Test your changes here
from src.wfExporter import main, DatabricksExporter

# Quick test
try:
    main('config.yml')
    
    print("✅ Development test passed!")
except Exception as e:
    print(f"❌ Test failed: {e}")