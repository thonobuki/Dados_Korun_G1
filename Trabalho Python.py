import subprocess
import sys
import os
import json

def install(package):
    subprocess.check_call([sys.executable, "-m", "pip", "install", package])

install('kaggle')