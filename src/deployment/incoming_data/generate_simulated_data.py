from pathlib import Path
import os
import sys
import pandas as pd
import numpy as np

def generate_new_data(original_data_file_path, rand_date, n):
    """
    Generates simulated data and stores it in /homelesslink/references
    """
    new_alerts = pd.read_csv(original_data_file_path)
    rand_samp = new_alerts.sample(frac=1).head(n)
    rand_samp['Alert ID'] = rand_samp['Alert ID'].str.replace('500','600')
    rand_samp['Date/Time Opened'] = rand_date
    rand_date_fp = rand_date.replace('/','_')
    rand_samp.to_csv(f'~/homelesslink/src/pipeline/incoming_data/simulated_new_data_{rand_date_fp}.csv', index=False)
