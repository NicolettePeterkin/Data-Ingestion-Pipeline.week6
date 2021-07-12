import dask.dataframe
import modin.pandas as m_ray_pd # using the Ray core in modin
import pandas as pd
import time
import ray

input_file = 'parking_violations_issued_fiscal_year_2016.csv' # 6+ GB file

# Pandas - (runs out of RAM and crashes Google Colab)
start_time = time.time();
data = pd.read_csv(input_file);
print('Pandas took %s seconds' % (time.time() - start_time))

# Pandas[chunksize] - (runs out of RAM and crashes Google Colab)
start_time = time.time();
data = pd.read_csv(input_file, chunksize=100000);
print('Pandas took with chunksize %s seconds' % (time.time() - start_time))

# Modin[Ray] 
start_time = time.time();
data = m_ray_pd.read_csv(input_file);
print('Modin[Ray] %s seconds' % (time.time() - start_time))

# dask 
start_time = time.time();
data = dask.dataframe.read_csv(input_file);
print('Dask took %s seconds' % (time.time() - start_time))

