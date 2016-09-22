# Copyright (C) 2016 The HDF Group
# All rights reserved
#
# Python script that converts HDF5 to Parquet using Apache arrow.
#
# If you have any questions, suggestions, or comments on this example,
# please use the HDF-EOS Forum (http://hdfeos.org/forums).
#
# If you would like to see an example of any other NASA HDF/HDF-EOS data
# product, feel free to contact us at eoshelp@hdfgroup.org or
# post it at the HDF-EOS Forum (http://hdfeos.org/forums).
#
# This script was tested on Mac OS X Mavericks machine with the latest
# parquet and arrow compiled from GitHub repository.
#
# Last tested: 9/22/2016
# Author: Hyo-Kyung Lee
import pyarrow as A
import pyarrow.parquet as pq
import pandas as pd
import h5py

FILE_NAME='/tmp/GSSTF_NCEP.3.1987.07.01.he5'
with h5py.File(FILE_NAME, mode='r') as f:
    dset_var = f['/HDFEOS/GRIDS/NCEP/Data Fields/SST']
    values = dset_var[0,:]
data = {}
data['i4'] = values.astype('i4')
filename='GSSTF.parquet'
df=pd.DataFrame(data)
arrow_table = A.from_pandas_dataframe(df)
A.parquet.write_table(arrow_table, filename, version="2.0")
table_read = pq.read_table(filename)
df_read = table_read.to_pandas()
print(df_read)
