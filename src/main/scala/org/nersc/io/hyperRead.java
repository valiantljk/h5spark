/************************************************************
  This example shows how to read and write data to a
  dataset by hyberslabs.  The program first writes integers
  in a hyperslab selection to a dataset with dataspace
  dimensions of DIM_XxDIM_Y, then closes the file.  Next, it
  reopens the file, reads back the data, and outputs it to
  the screen.  Finally it reads the data again using a
  different hyperslab selection, and outputs the result to
  the screen.
 ************************************************************/
package org.nersc.io;


import ncsa.hdf.hdf5lib.*;
import ncsa.hdf.hdf5lib.H5.*;
import ncsa.hdf.hdf5lib.HDF5Constants;
import ncsa.hdf.hdf5lib.exceptions.HDF5LibraryException;
import ncsa.hdf.hdf5lib.exceptions.HDF5Exception;
import org.slf4j.LoggerFactory;
import java.io.File;

public class hyperRead {
	public static double[][] readHyperslab(String para) {
		//String para="/global/cscratch1/sd/jialin/climate/oceanTemps.hdf5,temperatures,4584256,4584356";
		int file_id = -1;
		int filespace_id = -1;
		int dataset_id = -1;
		int dcpl_id = -1;
		String[] paralist =para.split(",");
		String FILENAME_dayabay = paralist[0].trim();
		String DATASETNAME_dayabay = paralist[1].trim();
		
	        int startr=Integer.parseInt(paralist[2].trim());
                int endr=Integer.parseInt(paralist[3].trim());
	        // Open an existing file.
                try {
                        file_id = H5.H5Fopen(FILENAME_dayabay, HDF5Constants.H5F_ACC_RDONLY, HDF5Constants.H5P_DEFAULT);
                }
                catch (Exception e) {
                        e.printStackTrace();
                }

                // Open an existing dataset.
                try {
                        if (file_id >= 0)
                                dataset_id = H5.H5Dopen(file_id, DATASETNAME_dayabay, HDF5Constants.H5P_DEFAULT);
                }
                catch (Exception e) {
                        e.printStackTrace();
                }
		long[]dset_dims={0,0};
		try{
		 filespace_id = H5.H5Dget_space(dataset_id);
                
                 H5.H5Sget_simple_extent_dims(filespace_id, dset_dims,null);
		}
		catch(Exception e){
			e.printStackTrace();
		}

		Integer dim2=(int)(long)dset_dims[1];
		double[][] dset_datasub = new double[endr-startr][dim2];
	 	double[] temp = new double[(endr-startr)*dim2];	
		// Define and select the hyperslab to use for reading.
		try {
			if (dataset_id >= 0) {
				long[] start = { startr, 0 };
				long[] count = { endr-startr, dset_dims[1]};
				int memspace;
				if (filespace_id >= 0) {	
					
					memspace=H5.H5Screate_simple(2, count, null);
					H5.H5Sselect_hyperslab(filespace_id, HDF5Constants.H5S_SELECT_SET,
							start, null, count, null);
					// Read the data using the previously defined hyperslab.
					if ((dataset_id >= 0) && (filespace_id >= 0))
						H5.H5Dread(dataset_id, HDF5Constants.H5T_NATIVE_DOUBLE,
								memspace, filespace_id, HDF5Constants.H5P_DEFAULT,
								temp);
				}
			}
		}
		catch (Exception e) {
			e.printStackTrace();
		}
		int i,j;
		for(i=0;i<endr-startr;i++)
		for(j=0;j<dim2;j++){
			dset_datasub[i][j]=temp[i*dim2+j];
		}
		// End access to the dataset and release resources used by it.
		try {
			if (dataset_id >= 0)
				H5.H5Dclose(dataset_id);
		}
		catch (Exception e) {
			e.printStackTrace();
		}

		try {
			if (filespace_id >= 0)
				H5.H5Sclose(filespace_id);
		}
		catch (Exception e) {
			e.printStackTrace();
		}

		// Close the file.
		try {
			if (file_id >= 0)
				H5.H5Fclose(file_id);
		}
		catch (Exception e) {
			e.printStackTrace();
		}
	 return dset_datasub;
	}
}
