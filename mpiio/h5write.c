#include "stdlib.h"
#include "hdf5.h"
#include "getopt.h"
#include <string.h>


#define NAME_MAX 255
char filename[NAME_MAX];
char dataset[NAME_MAX];
char cb_buffer_size[NAME_MAX];
char cb_nodes[NAME_MAX];

int main(int argc, char **argv){
  int     mpi_size, mpi_rank;
  hid_t   file_id2, group_id2, group_id2_temp, dset_id2, plist_id3, plist_id4, dataspace_id2,result_space, result_memspace_id;
  hsize_t result_offset[2], result_count[2], result_memspace_size[2], my_z_size;
  hsize_t dims2[2], dims_x, dims_y, dims_z;
  double *data_t;
  int i,j,k;

  MPI_Comm comm = MPI_COMM_WORLD;
  MPI_Info info;

  MPI_Init(&argc, &argv);
  MPI_Comm_size(comm, &mpi_size);
  MPI_Comm_rank(comm, &mpi_rank);

  int c;
  opterr = 0;
  strncpy(filename, "./test.h5", NAME_MAX);
  strncpy(cb_buffer_size,"16777216", NAME_MAX);
  strncpy(cb_nodes, "16", NAME_MAX);
  dims_x = 2000;
  dims_y = 1000;
  strncpy(dataset,"temperatures",NAME_MAX);
  int col=1;//collective write
     
  while ((c = getopt (argc, argv, "f:b:k:n:x:y:z:")) != -1)
    switch (c)
      {
      case 'f':
	strncpy(filename, optarg, NAME_MAX);
	break;
      case 'b':
	strncpy(cb_buffer_size,optarg, NAME_MAX);
	break;
      case 'n':
	strncpy(cb_nodes, optarg, NAME_MAX);
	break;
      case 'x':
	dims_x = strtoull(optarg, NULL, 10);
	break;
      case 'y':
	dims_y = strtoull(optarg, NULL, 10);
	break;
      case 'k':
	col = strtol(optarg,NULL, 10);
      default:
	break;
      }


  MPI_Info_create(&info); 
  //MPI_Info_set(info, "cb_buffer_size", cb_buffer_size);
  //MPI_Info_set(info, "cb_nodes", cb_nodes);
  
  float file_size = dims_x*dims_y*sizeof(double)/1024.0/1024.0/1024.0;
  if(mpi_rank == 0){
    printf("(x,y) is (%llu, %llu), file size is [%f]GB\n", dims_x,  dims_y, file_size);
  }


  //Create new file and write result to this file
  plist_id3 = H5Pcreate(H5P_FILE_ACCESS);
  if(col==1)
  H5Pset_fapl_mpio(plist_id3, comm, info);
    
  file_id2 = H5Fcreate(filename, H5F_ACC_TRUNC, H5P_DEFAULT, plist_id3);
  H5Pclose(plist_id3);
  dims2[0] = dims_x;
  dims2[1] = dims_y;
  dataspace_id2 = H5Screate_simple(2, dims2, NULL);
  dset_id2 = H5Dcreate(file_id2,dataset, H5T_STD_U16LE, dataspace_id2, H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT);
  H5Sclose(dataspace_id2);
 
  result_offset[1] = 0;
  result_offset[0] =  (dims_x / mpi_size) * mpi_rank;
  result_count[0] = dims_x / mpi_size;
  result_count[1] = dims_y;
  if(mpi_rank==mpi_size-1)
  result_count[0] = dims_x / mpi_size + dims_x % mpi_size;
  result_space = H5Dget_space(dset_id2);
  H5Sselect_hyperslab(result_space, H5S_SELECT_SET, result_offset, NULL, result_count, NULL);

  result_memspace_size[0] = result_count[0];
  result_memspace_size[1] = result_count[1];
  result_memspace_id = H5Screate_simple(2, result_memspace_size, NULL);
  data_t = (double *)malloc(result_count[0] * result_count[1] * sizeof(double)); 

  float my_size = (dims_x * dims_y *sizeof(double)) / 1024.0 / 1024.0 / 1024.0;
  srand((unsigned int)time(NULL)); 
  for (i = 0; i < result_count[0]; i++){
    for(j = 0; j < result_count[1]; j++){
 	data_t[i*result_count[1]+j] = ((double)rand()/(double)(RAND_MAX)) * i;
    }
  }
  MPI_Barrier(comm);
  double t0 = MPI_Wtime();  
  if(mpi_rank == 0){
    if(col==1)
    printf("Collective Write data ... \n");
    else 
    printf("Independent Write data ... \n");
  }
  if(col==1){
   
   plist_id4 = H5Pcreate(H5P_DATASET_XFER);
   H5Pset_dxpl_mpio(plist_id4, H5FD_MPIO_COLLECTIVE);

   H5Dwrite(dset_id2, H5T_NATIVE_DOUBLE, result_memspace_id, result_space, plist_id4, data_t);
   H5Pclose(plist_id4);
  }
  else{
   H5Dwrite(dset_id2, H5T_NATIVE_DOUBLE, result_memspace_id, result_space, H5P_DEFAULT, data_t);

  }

  //printf("rank %d,start0 %lld count0 %lld,start1 %lld count1 %lld\n",mpi_rank,result_offset[0],result_count[0],result_offset[1],result_count[1]);
  MPI_Barrier(comm);
  double t1 = MPI_Wtime()-t0;
  if(mpi_rank==0) printf("Data size %.2f GB, Write Cost %.2f, Bandwidth %.2f Numproc %d\n",file_size,t1,file_size/t1,mpi_size);

  free(data_t);

  H5Sclose(result_space);
  H5Sclose(result_memspace_id);
  H5Dclose(dset_id2);
  H5Fclose(file_id2);
  MPI_Finalize();
}
