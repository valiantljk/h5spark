#include "stdlib.h"
#include "hdf5.h"
#include "getopt.h"
#include <string.h>


#define NAME_MAX 255
char filename[NAME_MAX];
char cb_buffer_size[NAME_MAX];
char cb_nodes[NAME_MAX];

int main(int argc, char **argv){
  int      mpi_size, mpi_rank;
  hid_t   file_id2, group_id2, group_id2_temp, dset_id2, plist_id3, plist_id4, dataspace_id2,result_space, result_memspace_id;
  hsize_t result_offset[3], result_count[3], result_memspace_size[3], my_z_size;
  hsize_t dims2[3], dims_x, dims_y, dims_z;
  unsigned short *data_t;
  int i,j,k;

  MPI_Comm comm = MPI_COMM_WORLD;
  MPI_Info info;


  MPI_Init(&argc, &argv);
  MPI_Comm_size(comm, &mpi_size);
  MPI_Comm_rank(comm, &mpi_rank);

  int c;
  opterr = 0;
  strncpy(filename, "./fake_xyz_default.h5", NAME_MAX);
  //strncpy(cb_buffer_size,"16777216", NAME_MAX);
  //strncpy(cb_nodes, "16", NAME_MAX);
  dims_x = 2000;
  dims_y = 1000;
  dims_z = 200000;
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
      case 'z':
	dims_z = strtoull(optarg, NULL, 10);
	break;
      case 'k':
	col = strtol(optarg,NULL, 10);
      default:
	break;
      }


  MPI_Info_create(&info); 
  MPI_Info_set(info, "cb_buffer_size", cb_buffer_size);
  MPI_Info_set(info, "cb_nodes", cb_nodes);
  
  float file_size = dims_x*dims_y*dims_z*sizeof(unsigned short)/1024.0/1024.0/1024.0;
  if(mpi_rank == 0){
    printf("(x,y,z) is (%llu, %llu, %llu), file size is [%f]GB\n", dims_x,  dims_y,  dims_z, file_size);
  }


  //Create new file and write result to this file
  plist_id3 = H5Pcreate(H5P_FILE_ACCESS);
  H5Pset_fapl_mpio(plist_id3, comm, info);
    
  file_id2 = H5Fcreate(filename, H5F_ACC_TRUNC, H5P_DEFAULT, plist_id3);
  H5Pclose(plist_id3);

  group_id2_temp = H5Gcreate(file_id2, "/entry_0", H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT);
  group_id2 = H5Gcreate(group_id2_temp, "/entry_0/data_0", H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT);


  dims2[0] = dims_x;
  dims2[1] = dims_y;
  dims2[2] = dims_z;

  dataspace_id2 = H5Screate_simple(3, dims2, NULL);
  dset_id2 = H5Dcreate(group_id2, "/entry_0/data_0/data_0", H5T_STD_U16LE, dataspace_id2, H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT);
  H5Sclose(dataspace_id2);
  /*
  hsize_t rest_z_size;
  rest_z_size = dims_z % mpi_size;
  if(mpi_rank != (mpi_size -1)){
    my_z_size = dims_z/mpi_size;
  }else{
    my_z_size = dims_z/mpi_size + rest_z_size;
  }
  */
  result_offset[0] = mpi_rank;
  //result_offset[0] = 0;
  result_offset[1] = 0;
  //result_offset[2] =  (dims_z / mpi_size) * mpi_rank;
  result_offset[2] = 0;
  result_count[0] =1;
  //result_count[0] = dims_x;
  result_count[1] = dims_y;
  //result_count[2] = my_z_size;
  result_count[2] = dims_z;
  result_space = H5Dget_space(dset_id2);
  H5Sselect_hyperslab(result_space, H5S_SELECT_SET, result_offset, NULL, result_count, NULL);
   result_memspace_size[0] =1;
  //result_memspace_size[0] = dims_x;
  result_memspace_size[1] = dims_y;
  //result_memspace_size[2] = my_z_size;
  result_memspace_size[2] = dims_z;
  result_memspace_id = H5Screate_simple(3, result_memspace_size, NULL);

  if (mpi_rank == 0 || mpi_rank == (mpi_size -1))
    printf("mpi_rank:%d,  divide: %d,  rest: %d , my_s_size: %d \n", mpi_rank, (dims_z/mpi_size), (dims_z%mpi_size),  my_z_size);

  //data_t = (unsigned short *)malloc(dims_x * dims_y * my_z_size * sizeof(unsigned short)); 
  data_t = (unsigned short *)malloc(1 * dims_y * dims_z * sizeof(unsigned short)); 
  float my_size = (dims_x * dims_y * my_z_size * sizeof(unsigned short)) / 1024.0 / 1024.0 / 1024.0;
  if(data_t == NULL){
    printf("Memory allocation fails (mpi_rank = %d, [%d, %d, %d], [%f]) \n", mpi_rank, dims_x, dims_y, my_z_size, my_size);
    exit(1);
    return -1;
  }
  for(i=0;i<1;i++){
  //for (i = 0; i < dims_x; i++){
    for(j = 0; j < dims_y; j++){
      //for(k = 0; k < my_z_size; k++){
       for(k=0;k<dims_z;k++){
  	data_t[k+j*my_z_size+i*my_z_size*dims_y] = i;
      }
    }
  }
  //memset(data_t, mpi_rank, dims_x * dims_y * my_z_size);
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

   H5Dwrite(dset_id2, H5T_NATIVE_USHORT, result_memspace_id, result_space, plist_id4, data_t);
   H5Pclose(plist_id4);
  }
  else{
   H5Dwrite(dset_id2, H5T_NATIVE_USHORT, result_memspace_id, result_space, H5P_DEFAULT, data_t);

  }
  double t1 = MPI_Wtime()-t0;
  printf("rank %d,write time %.2fs\n",mpi_rank,t1);

  free(data_t);

  H5Sclose(result_space);
  H5Sclose(result_memspace_id);
  H5Dclose(dset_id2);
  H5Gclose(group_id2);
  H5Gclose(group_id2_temp);
  H5Fclose(file_id2);
  
  MPI_Finalize();
}
