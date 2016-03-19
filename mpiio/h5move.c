#include "stdlib.h"
#include "hdf5.h"
#include "getopt.h"
#include <string.h>


#define NAME_MAX 255
char filename[NAME_MAX];
char output[NAME_MAX];
char DATASETNAME[NAME_MAX];
char cb_buffer_size[NAME_MAX];
char cb_nodes[NAME_MAX];
int  NDIMS=2;
int main(int argc, char **argv){
  int      mpi_size, mpi_rank;
  MPI_Comm comm = MPI_COMM_WORLD;
  MPI_Info info;
  MPI_Init(&argc, &argv);
  MPI_Comm_size(comm, &mpi_size);
  MPI_Comm_rank(comm, &mpi_rank);

  int c;
  opterr = 0;
  strncpy(output, "/global/cscratch1/sd/jialin/hdf-data/climate/temp1.h5",NAME_MAX);
  strncpy(filename, "./fake_xyz_default.h5", NAME_MAX);
  strncpy(cb_buffer_size,"16777216", NAME_MAX);
  strncpy(cb_nodes, "16", NAME_MAX);


  int col=1;//collective read/write
  //input args: i: inputfilename,o:outputfilename b: collective_buffersize, n: collective_buffernodes, k:iscollective, v:datasetname   
  while ((c = getopt (argc, argv, "i:o:b:n:k:v:")) != -1)
    switch (c)
      {
      case 'i':
	strncpy(filename, optarg, NAME_MAX);
	break;
      case 'o':
        strncpy(output, optarg, NAME_MAX);
        break;
      case 'b':
	strncpy(cb_buffer_size,optarg, NAME_MAX);
	break;
      case 'n':
	strncpy(cb_nodes, optarg, NAME_MAX);
	break;
      case 'k':
	col = strtol(optarg, NULL, 10);
      case 'v':
	strncpy(DATASETNAME, optarg, NAME_MAX);
      default:
	break;
      }

  MPI_Info_create(&info); 
  MPI_Info_set(info, "cb_buffer_size", cb_buffer_size);
  MPI_Info_set(info, "cb_nodes", cb_nodes);
 

  //Open file/dataset
  hid_t fapl,file,dataset;
  fapl = H5Pcreate(H5P_FILE_ACCESS);
  H5Pset_fapl_mpio(fapl, comm, info);
  file= H5Fopen(filename, H5F_ACC_RDONLY, fapl);
  H5Pclose(fapl);
  if(mpi_rank==0) {
	if(file<0){
	  printf("File %s open error\n",filename); 
	  return 0;
	}
	else {
	  printf("File %s open ok\n",filename);
	}
  }
  dataset= H5Dopen(file, DATASETNAME,H5P_DEFAULT);
  if(dataset <0 && mpi_rank==0) {printf("Data %s open error\n",DATASETNAME); return 0;}
  
 
  hid_t datatype,dataspace;
  H5T_class_t class;                 /* data type class */
  H5T_order_t order;                 /* data order */
  size_t      size;                  /* size of data*/    
  //hsize_t     dims_out[NDIMS];           /* dataset dimensions */ 
  int i, status_n,rank;
  /* Get datatype and dataspace handles and then query
       dataset class, order, size, rank and dimensions  */
  datatype  = H5Dget_type(dataset);     /* datatype handle */ 
  class     = H5Tget_class(datatype);
  if (class == H5T_INTEGER && mpi_rank==0) printf("Data set has INTEGER type \n");
  if (class == H5T_FLOAT && mpi_rank==0) printf("Data set has Float type \n");
  order     = H5Tget_order(datatype);
  if (order == H5T_ORDER_LE && mpi_rank==0) printf("Little endian order \n");
  size  = H5Tget_size(datatype);
  if(mpi_rank==0) printf("Data size is %d \n", size);
  dataspace = H5Dget_space(dataset);    /* dataspace handle */
  rank      = H5Sget_simple_extent_ndims(dataspace);
  //if(rank!=NDIMS && mpi_rank==0) {printf("Dimension of dataset is not correct\n"); return 0;}
  hsize_t     dims_out[rank];
  status_n  = H5Sget_simple_extent_dims(dataspace, dims_out, NULL);
  if(mpi_rank==0){
   for(i=0;i<rank;i++)
   printf("Dimensions %d: %lu\n", i, (unsigned long)(dims_out[i]));
  }
 
  hsize_t offset[rank];
  hsize_t count[rank];


  //parallelize along x dims
  offset[0] = mpi_rank;
  hsize_t rest;
  rest = dims_out[0] % mpi_size;
  if(mpi_rank != (mpi_size -1)){
    count[0] = dims_out[0]/mpi_size;
  }else{
    count[0] = dims_out[0]/mpi_size + rest;
  }

  //select all for other dims
  for(i=1; i<rank; i++){
   offset[i] = 0;
   count[i] = dims_out[i];
  }
  //specify the selection in the dataspace for each rank
  H5Sselect_hyperslab(dataspace, H5S_SELECT_SET, offset, NULL, count, NULL);

  hsize_t rankmemsize=1;
  hid_t memspace;
  for(i=0; i<rank; i++){  
   rankmemsize*=count[i];
  }

  //memspace = H5Screate_simple(rank, dims_out, NULL);
  //H5Sselect_hyperslab(memspace,H5S_SELECT_SET,offset,NULL,count,NULL);
  memspace = H5Screate_simple(rank,count,NULL);
  float totalsizegb=mpi_size * rankmemsize / 1024.0 / 1024.0 / 1024.0;
  //alloc buffer for each rank
  void * data_t=NULL;
  if(class == H5T_FLOAT){
     if(size==4) data_t = (void *)malloc( rankmemsize * sizeof(float));
     else if(size==8) data_t = (void *)malloc( rankmemsize * sizeof(double));
  }
  else if(class == H5T_INTEGER)
    data_t = (void *)malloc( rankmemsize * sizeof(int)); 

  if(data_t == NULL){
    printf("Memory allocation fails mpi_rank = %d",mpi_rank);
    for (i=0; i< rank; i++){
    printf("Dim %d: %d, ",i,count[i]);
    }
    exit(1);
    return -1;
  }
 //memset(data_t, mpi_rank, dims_x * dims_y * my_z_size);
  double tr0 = MPI_Wtime();  
  if(mpi_rank == 0){
    if(col==1)
    printf("IO: Collective Read\n");
    else 
    printf("IO: Independent Read\n");
  }
  hid_t plist=H5P_DEFAULT;
  if(col==1){
   plist = H5Pcreate(H5P_DATASET_XFER);
   H5Pset_dxpl_mpio(plist, H5FD_MPIO_COLLECTIVE);
  }
  if(class == H5T_INTEGER)
    H5Dread(dataset, H5T_NATIVE_INT, memspace,dataspace, plist, data_t);
  else if(class == H5T_FLOAT){
    if(size==4) H5Dread(dataset, H5T_NATIVE_FLOAT, memspace,dataspace, plist, data_t);
    else if(size==8) H5Dread(dataset, H5T_NATIVE_DOUBLE, memspace,dataspace, plist, data_t);
  }

  H5Pclose(plist); 
  double tr1 = MPI_Wtime()-tr0;
  if(mpi_rank==0 ||mpi_rank==mpi_size-1){ 
   printf("\nRank %d, read time %.2fs\n",mpi_rank,tr1);
   for(i=0; i<rank; i++){
    printf("Start_%d:%d Count_%d:%d\n",i,offset[i],i,count[i]);
   }
   printf("\n");
   printf("Total Loading %f GB with %d mpi processes \n",totalsizegb,mpi_size);
  }

  //close object handle of file 1
  H5Tclose(datatype);
  H5Sclose(dataspace);
  H5Sclose(memspace);
  H5Dclose(dataset);
  H5Fclose(file);

  if(mpi_rank==0) printf("Closing File %s, Opening File %s \n",filename,output);
  //start to write to new places
  
  hid_t plist_id2, file_id2,dataspace_id2, dataset_id2;
  //new file access property
  plist_id2 = H5Pcreate(H5P_FILE_ACCESS);
  //mpiio
  H5Pset_fapl_mpio(plist_id2, comm, info);
  //create new file
  file_id2 = H5Fcreate(output, H5F_ACC_TRUNC, H5P_DEFAULT, plist_id2);
  if(mpi_rank==0) {
        if(file_id2<0){
          printf("File %s create error\n",output);
          return 0;
        }
        else {
          printf("File %s create ok\n",output);
        }
  }
  H5Pclose(plist_id2);
  //in mem data space for new file, rank, dims_out, same
  dataspace_id2 = H5Screate_simple(rank, dims_out, NULL);
  if(class == H5T_INTEGER)
     dataset_id2 = H5Dcreate(file_id2,DATASETNAME, H5T_NATIVE_INT, dataspace_id2, H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT);     
  else if(class == H5T_FLOAT){
     if(size==4) dataset_id2=H5Dcreate(file_id2,DATASETNAME, H5T_NATIVE_FLOAT, dataspace_id2, H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT);
     else if(size==8) dataset_id2=H5Dcreate(file_id2,DATASETNAME, H5T_NATIVE_DOUBLE, dataspace_id2, H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT);
  }
 
  H5Sselect_hyperslab(dataspace_id2, H5S_SELECT_SET, offset, NULL, count, NULL);

  double tw0 = MPI_Wtime();
  if(mpi_rank == 0){
    if(col==1)
    printf("IO: Collective Write\n");
    else
    printf("IO: Independent Write\n"); 
  }
 
  hid_t memspace_id2 = H5Screate_simple(rank, dims_out, NULL);
  hid_t plist_id3 = H5P_DEFAULT;
  if(col==1){
   plist_id3 = H5Pcreate(H5P_DATASET_XFER);
   H5Pset_dxpl_mpio(plist_id3, H5FD_MPIO_COLLECTIVE);
  }

  if(class == H5T_INTEGER)
	H5Dwrite(dataset_id2, H5T_NATIVE_INT, memspace_id2, dataspace_id2, plist_id3, data_t);
  else if(class == H5T_FLOAT){
     if(size==4) H5Dwrite(dataset_id2, H5T_NATIVE_DOUBLE, memspace_id2, dataspace_id2, plist_id3, data_t); 
     else if(size==8) H5Dwrite(dataset_id2, H5T_NATIVE_FLOAT, memspace_id2, dataspace_id2, plist_id3, data_t); 
  }
  H5Pclose(plist_id3);
  
  double tw1 = MPI_Wtime()-tw0;
  if(mpi_rank==0||mpi_rank==mpi_size-1)
  {
	printf("rank %d,write time %.2fs\n",mpi_rank,tw1);
	printf("Total Writing %f GB with %d mpi processes \n",totalsizegb,mpi_size);
  }
  if(data_t!=NULL)
   free(data_t);

  //clean up object handle
  H5Sclose(dataspace_id2);
  H5Sclose(memspace_id2);
  H5Dclose(dataset_id2);
  H5Fclose(file_id2);
  MPI_Finalize();
}
