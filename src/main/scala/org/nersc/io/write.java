//package javaExample;
package org.nersc.io;

import ncsa.hdf.object.*; // the common object package
import ncsa.hdf.object.h5.*; // the HDF5 implementation

public class write {

    private static String fname = "1.h5";
    private static long[] dims2D = { 4, 3 };

    public static void main (String args[]) throws Exception {
        // create the file and add groups ans dataset into the file
        createFile();

        // retrieve an instance of H5File
        FileFormat fileFormat = FileFormat.getFileFormat(FileFormat.FILE_TYPE_HDF5);

        if (fileFormat == null)
        {
            System.err.println("Cannot find HDF5 FileFormat.");
            return;
        }

        // open the file with read-only access
        FileFormat testFile = fileFormat.open(fname, FileFormat.READ);

        if (testFile == null)
        {
            System.err.println("Failed to open file: "+fname);
            return;
        }

        // open the file and retrieve the file structure
        testFile.open();
        Group root = (Group)((javax.swing.tree.DefaultMutableTreeNode)testFile.getRootNode()).getUserObject();

        // retrieve athe dataset "3D 32-bit integer 4x3x2"
        Dataset dataset = (Dataset)root.getMemberList().get(0);

        // HDF folks add these lines here
        dataset.init();
        long dims[] = dataset.getDims();
        long selected[] = dataset.getSelectedDims();
        int rank = dataset.getRank();
        for (int i=0; i<rank; i++) selected[i] = dims[i];

        float[] dataRead = (float[])dataset.read();
        System.out.println("The length of the returned array is: " + dataRead.length);

        // print out the data values
        for (int k = 0; k < 4; k ++){
            for (int i = 0; i < 3; i++){
                //for (int j = 0; j < 3; j++){
                    System.out.print(dataRead[k*3+i] + ", ");
                //}
            }
        }
        System.out.print("\n");
    }

    /**
     * create the file and add groups ans dataset into the file, which is the
     * same as javaExample.H5DatasetCreate
     *
     * @see javaExample.H5DatasetCreate
     * @throws Exception
     */
    private static void createFile() throws Exception {
        // retrieve an instance of H5File
        FileFormat fileFormat = FileFormat
                .getFileFormat(FileFormat.FILE_TYPE_HDF5);

        if (fileFormat == null) {
            System.err.println("Cannot find HDF5 FileFormat.");
            return;
        }

        // create a new file with a given file name.
        H5File testFile = (H5File) fileFormat.create(fname);

        if (testFile == null) {
            System.err.println("Failed to create file:" + fname);
            return;
        }

        // open the file and retrieve the root group
        testFile.open();
        Group root = (Group) ((javax.swing.tree.DefaultMutableTreeNode) testFile
                .getRootNode()).getUserObject();

        // create groups at the root
        //Group g2 = testFile.createGroup("float arrays", root);

        // create 3D 32-bit (4 bytes) float dataset of 4 by 3 by 2
        float[] temp = { .1f, .2f, .3f, .4f, .5f, .6f, .7f, .8f, .9f, 1.0f,1.1f, 1.2f };
        Datatype dtype = testFile.createDatatype(Datatype.CLASS_FLOAT, 4,
                Datatype.NATIVE, -1);
        Dataset dataset = testFile.createScalarDS("test",
                root, dtype, dims2D, null, null, 0, temp);

        // close file resource
        testFile.close();
    }
}
