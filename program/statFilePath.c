/*******************************************************************
 *       Filename:  statFilePath.c                                     
 *                                                                 
 *    Description:                                        
 *                                                                 
 *        Version:  1.0                                            
 *        Created:  09/19/2017 04:14:33 PM                                 
 *       Revision:  none                                           
 *       Compiler:  gcc                                           
 *                                                                 
 *         Author:  Ruan Huabin                                      
 *          Email:  ruanhuabin@tsinghua.edu.cn                                        
 *        Company:  Dep. of CS, Tsinghua Unversity                                      
 *                                                                 
 *******************************************************************/

#include "mpi.h"
#include <stdio.h>
#include <math.h>
#include <stdlib.h>
#include <string.h>
int main(int argc,char *argv[])
{
    int    myid, numprocs;
    double startwtime = 0.0, endwtime;
    int    namelen;
    char   processor_name[MPI_MAX_PROCESSOR_NAME];


    MPI_Init(&argc,&argv);
    MPI_Comm_size(MPI_COMM_WORLD,&numprocs);
    MPI_Comm_rank(MPI_COMM_WORLD,&myid);
    MPI_Get_processor_name(processor_name,&namelen);

    fflush(stdout);
    
    startwtime = MPI_Wtime();
    char commandToRun[2048];
    char *inputDir = argv[1];
    char *outputDir = argv[2];
    char *fileNamePrefix = argv[3];
    
    /*python searchRegularFile.py -i tmp8/0.txt -o tmp9 -p file_path_[n]*/
    sprintf(commandToRun, "python statRegularFile.py -i %s/path_list_%d.txt -o %s/stat_%d.txt -p %s_%d", inputDir, myid, outputDir, myid, fileNamePrefix, myid);
    fprintf(stdout,"Process %d of %d is on %s, commandToRun = %s\n", myid, numprocs, processor_name, commandToRun);
    system(commandToRun);
    
    endwtime = MPI_Wtime();
    printf("process:%d:wall clock time = %f\n", myid, endwtime-startwtime);	       
    fflush(stdout);
    MPI_Finalize();
    return 0;
}


