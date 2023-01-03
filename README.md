## Project details:
**Goal:** To implement k-means clustering with Apache Spark for two & three-dimensional data.

A test suite was used to test the functions/tasks implemented, with the result being a pass or fail.

*Project tasks*. The project involved four tasks in total to implement.

1. Task 1.
    
   Implementing k-means clustering for the two-dimensional data. The data          used can be found in file data/dataK5D2.csv(LABEL variable was exclude). The    task is to compute cluster means using DataFrames and MLlib. K is given        as a parameter. See task1 in the assignment.scala file. 
   
2. Task 2.

   Involves same steps as task 1 but using the 3-dimensional data. The data      used can be found in file data/dataK5D3.csv (the variables LABEL was          omitted for this task). See task2 in the assignment.scala.file.
   
3. Task 3.

   K-means clustering has been used in medical research. For instance,          referring to the example the data could model some laboratory results of a    patient and the label could implicate whether they have a fatal condition    or not. The labels are Fatal and Ok. Using two-dimensional data (like in      the file data/dataK5D2.csv), map the LABEL column to a numeric scale,        cluster in three dimensions (including columns a, b and num(LABEL)) and      return only the two-dimensional clusters means for two clusters which have    the biggest possibility for Fatal condition. See task3 in assignment.scala    file. 
   
4. Task 4.

   Elbow method is used to find the optimal number of the cluster means.        The task was to Implement a function which returns an array of (k, cost)      pairs, where k is the number of means and cost is some cost for the 
   clustering. 





















