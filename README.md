# Rishabh Mehta (rmehta35)
# Homework 2 DBLP MAP REDUCE



## Instructions 
Repository can be cloned using the following command 
```

git clone https://rmehta35@bitbucket.org/cs441-fall2020/rishabh_mehta_hw2.git

```
### Prerequisites 
* System needs to have hadoop,sbt installed 

### Running the project
* The program requires two run time arguments 
    *Path to INPUT_DIR
    *Path to OUTPUT_DIR 
* Import the cloned repository to Intellij and choose between different available  ___Map Reduce Jobs___ in application.conf file.
    * Top_Authors_Published_Venue
    * Author_Most_Coauthor
    * Author_No_Coauthor
    * Publication_Highest_Authors_Venues
    * PublicationVenueOneAuthor
    * Author_Published_Consecutively

* Open `rishabh_mehta_hw2/src/main/resources/application.conf `
and edit JobsToRun variable to include the jobs that you want to run .Default value set will run all Jobs
* Additional details about implementation of Mapper and Reducer for the above jobs are present in their respective classes.
* Once JobsToRun is finalised run the following commands to build JAR 
```
    $ sbt clean
    $ sbt assembly
```
* This will create a JAR in 
 ```/home/rishabh/rishabh_mehta_hw2/project/target/scala-2.12/rishabh_mehta_hw2-assembly-0.1.jar```
* This JAR can now be used to run MAPReduce Jobs
    * To run locally use the following command
    
```    
    $ /usr/local/hadoop/bin/hadoop jar ~/rishabh_mehta_hw2/project/target/scala-2.12/rishabh_mehta_hw2-assembly-0.1.jar  ~/PATH_TO_INPUTDIR ~/PATH_TO_OUTPUTDIR
    
    Change usr/local/hadoop/bin/ to your local hadoop installation (HADOOP_HOME)  
   
```   
```
    Sample
    $ /usr/local/hadoop/bin/hadoop jar ~/rishabh_mehta_hw2/project/target/scala-2.12/rishabh_mehta_hw2-assembly-0.1.jar ~/home/hadoop/input/ ~/home/hadoop/output/
```
     
* Jobs can alternatively run on AWS EMR
    * For more details on how to run in AWS EMR refer to the video below

  
