# docker-pyspark-poc1
This repository contains a dockerFile for creating an image required for running pytests. Included example runs pytest for a simple ETL script.

Run Steps:

1. Clone this repository.
   https://github.com/ripathak1988/docker-pyspark-poc1.git
        

2. Traverse to the repository root folder (where DockerFile is present)

   cd /path/to/rootFolder/
      

3. Run the below command for creating a docker image.
   
   docker build -t spark .
   
      
4. Please ensure that tests folder doesn’t have any folder named as ‘__pycache__’.

    
5. Run below command for executing pytest on Docker image.

   Mac:
   
   docker run --rm -it -p 4040:4040 -v $(pwd)/:/tmp/tests/ spark pytest /tmp/tests
   
   Windows Powershell:
   
   docker run --rm -it -p 4040:4040 -v ${pwd}/:/tmp/tests/ spark pytest /tmp/tests
