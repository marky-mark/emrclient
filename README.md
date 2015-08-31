### EMR Client

This is created to view and kill applications/jobs running on yarn inside Amazon EMR or any other remote location. 
Currently the amazon api does not include stopping jobs. Also supports adding steps to EMR and listing them. The amazon 
EMR api does not contain have calls to terminate jobs and so must be done via yarn api. A small layer of caching is also
added.

### Commands

In order to use the `list_applications` and `kill_application` the master ec2 instance of EMR cluster created must be 
assigned a public ip 

##### Display help
    
    ./emrclient --help

##### Configure

The purpose of this call is to cache some of the common parameters.
Set the master ip and port of the EMR master instance and yarn api (default 8088), this can be found in the EC2 tab. The cache
is currently in `~/.emrclient`

    ./emrclient configure -m <MASTER IP:PORT> -b <S3 BUCKET> -c <Cluster id> -r <REGION>

##### List Running Applications
    
Once this is set you may list applications by running

    ./emrclient list_applications_running

Alternatively the master may be temporally overwritten by using `-m <MASTER ADDRESS>`
   
##### List Applications by State
    
Once this is set you may list applications by state(RUNNING, KILLED, FAILED)

    ./emrclient list_applications <STATE>

Alternatively the master may be temporally overwritten by using `-m <MASTER ADDRESS>`
   
##### Kill an Application

Pick an application from the list to kill

    ./emrclient kill_application <APPLICATION ID>
    
Alternatively the master may be temporally overwritten by using `-m <MASTER ADDRESS>`

##### List jobs on cluster

List Jobs by state. 'PENDING','RUNNING','COMPLETED','CANCELLED','FAILED','INTERRUPTED'

    ./emrclient list_steps <STATE>  
    
Options

* -c, --cluster-id TEXT  Overwrite region of cluster. Not cached
* -r, --region TEXT      Overwrite region of cluster. Not cached

##### Submit job

Pick an application from the list to kill

    ./emrclient submit_job <NAME> <MAIN CLASS> 
    
Options

* -f, --file TEXT        Upload the file. This will be uploaded to s3 and overwrite whatever is there
* -b, --s3-bucket TEXT   Overwrite the s3 bucket location for the file to be uploaded to. Does not get cached
* -s, --s3-file TEXT     s3 file for the job. Used if already uploaded
* -c, --cluster-id TEXT  Overwrite the cluster id of EMR. Not cached
* -r, --region TEXT      Overwrite region of cluster. Not cached
* -a, --args TEXT        arguments for jar
* --help                 Display help message
    
Example of file already up on s3

    ./cli.py submit_job SmartProduct StreamingApp -a -m,yarn-cluster,-z,XXX.YYY.ZZZ:2181 -s s3://some-bucket/smart-product-assembly-0.0.1-SNAPSHOT.jar
    
Example of uploading file to s3 and using it

    ./cli.py submit_job SmartProduct StreamingApp -a -m,yarn-cluster,-z,XXX.YYY.ZZZ:2181 -f /some/file.jar -b some-bucket
    
    