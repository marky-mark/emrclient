### EMR Client

This is created to view and kill applications/jobs running on yarn inside Amazon EMR or any other remote location. 
Currently the amazon api does not include stopping jobs. Also supports adding steps to EMR

### Pre-requisites

Yarn must be installed on machine

#### Commands

In order to use this the master ec2 instance created must be assigned a public ip 

Display help
    
    ./emrclient --help

Set the master ip and port of the EMR master instance and yarn api (default 8088), this can be found in the EC2 tab. This caches the address 
in `~/.emrclient`

    ./emrclient set_master <MASTER IP:PORT>
    
Once this is set you may list applications 

    ./emrclient list
    
Pick an application from the list to kill

    ./emrclient kill <APPLICATION ID>
    
    