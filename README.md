# databaseProject  
An interface and implementation of a distributed database system integrating independent child postgres servers  

### Requirements  

`python3` and `pip3` are required to run this project  

### Using the software  

After cloning the repository, go to the root folder of the repository  

`make deps` to install dependencies  

Versions of libraries compatible with  
aenum == 2.0.8  
pg-query == 0.12  
psycopg2 == 2.7.3  
tabulate == 0.7.7  

`make run` to launch the command line interface  


### Commands  

To add a new server `add [-h] [--password PASSWORD] host port database username`  

To set a master server `master [-h] [--password PASSWORD] host port database username`  

To display the list of current working servers `display`  

To delete a server `del [-h] host port database`  

To execute any query(insert, delete, select, drop) `execute [-h] queryString [queryString ...]`  

To create servers and create/populate database from a script file `load [-h] filename`  

To set debug mode off `debug false`  

To set debug mode on `debug true`  

To get help string and summary of all commands `help`  

To exit the CLI `exit` or `^C`

### Demonstration  

Create the metadata and child databases, gather the hostname, port, database names and authentication details.

Edit the `config` script file provided to;  
	set the master/metadata server using the `master` command  
	add the required sites using the thee `add` command  

Run the `config` script file using `run config`  
This creates the system and a sample schema and populates it with some data  

Run queries using `execute querystring`  