# A setup guide for using Spark on Brutus taking advantage of MPI job scheduling

To get started type (On Euler or Brutus)
```
git config --global http.proxy http://proxy.ethz.ch:3128
git clone https://skicavs@bitbucket.org/skicavs/spark-on-brutus.git
```

This will then create a folder with everything needed to start a spark job. 
To actually install spark run 
```
./setup_spark.sh
```
And it will download and install a version of Spark tested on Brutus.

## Running a Job
To actually start the *example* job of calculating pi, type the following.

```
cd spark-on-brutus
bsub < pijob.lsf
```

## Monitoring

To monitor the status of a job you can use the bjobs command
```
bjobs
```
which outputs
```
[maderk@euler01 spark-on-brutus]$ bjobs
JOBID      USER    STAT  QUEUE      FROM_HOST   EXEC_HOST   JOB_NAME   SUBMIT_TIME
2227145    maderk  RUN   beta.4h    euler01     12*e1442    spark_job  Oct  8 18:01
```

You can then view the spark console for this job by finding the main host (in this case e1442) and running the following ssh command to create a tunnel (replacing e1442 with the right name, and maderk with your username)
```
ssh -L8080:e1442:8080 -L4040:e1442:4040 maderk@brutus.ethz.ch 
```

And then opening up a browser on your local machine to

* [http://localhost:8080](http://localhost:8080)  shows you the master/cluster status panel
* [http://localhost:4040](http://localhost:4040)  shows you the application status panel


### SOCKS
Alternatively a SOCKS proxy server can be run by using the command
```
ssh -D 9999 maderk@brutus.ethz.ch
```

Which will start SOCKS Proxy server / tunnel inside the Brutus network allowing you to connect using the following commands on a properly configured browser (http://www.noah.org/wiki/SSH_tunnel#SOCKS5_with_Firefox set to localhost port 9999 in this case). 
On Mac systems it is easiest to use a dedicated Firefox since otherwise global "System Preferences" need to be changed.

* [http://e1442:8080](http://e1442:8080)  shows you the master/cluster status panel
* [http://e1442:4040](http://e1442:4040)  shows you the application status panel
