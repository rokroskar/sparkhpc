# A setup guide for using Spark on Brutus taking advantage of MPI job scheduling

To get started type (On Euler or Brutus)
```
git config --global http.proxy http://proxy.ethz.ch:3128
git clone https://skicavs@bitbucket.org/skicavs/spark-on-brutus.git
```

This will then create a folder with everything needed to start a spark job, to actually start the *example* job of calculating pi, type the following

```
cd spark-on-brutus
bsub -n 12 -W 1:00  < all.sh
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
ssh -L8080:e1442:8080 -L4040:e1442:4040 maderk@euler01.hpc-lca.ethz.ch 
```

And then opening up a browser on your local machine to

* [http://localhost:8080](http://localhost:8080)  shows you the master/cluster status panel
* [http://localhost:4040](http://localhost:4040)  shows you the application status panel