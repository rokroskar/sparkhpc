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