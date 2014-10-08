# A setup guide for using Spark on Brutus taking advantage of MPI job scheduling

To get started type 
```
git config --global http.proxy http://proxy.ethz.ch:3128
git clone https://skicavs@bitbucket.org/skicavs/spark-on-brutus.git
```
On Euler (or Brutus)
```
cd spark-on-brutus
bsub -n 12 -W 1:00  < all.sh
```
