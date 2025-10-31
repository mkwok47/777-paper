Term paper on visualizing big data

Contents
1. large-dataset-cloud-run: py file to run on large taxi dataset on AWS
2. small-dataset-local-run: ipynb file to run on small taxi dataset on local machine

How to run on AWS cloud
1. When creating the EMR cluster, add the install_packages.sh file as a bootstrap action. This installs necessary 3rd party package dependencies.
2. When running the step job, supply 4 arguments:
      i. Full S3 URI path of the taxi dataset. Example: s3://awesome-bucket/term-paper/taxi-data-sorted-large.csv.bz2 
     ii. Full S3 URI path of desired output directory. It should already exist. Example: s3://awesome-bucket/term-paper/run-1/
    iii. Name of S3 bucket. Example: awesome-bucket
     iv. Name of subfolder for plot outputs. It should already exist. Example: term-paper/run-1/
