This term paper showcases techniques to visualize big data using the NYC taxi dataset.

# Contents
1. large-dataset-cloud-run: py file to run on large taxi dataset (135 million rows) on AWS
2. small-dataset-local-run: ipynb file to run on small taxi dataset (1.5 million rows) on local machine

# Outputs
1. Bar graph after aggregating data to downsize data volume
2. Scatterplot after sampling data to downsize data volume
3. Datashader points plot without needing to downsize data volume

# How to run on AWS cloud
1. When creating the EMR cluster, add the install_packages.sh file as a bootstrap action. This installs necessary 3rd party package dependencies used in the python file.
2. When running the step job, supply 4 arguments:
      - i) Full S3 URI path of the taxi dataset. Example: s3://awesome-bucket/term-paper/taxi-data-sorted-large.csv.bz2 
     - ii) Full S3 URI path of desired output directory for txt log output. It should already exist. Example: s3://awesome-bucket/term-paper/run-1/
    - iii) Name of S3 bucket. Example: awesome-bucket
     - iv) Name of subfolder for plot outputs. It should already exist. Example: term-paper/run-1/
