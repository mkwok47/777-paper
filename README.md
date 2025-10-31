# Big data visualization
This term paper showcases techniques to visualize big data using the NYC taxi dataset. By: Michael Kwok & Princely Oseji.
- Data source: https://chriswhong.com/open-data/foil_nyc_taxi/

# Contents
1. large-dataset-cloud-run: py file to run on large taxi dataset (135 million rows) on AWS
2. small-dataset-local-run: ipynb file to run on small taxi dataset (1.5 million rows) on local machine

# Outputs
1. Bar graph after aggregating data.
    - Interpretation: by aggregating the data (such as by a categorical column), we can downsize from millions of rows to just several and derive useful, high-level insights that summarize the data.

2. Scatterplot after sampling data to downsize data volume.
    - Interpretation: by plotting a random sample of the data, we can still obtain a rough idea of the data distribution. In the small-dataset run, notice that 10% sampling and 50% sampling have  similar-looking outputs.

3. Datashader points plot without needing to downsize data volume.
    - Interpretation: Datashader is suited to plot large amounts of data. However, it requires either a Pandas dataframe (which cannot handle big data) or a Dask dataframe. Note: the small-dataset local-run utilizes Dask, but getting Dask to run properly on AWS cloud EMR cluster was too troublesome to be feasible due to many package/version dependencies which require spinning up a new cluster each time a new issue is found and resolved. Thus, for large-dataset cloud-run, Datashader was performed on the sampled Pandas dataframe from prior steps, showing that these techniques can be combined.

# How to run on AWS cloud
1. When creating the EMR cluster, add the install_packages.sh file as a bootstrap action. This installs necessary 3rd party package dependencies used in the python file.

2. When running the step job, supply 4 arguments:
    - i) Full S3 URI path of the taxi dataset.
        - Example: s3://awesome-bucket/term-paper/taxi-data-sorted-large.csv.bz2
    - ii) Full S3 URI path of desired output directory for txt log output. It should already exist.
        - Example: s3://awesome-bucket/term-paper/run-1/
    - iii) Name of S3 bucket.
        - Example: awesome-bucket
    - iv) Name of subfolder for plot outputs. It should already exist.
        - Example: term-paper/run-1/
