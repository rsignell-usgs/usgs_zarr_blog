# Cloud-optimized USGS Time Series Data
by Jeff Sadler

## Introduction
The US Geological Survey (USGS) is exploring how distribution, analysis and visualization of earth data on the Cloud could advance their mission.  The Cloud offers scalability, flexibility and configurabilty, but the full potential is only realized if cloud-optimized data is used. 

In this blog post we compare three formats for storing time series data: Zarr, Parquet, and CSV. Zarr ([zarr.readthedocs.io](https://zarr.readthedocs.io/en/stable/)) and Parquet ([parquet.apache.org](https://parquet.apache.org)) are compressed, binary data formats that can also be chunked or partitioned. This makes them a good choice for use on cloud-based environments where the number of computational cores can be scaled quickly and without limits common to more traditional HPC/HTC platforms. CSV will serve as a baseline comparison as a long-time standard format for time series data storage.

This comparision uses time series data from the USGS National Water Information System (NWIS). NWIS serves dozens of variables from thousands of locations.  Our comparison uses the discharge (aka streamflow) data recorded at 15 minute intervals.

We wanted to answer the question: "If we have a bunch of sites in a data base (NWIS) or if we have a bunch of sites in a Zarr store, which one performs better at retrieving a relevant subset?" To address this, we did the retrieval for one station (the overall outlet) and for all 23 stations in the Schuylkill River Basin, a sub-basin of the Delaware River Basin.  We chose the Delware because the USGS recently initiated a program for the Next Generation Water Observation System ([NGWOS](https://www.usgs.gov/mission-areas/water-resources/science/usgs-next-generation-water-observing-system-ngwos?qt-science_center_objects=0#qt-science_center_objects)) in the Delaware River Basin. NGWOS will provide denser, less expensive, and more rapid water quality and quantity data than what is currently being offered by the USGS and the DRB is the first basin to pilot the new systems. 

![Delaware and Schuylkill River Basins](fig/drb_gauges1.png)

## Methods

We gathered data from more than 12,000 stations across the continental US for a period of record of 1970-2019 using the NWIS web services ([waterservices.usgs.gov/rest/IV-Service.html](https://waterservices.usgs.gov/rest/IV-Service.html)).
The data was then converted to Zarr, Parquet and CSV.  For Zarr, we used chunks XX in the time dimension and XX in the station dimension, resulting in chunk size of XX MB.  There is more latency on S3 compared to file systems, which would suggest better performance using larger chunk sizes, but the cloud also allows reading with multiple processors, so the total time can be reduced by reading many chunks in parallel.  The 20-200Mb range seems to be a good compromise.  For Parquet, we used chunks of XX in the time dimension only because Parquet does not allow chunking in more than one dimension.  The text-based CSV data was written with XX significant digits to avoid wasting space, and does not support chunking or parallel reading. 

The NWIS data required some formatting of the data before it could be used in a Python DataFrame because NWIS webservices currently returns a plain text response which then has to be parsed and read into memory before analysis. When Zarr or Parquet data is retrieved, it is already in an analysis ready format and can be accessed as an Xarray dataset (Zarr) or Dask or Pandas DataFrame (Parquet). 

The data were accessed using a stock AWS EC2 t3a.large machine (8GB memory), with 4 cores.  The code used to do these comparisons is [here](https://github.com/jsadler2/usgs_zarr_blog/blob/master/comparison.py). Both comparisons were done for 1) a 10-day period and 2) a 40-year period.  


## Results

#### Table 1. Data Access Time (seconds)
|read times (s) | 1 station, 10 days| 1 station 40 years | 23 stations, 10 days | 23 stations, 40 years |
|---|------| ---|---| ---|
|NWIS| 0.3 | 0.02 | 0.02 | ---| 
|CSV| 2.4 | 0.2 | 0.26 | ---| 
|Parquet| 51.3 | 40.8 | 124.1 | ---| 
|Zarr |---| ---|---| ---|

#### Table 2. Data storage size (Mb)

|read times (s) | 1 station, 10 days| 1 station 40 years | 23 stations, 10 days | 23 stations, 40 years |
|---|------| ---|---| ---|
|NWIS| 0.3 | 0.02 | 0.02 | ---| 
|CSV| 2.4 | 0.2 | 0.26 | ---| 
|Parquet| 51.3 | 40.8 | 124.1 | ---| 
|Zarr |---| ---|---| ---|

### Comparison 1: Data retrieval and formatting

#### Table 1. Time in seconds to retrieve and format 10 days of data
| | Zarr | NWIS|
|---|---|---|
|Schuylkill outlet (sec)| 5.9 | 1.04| 
|all stations in Schuylkill basin (sec)| 6.1 | 19.7|  

#### Table 2. Time in seconds to retrieve and format 40 years of data 
| | Zarr | NWIS|
|---|---|---|
|Schuylkill outlet (sec)| 5.8 | 29.8 | 
|all stations in Schuylkill basin (sec)| 6.3 | 830 |  
|all stations in Schuylkill basin retrieve (sec)| | 401 |  

Overall Zarr was much faster at retrieving data from a subset of observations compared to the NWIS web services and scaled very well. In fact, there was hardly any difference in retrieval time when increasing the volume of data requested. Consequently, the performance difference between Zarr and NWIS increased as the volume of data requested increased. NWIS was actually faster for a single station for the 10-day request. When we increased the 10-day request to all 23 stations or the single station to a 40-year request, Zarr was 3x and 4x faster, respectively. The largest difference, though, occurred when pulling the 40 years of data for all 23 stations: retrieving and formatting the data from Zarr was more than 127x faster compared to NWIS web services! And this difference was only for 23 stations! Imagine the difference if we wanted only 123 stations (only about 1% of the total stations available).

<!-- The 830 seconds it took to retrieve from NWIS and format the 40 years of data from the 23 stations in the Schuylkill basin was split nearly evenly between retrieval and formatting. -->

### Comparison 2: Data write, read, and storage


#### Table 3. Read, write, and storage for 10 days of data
| | Zarr | Parquet| CSV|
|---|---|---| ---|
|read (sec)| 0.3 | 0.02 | 0.02 | 
|write (sec)| 2.4 | 0.2 | 0.26 | 
|storage (kB)| 51.3 | 40.8 | 124.1 | 

#### Table 4. Read, write, and storage for 40 years of data 
| | Zarr | Parquet| CSV|
|---|---|---| ---|
|read (sec)| 1.1 | 0.6 | 3.7 | 
|write (sec)| 6.1 | 1.6 | 31.1 | 
|storage (MB)| 33.5 | 15.4 | 110 | 

Except for reading the 10-day data, Parquet was the best performing for read and write times and storage size for both the 10-day and 40-year datasets. Zarr was the slowest format for read/write for the 10-day dataset with write speeds an order of magnitude slower than Parquet and CSV, though still under a second and a half. The performance of the CSV format was comparable to Parquet with the 10-day dataset and even faster reading. However, CSV scaled very poorly with the 40-year dataset. This was especially true of the the read and write times which increased over 170x for CSV. In contrast, the maximum increase for either Zarr or Parquet between the 10-day and 40-year dataset was an 11x increase in the Parquet write time. CSV also required a considerably larger storage size (3x compared to Zarr for the 40-year dataset).

## Discussion 
Since Parquet performed the best in nearly all of the categories of Comparison 2, you may be wondering, "why didn't you use Parquet to store all of the discharge data instead of Zarr?" The answer to that is flexibility. Zarr is a more flexible format than Parquet. Zarr allows for chunking along any dimension. Parquet is a columnar data format and allows for chunking only along one dimension. Additionally, Xarray's interface to Zarr makes it very easy to append to a Zarr store. This was very handy when I was making all of the NWIS web service calls. Even though the connection was very reliable because it was on a cloud machine, there times where the connection was dropped whether it was on EC2 side or the NWIS side. Because I could append to Zarr, when the connection dropped I could just pick up from the last chunk of data I had written and keep going.

The results of Comparison 1 show great promise in making large-scale USGS data more readily accessible through cloud-friendly formats on cloud storage. My speculation is that cloud-accessible data one day may serve as a complement to or a complete replacement of traditional web services. Because the S3 bucket is in the CHS cloud, any USGS researcher that has CHS access will have access to the same dataset that I did the tests on. Although I did the analysis on stations in the Schuylkill River Basin, similar results should be able to be produced with any arbitrary subset of NWIS stations. This retrieval is possible without any type of web-service for subsetting the data. Since Zarr is chunked, object storage it is easily and efficiently subsettable with functionality built into the interfacing software package (i.e., Xarray in Python). Additionally, the data is read directly into a computation friendly in-memory format (i.e., an Xarray dataset) instead of plain text in an HTML response as is delivered by a web service.

Beside efficient access, a major benefit of storing the data in the CHS S3 bucket in Zarr is the proximity to and propensity for scalable computing. Through the cloud, a computational cluster could be quickly scaled up to efficiently operate on the chunks of Zarr data. As USGS scientists become more accustomed to using cloud resources on CHS, having USGS data accessible in cloud-friendly formats will be a great benefit for large-scale research. The [Pangeo software stack](https://www.pangeo.io), which should be available through CHS soon, provides intuitive and approachable tools to help scientists perform cloud-based, scalable analyses on large cloud-friendly datasets.  

## Conclusion
The two big takeaways for me from this exercise were 1) time series data retrieval scales much much better with Zarr compared to using NWIS and 2) Parquet and Zarr scale much better in the reading, writing, and storage of time series compared CSV. These takeaways highlight the benefits of cloud-friendly storage formats when storing data on the cloud.
