# Data-Ingestion-Pipeline.week6

The file used in this comparison is the Parking_Violations_Issued_-_Fiscal_Year_2015.csv

If you wish to view the 2GB data file please click this [link](https://www.kaggle.com/new-york-city/nyc-parking-tickets?select=Parking_Violations_Issued_-_Fiscal_Year_2015.csv)


Time taken to read files comparison 

1.	Pandas took 69.10192894935608 seconds
2.	Pandas took with chunk size 1.3400604724884033 seconds
3.	Modin [Ray] 193.85107588768005 seconds
4.	Dask took 0.1345052719116211 seconds
