##### PySpark RealEstate Basic ETL PipeLine

###### System Design:
1. The basic pipeline design is available in powerpoint in resources folder.
2. The functionalities of each zone is mentioned in the "notes" section of the powerpoint
3. The technologies mentioned in the powerpoint can be changed depending on the requirements of the use case.

###### Process Development:
1. The process is designed to capture the daily snapshots of the data
2. The results are stored in csv files instead of tables due to environment constraints but the table partition logic 
    is maintained in the form of folders.

`How the house data and transit classification data is used?`

In order to identify if the transit accessibility has any impact on the sold house prices, it is important to know if 
the transit accessibility is available within certain distance from the house location.
 
From the raw files, sold house data has location coordinates in the form of latitude and longitude. But the 
transit classification data has location coordinates in spcs (state plane coordinate system) format. So, it is required
to convert the spcs coordinates to latitude and longitude so that the distance can be measured between houses and 
transit access. The functions convert_spcs_to_lat_long and get_distance helps in conversion and measuring the distance. 
The cut-off distance is taken as 5 kilometers and this data is reflected in the column "TRANSIT_NEARBY(5KMS)" from the 
table  "houses_with_transit_nearby" (Yes - the transit access available within 5 kms, No - Transit access not 
available within 5 kilometers)

