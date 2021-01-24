# Data Lake for US Immigration Analytics

#### Project Summary
Be it work, study, or leisure, the Unites States has become one of the most popular destinations for many international travelers. Due to this, the country experiences a large number of immigration events throughout the year. The US National Tourism and Trade Office has aggregated data about such events in a relational table. This project aims to create a data lake using this data. The data lake is hosted on AWS S3 bucket and ingests over 30 million records of immigration data. The data model chosen in this project is that of a star schema that distributes the data efficiently over a set of facts and dimension tables. Such an architecture facilitates quick querying and is suitable for low latency applications.

The project performs the following steps:
* Step 1: Discuss the project Scope and data collection.
* Step 2: Perform exploratory data analysis (EDA) and cleaning.
* Step 3: Describe the Data Model.
* Step 4: Construct and run ETL pipelines to Model the Data
* Step 5: Run data quality checks

### Step 1: Project scope and data description

#### 1.1 Scope

This project aims to build a data lake on AWS S3 that supports fast analytics queries on the immigration data. To achieve this the following sub-tasks are achieved:

1. Gather data from different sources.
2. Perform Exploratory data analysis to check for null values in the data.
3. Preprocess the data by deleting columns and rows with null values.
4. Creating a data model that supports star schema.
5. Build ETL pipelines that load the data onto a data lake consisting of the facts and dimension tables.
6. Execute data quality checks to ensure the integrity of the data.

#### 1.2 Data Description

In this project, data is collected from 3 sources, each of which provides important information :

1. [Airport data](https://datahub.io/core/airport-codes#data)

This is a relational table of airport codes and their corresponding cities. The following describes the attributes of the table after cleaning:

| Attributes   | Description                    |
|--------------|--------------------------------|
| ident        | identification code of airport |
| type         | Type of airport based on size  |
| name         | Name of the airport            |
| elevation_ft | Elevation above sea level      |
| iso_country  | Country name                   |
| iso_region   | Region name code               |
| municipality  | Municipality of the airport    |
| gps_code     | GPS location code of airport   |
| iata_code    | IATA code of airport           |
| coordinates  | latitude, longitude            |

2. [Demographics data](https://public.opendatasoft.com/explore/dataset/us-cities-demographics/export/)

The demographics dataset is a relational table and is hosted on opendatasoft.com and offers insight into the demographics of cities in the US. The following describes the attributes of the table after cleaning:

| Attributes             | Description                            |
|------------------------|----------------------------------------|
| City                   | Name of the US city                    |
| State Code             | Name of US State                       |
| State                  | 2 character state code                 |
| Race                   | Ethnicity                              |
| Count                  | Total count                            |
| Median Age             | Median age of residents                |
| Male Population        | Total number of male residents         |
| Female Population      | Total number of female residents       |
| Total Population       | Total number of residents              |
| Number of Veterans     | Total number of veterans               |
| Foreign-born           | Total number of foreign-born residents |
| Average Household Size | Average household size                 |


3. [Immigration data](https://travel.trade.gov/research/reports/i94/historical/2016.html)

This data is hosted by the US National Tourism and Trade Office and has a lot of information about immigration events like date of arrival and departure of travelers, visa types, mode of transport, etc. The data has more than 30 million records. The following describes the attributes of the table after cleaning:

| Attribute | Description                                                                         |
|-----------|-------------------------------------------------------------------------------------|
| cicid     | Unique record ID                                                                    |
| i94yr     | 4 digit year                                                                        |
| i94cit    | Country of origin (3 digits)                                                        |
| i94res    | Country of residence (3 digits)                                                     |
| i94port   | Port of arrival code (3 digits)                                                     |
| arrdate   | Arrival date                                                                        |
| i94mode   | Mode of transport Code: 1 = Air, 2 = Sea, 3 = Land, 9 = Not Reported                |
| i94addr   | USA State code                                                                      |
| depdate   | Departure Date from the USA                                                         |
| i94bir    | Age of Respondent in Years                                                          |
| i94visa   | Visa Codes: 1 = Business, 2 = Pleasure, 3 = Student                                 |
| count     | Summary Statistics                                                                  |
| dtadfile  | Character Date Field - Date added to I-94 Files                                     |
| entdepa   | Arrival Flag                                                                        |
| entdepd   | Departure Flag                                                                      |
| matflag   | Match flag - Match of arrival and departure records                                 |
| biryear   | 4 digit year of birth                                                               |
| dtaddto   | Character Date Field - Date to which admitted to U.S. (allowed to stay until)       |
| gender    | Non-immigrant sex                                                                   |
| airline   | Airline used to arrive in the U.S.                                                  |
| admnum    | Admission Number                                                                    |
| fltno     | Flight number of Airline used to arrive in the U.S.                                 |
| visatype  | Class of admission legally admitting the non-immigrant to temporarily stay in U.S.A |
| i94mon    | Numeric month                                                                       |

### Step 2: Exploratory data analysis and cleaning

#### 2.1 Explore the Data 
The main objective of this step is to identify important data quality issues, like missing values, duplicate data, etc. To support this, the **plot_nulls** function in the `helper_functions.py` is used (for more information see the helper_functions.py file).

#### 2.2 Data Cleaning
The next task is to clean the data and ensure data integrity. To achieve this the following steps are taken:
1. The columns identified to have maximum missing data are dropped.
2. Rows containing missing values are deleted.
3. Rows containing duplicate values are deleted.

To achieve this, the data preprocessing functions contained in the `helper_functions.py` are used (see the `Capstone_Project.ipynb`). There are 3 preprocessing functions, each corresponding to one of the data sources (for more information see the helper_functions.py file).

**NOTE:** Although iata_code in the airport data has a lot of missing values, it is not dropped because IATA codes for airports act as primary keys.

### Step 3: Data Model Description

#### 3.1 Conceptual Data Model

The architecture chosen for the data lake is a star-schema database containing the following tables:

##### Fact Table

The fact table consists of all the i94 related fields such as year, port, mode of transport, etc. 


##### Dimension Tables

5 dimension tables can be joined with the fact table to get aggregated information. Here is a description of each dimension table:

1. **Airports Dimension Table**

This table holds information about airports like IATA code, type, elevation coordinates, etc.


2. **Demographics Dimension Table**

This table stems out of the demographics dataset, it contains summary statistics about different segments of the population based on race, sex, veteran status, etc. for various cities in the US.


3. **Immigrants Dimension Table**

This table contains information about each immigrant like their birth year, country of origin, visa type, sex, etc.


4. **Arrivals Dimension Table**

This table contains the date, day, week, month, and year details of all the arrivals of visa holders to the USA.


5. **Departures Dimension Table**

This table contains the date, day, week, month, and year details of all the departures of visa holders from the USA.

#### 3.2 Mapping Out Data Pipelines
To realize the data model discussed above the following functions present in `prepare_tables.py` are used:

- create_airports_dim_table
- create_demographics_dim_table
- create_i94_fact_table
- create_immigrants_dim_table
- create_arrival_dim_table
- create_departures_dim_table

Each of the above functions performs 4 salient subtasks:

1. Create a view of the input spark dataframe.
2. Extract relevant information from the view and populate the same into the dimension table.
3. Write the dimension table to a parquet file.
4. Return the dimension table.

### Step 4: Run Pipelines to Model the Data 

#### 4.1 Create the data model
This step deals with the creation of all the dimensions and fact tables using the functions discussed in section 3.2

#### 4.2 Data Quality Checks
The data quality checks are performed to ensure the pipeline ran as expected. They do the:
- Check if the primary keys in certain tables do not have null values.
- Check if there are no duplicate rows in tables

#### 4.3 Data dictionary 
This section provides a brief description of all the fields in the various tables (fact and dimension) utilized in this project:


##### 4.3.1 Fact Table

| ***I94 Fact Table*** |                                                                |
|----------------|----------------------------------------------------------------------|
| **Attributes**     | **Description**                                                      |
| cicid (Primary Key)         | Unique record ID                                        |
| year          | 4 digit year                                                         |
| port_of_entry        | Port of arrival code (3 characters)                                   |
| arr_date        | Arrival date                                                         |
| mode        | Mode of transport Code: 1 = Air, 2 = Sea, 3 = Land, 9 = Not Reported |
| arrival_state        | USA State code                                                       |
| dep_date        | Departure Date from the USA                                          |
| dtad_file       | Character Date Field - Date added to I-94 Files                      |
| arr_flag        | Arrival Flag                                                         |
| dep_flag        | Departure Flag                                                       |
| match_flag        | Match flag - Match of arrival and departure records                  |
| airline        | Airline used to arrive in the U.S.                                       |
| adm_num         | Admission Number                                                     |
| flight_num          | Flight number of Airline used to arrive in the U.S.                      |
| i94_month         | Numeric month                                                        |


##### 4.3.2 Dimension Tables


1. **Airports Dimension Table**

| ***Airports Dimension Table*** |                          |
|--------------------------|--------------------------------|
| **Attributes**           | **Description**                |
| airport_ident            | identification code of airport |
| airport_name             | Type of airport based on size  |
| airport_type             | Name of the airport            |
| elevation_ft             | Elevation above sea level      |
| airport_elevation        | Country name                   |
| airport_region           | Region name code               |
| airport_mun              | Municipality of the airport    |
| airport_gps_code         | GPS location code of airport   |
| airport_iata_code (Primary Key)       | IATA code of airport           |
| latitude                 | latitude coordinate            |
| longitude                | longitude coordinate           |


2. **Demographics Dimension Table**

| ***Demographics Dimension Table*** |                                  |
|------------------------------|----------------------------------------|
| **Attributes**               | **Description**                        |
| unique_id (Primary key)      | Uniquely identifies each row           |
| city                         | Name of the US city                    |
| state                        | Name of US State                       |
| State_code                   | 2 character state code                 |
| race                         | Ethnicity                              |
| count                        | Total count                            |
| median_age                   | Median age of residents                |
| male_population              | Total number of male residents         |
| female_population            | Total number of female residents       |
| total_population             | Total number of residents              |
| total_veterans               | Total number of veterans               |
| foreign_born                 | Total number of foreign-born residents |
| avg_household_size           | Average household size                 |


3. **Immigrants Dimension Table**

| ***Immigrants Dimension Table*** |                                                                               |
|----------------------------|-------------------------------------------------------------------------------------|
| **Attributes**             | **Description**                                                                     |
| cicid (Primary key)        | Unique record ID                                                                    |
| age                        | Age of Respondent in Years                                                          |
| birth_year                 | 4 digit year of birth                                                               |
| valid_till_date            | Character Date Field - Date to which admitted to U.S. (allowed to stay until)       |
| sex                        | Non-immigrant sex                                                                   |
| visa_type                  | Class of admission legally admitting the non-immigrant to temporarily stay in U.S.A |
| birth_country              | Country of origin (3 digits)                                                        |
| res_country                | Country of residence (3 digits)                                                     |
| visa_desc                  | Describes the nature of visa held (business, pleasure or student)                   |


4. **Arrivals Dimension Table**

| **Arrivals Dimension Table** |                 |
|--------------------------|---------------------|
| **Attributes**           | **Description**     |
| arrdate (Primary Key)    | Arrival date        |
| day                      | day of the month (1-31) |
| week                     | week number (1-52)  |
| month                    | month number (1-12) |
| year                     | four-digit year     |


5. **Departures Dimension Table**

| **Departures Dimension Table** |               |
|--------------------------|---------------------|
| **Attributes**           | **Description**     |
| depdate (Primary Key)    | Arrival date        |
| day                      | day of the month (1-31) |
| week                     | week number (1-52)  |
| month                    | month number (1-12) |
| year                     | four-digit year     |


#### Step 5: Summary

##### 5.1 FAQS

* Why Spark?

There are over 30 million records in the data used in this project. Given the amount of data, it makes sense to use Spark for the following reasons:

    1. Ability to avail fast analytics on big data
    2. Ability to handle large multi-source data.
    3. Ease of use and python library support.

* How often the data should be updated and why?

Given that the immigration data is refreshed every month, I believe that the pipeline should be run once every month to keep the data lake updated.


##### 5.2 How to handle the following scenarios

1. The data is increased by 100x.
 
Increase the nodes of the Spark cluster to utilize more parallelization.

2. The data populates a dashboard that must be updated daily by 7 am every day.
 
Utilize automated data pipeline services like apache airflow to maintain and run scheduled ETL operations regularly.

3. The database is accessed by 100+ users.
 
To ensure quick service, copies of data can be maintained on various nodes. This can be achieved by modeling the data using NoSQL tools like apache Cassandra.
