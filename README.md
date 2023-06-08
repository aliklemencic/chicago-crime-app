# Chicago Crime and Weather Application

A big data application to see Chicago crime by type, year, and weather.

## Overview

This application is a toy implementation of the Lambda architecture using the Apache stack. Using weather data from the National Weather Service and 20 years of crime data from the Chicago Data Portal's [crime dashboard](https://data.cityofchicago.org/Public-Safety/Crimes-2001-to-present-Dashboard/5cd6-ry5g), it implements batch, serving, and speed layers as well as a front-end web application to display crimes in Chicago by crime type, year, and weather. A video walkthrough of the application can be found [here](https://youtu.be/Z3n7DJ34Fu8).

This project was completed in December 2022 as a final project for the MPCS 53014 Big Data Application Architecture class at the University of Chicago.

Languages and platforms used: Hadoop DFS, HBase, HiveQL, Java, Kafka, Node.js, Scala, Spark

## Project Structure

A quick summary of what is included in this repo.

```
.
├── chicago_crime_app: deploys the serving layer locally
├── chicago_crime_speed_layer: increments serving layer views in HBase
└── chicago_scalable_crime_app: scalably deploys the serving layer through CodeDeploy
```

## Technical Implementation

The Lambda architecture is composed of three layers: the batch layer, the speed layer, and the serving layer. Below is an explanation of how I implemented each layer.

### Batch Layer

The batch layer stored historical Chicago crime data from 2001-2021 in Hadoop DFS (HDFS) hosted on class-provided servers. Files were stored in ORC format. The master dataset was created once via bulk upload to HDFS. 

### Speed Layer

The speed layer implements a Kafka streaming buffer to mediate between new data pushes and ingestion into the serving layer views. Since the data for this project could not be automatically ingested via API, the web application includes pages to submit simulated crime and weather data. This simulated data is treated as real-time updates and combined with the pre-computed batch views in the serving layer.

### Serving Layer

The serving layer hosts pre-computed views in HBase. The front-end web application provides an interface where users may select a type of crime from a dropdown menu and see how many instances of that crime occured during different types of weather for each year data was available. Users may also select a specific year.
