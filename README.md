# AWS-Data-Pipeline-with-S3-Lambda-Redshift-SNS-and-QuickSight
This project demonstrates an end-to-end data pipeline using AWS services such as S3, Lambda, Redshift, SNS, and QuickSight to handle data ingestion, transformation, and visualization.

<div align="center">
    <img src="https://github.com/AdhamAymanElsayed/AWS-Data-Pipeline-with-S3-Lambda-Redshift-SNS-and-QuickSight/blob/main/AWS.jpeg" alt="AWS" width="700"/>
</div>


## Architecture Overview
The diagram above depicts a data pipeline that includes the following components:

1 - S3 Project Bucket: Acts as the source for incoming data. Data uploads to this bucket trigger the Lambda function.

2 - Lambda Function:

  * Trigger: Gets triggered by events in the S3 Project bucket.
  * Processing: The Lambda function processes incoming data and writes it to Amazon Redshift.
  * Notifications: Sends a notification to the SNS topic in case of failure or success.

4 - Redshift: Acts as the central data warehouse where the data processed by Lambda is stored. Data is transferred from Redshift to different S3 buckets as follows:

  * S3 Archive: Stores archived data.
  * S3 Inserts: Contains inserted data records.
  * S3 Updates: Stores updated data records.

5 - SNS (Simple Notification Service): Receives notifications from the Lambda function about processing status (success or failure).

6 - QuickSight: Connects to Redshift to provide data visualization and insights.

## How It Works
1 - Data is uploaded to the S3 Project bucket.

2 - The upload triggers the Lambda function, which processes the data.

3 - Processed data is stored in Redshift, and the relevant S3 buckets (Archive, Inserts, Updates) are populated.

4 - SNS sends notifications regarding the status of data processing.

5 - QuickSight connects to Redshift to visualize the processed data.
