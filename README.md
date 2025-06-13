# Project: Data lakehouse with AWS Glue and Spark

## Purpose
In this project I'm a data engineer contracted by a company called STEDI.  
I built a Data Lakehouse solution for sensor data that trains a machine learning model.  
I have built data pipelines using Apache Spark and AWS to store, filter, process, and transform data from STEDI users for data analytics and machine learning applications.

## The Device
The STEDI team has been working on a hardware STEDI step trainer that helps users perform a balance exercise.  
The device has sensors that collect data to train a machine learning algorithm to detect steps.  
It also has a companion mobile app that gathers customer data and interacts with the device sensors.  
The step trainer is essentially a motion sensor that records the distance of the object it detects.


## Implementation tools and steps

These are the tools I used and what I did with them:  

1. **CloudShell**:  

1.1. Created [IAM role](https://github.com/MileRoss/data_engineering_with_AWS/blob/main/3_data_lakehouse_with_glue_and_spark/project_3_data_lakehouse_with_glue_and_spark/iam_role_policy.json) with required policies for the project.

1.2. Cloned this [GitHub Repository](https://github.com/MileRoss/data_engineering_with_AWS.git)

1.3. Copied customer, accelerometer, and step trainer data from the cloned repo to my S3 bucket with respect to the following folder structure:
```
- bucket-milenko/
-- accelerometer/
--- landing/
--- trusted/
-- customer/
--- curated/
--- landing/
--- trusted/
-- machine_learning/
--- curated/
-- step_trainer/
--- landing/
--- trusted/
```


2. Manually created 3 tables using **Glue Data Catalog**:

2.1. [customer_landing](https://github.com/MileRoss/data_engineering_with_AWS/blob/main/3_data_lakehouse_with_glue_and_spark/project_3_data_lakehouse_with_glue_and_spark/customer/customer_landing.sql)

2.2. [accelerometer_landing](https://github.com/MileRoss/data_engineering_with_AWS/blob/main/3_data_lakehouse_with_glue_and_spark/project_3_data_lakehouse_with_glue_and_spark/accelerometer/accelerometer_landing.sql)

2.3. [step_trainer_landing](https://github.com/MileRoss/data_engineering_with_AWS/blob/main/3_data_lakehouse_with_glue_and_spark/project_3_data_lakehouse_with_glue_and_spark/step_trainer/step_trainer_landing.sql)  

To recreate the tables, each folder [accelerometer, customer, step_trainer] in this repo includes "...landing_edit_schema_as_json.txt" file to speed up the column creation for you.


3. Created 5 Jobs using **Glue Studio Visual ETL**, with each Job saving data in a new Data Catalog table:

3.1. [customer_landing_to_trusted](https://github.com/MileRoss/data_engineering_with_AWS/blob/main/3_data_lakehouse_with_glue_and_spark/project_3_data_lakehouse_with_glue_and_spark/customer/customer_landing_to_trusted.py) to produce customer_trusted table.

3.2. [accelerometer_landing_to_trusted](https://github.com/MileRoss/data_engineering_with_AWS/blob/main/3_data_lakehouse_with_glue_and_spark/project_3_data_lakehouse_with_glue_and_spark/accelerometer/accelerometer_landing_to_trusted.py) to produce accelerometer_trusted table.

3.3. [customer_curated](https://github.com/MileRoss/data_engineering_with_AWS/blob/main/3_data_lakehouse_with_glue_and_spark/project_3_data_lakehouse_with_glue_and_spark/customer/customer_curated.py) to produce customer_curated table.

3.4. [step_trainer_trusted](https://github.com/MileRoss/data_engineering_with_AWS/blob/main/3_data_lakehouse_with_glue_and_spark/project_3_data_lakehouse_with_glue_and_spark/step_trainer/step_trainer_trusted.py) to producte step_trainer_trusted table.

3.5. [machine_learning_curated](https://github.com/MileRoss/data_engineering_with_AWS/blob/main/3_data_lakehouse_with_glue_and_spark/project_3_data_lakehouse_with_glue_and_spark/machine_learning_curated.py) to produce machine_learning_curated table.


4. Ran 10 queries using Athena Query Editor to test each created table against expected results provided by STEDI:

4.1. [customer_landing_1](https://github.com/MileRoss/data_engineering_with_AWS/blob/main/3_data_lakehouse_with_glue_and_spark/project_3_data_lakehouse_with_glue_and_spark/customer/customer_landing%201.jpg)  
<img src="https://raw.githubusercontent.com/MileRoss/data_engineering_with_AWS/main/3_data_lakehouse_with_glue_and_spark/project_3_data_lakehouse_with_glue_and_spark/customer/customer_landing%201.jpg" width="50%">

4.2. [customer_landing_2](https://github.com/MileRoss/data_engineering_with_AWS/blob/main/3_data_lakehouse_with_glue_and_spark/project_3_data_lakehouse_with_glue_and_spark/customer/customer_landing%202.jpg)  
<img src="https://raw.githubusercontent.com/MileRoss/data_engineering_with_AWS/main/3_data_lakehouse_with_glue_and_spark/project_3_data_lakehouse_with_glue_and_spark/customer/customer_landing%202.jpg" width="50%">

4.3. [customer_trusted_1](https://github.com/MileRoss/data_engineering_with_AWS/blob/main/3_data_lakehouse_with_glue_and_spark/project_3_data_lakehouse_with_glue_and_spark/customer/customer_trusted%201.jpg)  
<img src="https://raw.githubusercontent.com/MileRoss/data_engineering_with_AWS/main/3_data_lakehouse_with_glue_and_spark/project_3_data_lakehouse_with_glue_and_spark/customer/customer_trusted%201.jpg" width="50%">

4.4. [customer_trusted_2](https://github.com/MileRoss/data_engineering_with_AWS/blob/main/3_data_lakehouse_with_glue_and_spark/project_3_data_lakehouse_with_glue_and_spark/customer/customer_trusted%202.jpg)  
<img src="https://raw.githubusercontent.com/MileRoss/data_engineering_with_AWS/main/3_data_lakehouse_with_glue_and_spark/project_3_data_lakehouse_with_glue_and_spark/customer/customer_trusted%202.jpg" width="50%">

4.5. [customer_curated](https://github.com/MileRoss/data_engineering_with_AWS/blob/main/3_data_lakehouse_with_glue_and_spark/project_3_data_lakehouse_with_glue_and_spark/customer/customer_curated.jpg)  
<img src="https://raw.githubusercontent.com/MileRoss/data_engineering_with_AWS/main/3_data_lakehouse_with_glue_and_spark/project_3_data_lakehouse_with_glue_and_spark/customer/customer_curated.jpg" width="50%">

4.6. [accelerometer_landing](https://github.com/MileRoss/data_engineering_with_AWS/blob/main/3_data_lakehouse_with_glue_and_spark/project_3_data_lakehouse_with_glue_and_spark/accelerometer/accelerometer_landing.jpg)  
<img src="https://raw.githubusercontent.com/MileRoss/data_engineering_with_AWS/main/3_data_lakehouse_with_glue_and_spark/project_3_data_lakehouse_with_glue_and_spark/accelerometer/accelerometer_landing.jpg" width="50%">

4.7. [accelerometer_trusted](https://github.com/MileRoss/data_engineering_with_AWS/blob/main/3_data_lakehouse_with_glue_and_spark/project_3_data_lakehouse_with_glue_and_spark/accelerometer/accelerometer_trusted.jpg)  
<img src="https://raw.githubusercontent.com/MileRoss/data_engineering_with_AWS/main/3_data_lakehouse_with_glue_and_spark/project_3_data_lakehouse_with_glue_and_spark/accelerometer/accelerometer_trusted.jpg" width="50%">

4.8. [step_trainer_landing](https://github.com/MileRoss/data_engineering_with_AWS/blob/main/3_data_lakehouse_with_glue_and_spark/project_3_data_lakehouse_with_glue_and_spark/step_trainer/step_trainer_landing.jpg)  
<img src="https://raw.githubusercontent.com/MileRoss/data_engineering_with_AWS/main/3_data_lakehouse_with_glue_and_spark/project_3_data_lakehouse_with_glue_and_spark/step_trainer/step_trainer_landing.jpg" width="50%">

4.9. [step_trainer_trusted](https://github.com/MileRoss/data_engineering_with_AWS/blob/main/3_data_lakehouse_with_glue_and_spark/project_3_data_lakehouse_with_glue_and_spark/step_trainer/step_trainer_trusted.jpg)  
<img src="https://raw.githubusercontent.com/MileRoss/data_engineering_with_AWS/main/3_data_lakehouse_with_glue_and_spark/project_3_data_lakehouse_with_glue_and_spark/step_trainer/step_trainer_trusted.jpg" width="50%">

4.10. [machine_learning_curated](https://github.com/MileRoss/data_engineering_with_AWS/blob/main/3_data_lakehouse_with_glue_and_spark/project_3_data_lakehouse_with_glue_and_spark/machine_learning_curated.jpg)  
<img src="https://raw.githubusercontent.com/MileRoss/data_engineering_with_AWS/main/3_data_lakehouse_with_glue_and_spark/project_3_data_lakehouse_with_glue_and_spark/machine_learning_curated.jpg" width="50%">


Here's a simplified graph of this project.

<img src="https://video.udacity-data.com/topher/2023/October/6527a6fc_flowchart/flowchart.jpeg" width="50%">


## Troubleshooting

### Messy learning material
To do this project, I had to rely on Udacity's course [Data Engineering With AWS](https://www.udacity.com/course/data-engineer-nanodegree--nd027), Course 4: Spark and Data Lakes. The material is outdated and no longer in sync with the latest version of AWS interface. Some lessons on Udacity's platform feel like a draft version with legacy and updated parts alternating. 
Updating the course seems to be in progress, but overall it's an unacceptable state for a course using AWS in its title as a selling point. In the meantime, you should rely on the official [AWS Documentation](https://docs.aws.amazon.com/).

### Unable to save a Glue Job
What may lead you to this problem is mismatch between lessons in terms of node choices and the options tied to those choices. One lesson will ask you to choose S3 as your source node, and in the subsequent lesson the instructor says "as we have previously chosen Data Catalog source". Similar confusion goes on with Target node / Data Catalog update options. From the lesson "4.10  Create a Spark Job using Glue Studio" ending with asking to pick "Create a table in the Data Catalog and on subsequent runs, update the schema and add new partitions", the content moves to the lesson "4.11  Exercise: Define a Trusted Zone Path" where the pre-recorded video demonstrates the instructor going with "Do not update Catalog". If you end up picking "Create a table in the Data Catalog and on subsequent runs, update the schema and add new partitions" at this point, you won't be able to save your Job, and you may not understand why because they will only teach this later in the course. When you choose to "Create a table..." you also need to give your Job the information where to create it and how to name it.

### Glue Job Data Preview fails / Glue Job Run fails
Lessons 4.7 and 4.8 demonstrate Creating the Glue Service Role. However, they miss to include the policies required to preview data in Glue Studio Visual ETL, and to Run a Glue Job. Even the lesson ends with the example job still running, so we never see if it succeeded or failed.  
If your Data Preview fails, or your Job Run returns status Failed and not Succeeded, you may need more policies. You can use this [iam_role_policy.json](https://github.com/MileRoss/data_engineering_with_AWS/blob/main/3_data_lakehouse_with_glue_and_spark/project_3_data_lakehouse_with_glue_and_spark/iam_role_policy.json) file to have all the policies you need to complete this project.

### Athena query results seem multiplied vs. what's expected
Is your Glue Job / Target Node / Data Catalog update options / Create a table in the Data Catalog and on subsequent runs, update the schema and add new partitions ? If so, every time you run the Job, it will not overwrite the previous results but add the same results to the same folder/table that already contains the previous results. So, if you run the same Job twice, your results will be doubled.

### Glue Job creates empty tables
The course instructs for Target node to pick Format: JSON and leave Compression Type field empty. If you do this, observe the auto-generated script in the Glue Job. It will say Compression Type: Snappy. If you run this Job, it will create a table, but your data will be compressed into Snappy files. Athena Query Editor can read this table, but Glue Job can't. When you wish to use this table in another Glue Job, nothing will show.
The solution is to pick Compression Type: None.

### Budget 
Each of my 5 Glue Jobs took around 1.5 minutes to run, with default settings. Mind your budget because running them is not cheap. I wasted 25$ because of the Compression Type setting.

## Conclusion
This project demonstrates how to build a scalable, multi-zone data lakehouse using AWS Glue, S3, and Athena, enabling downstream machine learning and analytics applications.
