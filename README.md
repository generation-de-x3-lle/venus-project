# Project Venus

docker-compose.yml can be found in the .devcontainer folder 
To set up PostgreSQL and Adminer run the command 'docker compose up -d' within .devcontainer folder

Design Schema
---

|     |clean_data(staging table)       |
|:----|:------------------------------:|
| PK  | id int NOT NULL AUTO_INCREMENT |
|-----|:------------------------------:|
|     | order_date_time DATETIME       |
|     | branch_location VARCHAR(50)    |
|     | products_on_order VARCHAR(2500)|
|     | total_payment DECIMAL(19,2)    |
|     | payment_type VARCHAR(20)       |

|     |             orders                   |
|:----|:------------------------------------:|
| PK  | order_id int NOT NULL AUTO_INCREMENT |
|-----|:------------------------------------:|
|     | order_date_time DATETIME             |
|     | branch_location VARCHAR(50)          |
|     | total_payment DECIMAL(19,2)          |
|     | payment_type VARCHAR(20)             |

|     |             products                   |
|:----|:--------------------------------------:|
| PK  | product_id int NOT NULL AUTO_INCREMENT |
|-----|:--------------------------------------:|
|     | product VARCHAR(100)                    |
|     | flavour VARCHAR(30)                    |
|     | price DECIMAL(19,2)                    |

|     |             products_on_order                   |
|:----|:-----------------------------------------------:|
| PK  | product_on_order_id int NOT NULL AUTO_INCREMENT |
|-----|:-----------------------------------------------:|
| FK  | order_id INT NOT NULL                           |
| FK  | product_id INT NOT NULL                         |


 ## Cloudformation ##

A cloudformation template with MVP has been implemented using YAML for readability purposes Two resources are created which are AWS Lambda, and IAM role

The first time CloudFormation stack is being created, the following lines in the .YML has to be commented out due to a bug: 

   NotificationConfiguration:
        LambdaConfigurations:
          - Event: s3:ObjectCreated:*
            Function: !GetAtt LambdaFunction.Arn

Then the CloudFormation stack has to be updated manually by running 'aws cloudformation update-stack' command.

## Grafana Infrastructure ##

The Grafana instance has been setup inorder to help visualise our application and data.

- We created EC2 Instance and added a security group to the EC2.
- Opened SSH (22) and default Grafana port (3000) to the internet (0.0.0.0/0) in the security group.
- Mapped port 80 to 3000
- installed docker in the EC2
- installed grafana image in docker and run it
- Created users with usernames and passwords
- Used SQL to code various tables in Grafana to display relevant Data Visuals
- Used Grafana's built in visuals to turn data tables into engaging visuals