# Environment Setup

This project uses environment variables to securely store Snowflake credentials and configuration values.
Environment variables are loaded using python-dotenv from a .env file

##### Prerequisites

1. [ ] Python 3.8+
2. [ ] pip
3. [ ] A Snowflake account

##### Install required Python packages

`pip install snowflake-connector-python 
pandas python-dotenv openpyxl`

##### Create and Add environment variables to .env

### Snowflake Credentials

1. USER=your_snowflake_username
2. PASSWORD=your_snowflake_password
3. ACCOUNT=your_account_identifier

### Snowflake Configuration
1. WAREHOUSE=COMPUTE_WH
2. DATABASE=YOUR_DATABASE_NAME
3. SCHEMA=PUBLIC