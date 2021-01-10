# Data-Pipeline-Demo

## Pipeline steps
1. Create lambda function to scrape data from reddit
2. Persist data to S3
3. Create lambda function to process raw data from s3
4. Persist results to S3
5. Create lambda function to generate wordcloud from lambda
6. Output images to S3

### Lambda layers
- https://github.com/keithrozario/Klayers