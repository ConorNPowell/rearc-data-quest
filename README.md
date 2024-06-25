# rearc-data-quest
## By Conor Powell
##### Additional documentation provided by in-comment code and logging within script

### Part 1 Folder
#### Includes the source code for the script used in order to populate an S3 bucket with data from the BLS Website. The script works by pulling in the avaiable data on the website, checks for changes in the data by comparing last modified dates in S3 versus whats given on the BLS website, uploads only new data, and then delete any files that are no longer found on the BLS Website. All the files are dropped into one singular folder and not further organized.

### Part 2 Folder
#### Includes the source code for the script used to pull in data from the USA Data API. The script take a different approach to data storage compared to part one. Instead of all the data being dumped into the same folder, instead the data is dumped into a new sub-folder for the specific day. This would allow us to track historical changes a bit easier copmared to only have one file to compare to, e.g. part one. Because we are creating additional copies, bucket clean up is neccesary. That is why at the end of the script there is a bucket clean up fucntion that will delete any folders that 4 days or older. This makes sure were not wasting space for duplicate data

### Part 3 Folder
#### Contains the jupyter notebook report that answers all of the data analysis realted questions.

### Part 4 Folder
#### I ended up having to do this manually as I was battling some demons locally with Terraform and my local enviroments. I first started by creating an S3 event notication tied to the bucket where we are dumpign data that would dump notications into an SQS queue. After that I imported a custom layer into Lambda. Created a function within lambda to run Part 1 and Part 2 and attached it to a CRON job to run once daily. After that I created another lambda function to run the report script tied to the aforementioned SQS queue so that the report is only run with new data.

#### I have attached some images within the Part 4 folder for proof
