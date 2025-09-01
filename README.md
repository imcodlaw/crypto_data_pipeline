# Data processing pipeline
A pipeline that includes the following task :

- Listening into Bybit's websocket stream for klines and trades data

  Establishing a websocket connection from Bybit that streams trades and klines data. Raw trades and klines data are pushed into different tables
  
- Aggregating raw data into 1 minute format
  
  Raw transaction data are generated into 1 minute data format 
  
- Extracting features from 1 minute aggregate data
  
  Feature engineering from aggregated data
  
- Pushing the extracted features to a sqlite db
  
  Extracted features are stored while the raw data are being deleted every 30 minutes to optimize storage use

