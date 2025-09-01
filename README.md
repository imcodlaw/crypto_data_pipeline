Data processing pipeline that includes :

- Listening into Bybit's websocket stream for klines and trades data
- Aggregating raw data into 1 minute format
- Extracting features from 1 minute aggregate data
- Pushing the extracted features to a sqlite db
