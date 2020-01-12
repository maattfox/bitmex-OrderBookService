# :chart_with_upwards_trend: bitmexOrderBook-Service  

Provides a Redis Limit Order Book implementation with other functions.

Written as a proof-of-concept for creating a simple websocket to database service for storing a temporary orderbook, quote and trade data for research. 

The orderbook is implemented with a Redis db, where each each key refers to the price, and value the size of orders at the price level. Quotes and Trade data are stored in MongoDB collections. A threaded websocket connection is created to BitMEX and messages are recieved and added to a thread-safe queue. A parser reads the message and adds to the databases.

## Todo:
Here are the few things I noticed, and wanted to add.

- [ ] Finish implementing docker
- [ ] Test latency and costs on AWS
- [ ] Use AWS Elasticache, VPC for MongoDB 
- [ ] Implement better database handling
- [ ] More efficient handling for websockets
- [ ] Threading support for Orderbook, Quote and Trade parsing - currently just manages the current number of messages from BitMEX
