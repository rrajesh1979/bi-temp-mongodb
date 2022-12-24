# Bi-Temp MongoDB Reference

This is a reference for the MongoDB database used as Bi-Temporal persistence. It is a work in progress.

## Setup and Configuration
### Set environment variables for bi-temp-persist
```
export spring.data.mongodb.collection=forex;
export spring.data.mongodb.database=bitemp;
export spring.data.mongodb.uri=mongodb://localhost:27017/?readPreference=primary&directConnection=true&ssl=false
```