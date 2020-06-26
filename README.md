# Purpose
The prototype project was used to learn the basics of the Apache Fluo API and to explore how it works for supporting CRUD-like operations while maintaining an index on the records being stored. 

# Data Model
In the Fluo table I store both the original record as well as a inverted index. Records look like:

```
{
  "id":"abc12345",
  "value": 1122
}
```
These records are represented in Java with the `Item` class. An index on the `value` field is maintained and is used to search for Items by their value.

Per the notation from https://fluo.apache.org/tour/data-model/, record keys in Fluo look like `item<itemId>:item:json` with the value being the JSON serialized representation.

Index keys in Fluo look like `index<value>:id:<itemId>` with the value being empty.

# REST API
To create/update and read records, a simple REST API was written with the following end points


Resource: **/item**  
Method: PUT, POST  
For simplicity both methods create or update an Item. The Item is sent as JSON in the body of the request. The following happens within a Transaction. If it is an update and the Item already exists, the existing index value is deleted. Then for both creates and updates, a value is written for the record with its JSON serialized string representation as well as an index value.

Resource: **/item/find**  
Method: GET  
Parameters: value  
Looks up and returns all Items with the provided value. Within a Snapshot, all keys with a Row matching the search value are scanned, and a list of Item ids are returned by extracting the Column Qualifier from the returned keys. Then record keys/values are fetched based on this list of ids.

# Testing
All testing was with MiniFluo, so maybe it isn't wise to read into any performance numbers. The tests ran multiple threads which simultaneously updated Items and searched for Items. When Items where searched for, the client verified that the returned Items actually contained the value searched for. That is, showing the snapshot isolation ensuring that reads of the index and associated records is not impacted by concurrent updates to these records.

