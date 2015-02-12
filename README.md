Token plugin
=========================================

This plugin is experimental, untested and currently only implements two features:

All terms endpoint
--------

Retrieves all tokens in an index from a user given term onwards.

Example:

```
GET wiki/_allterms/text?size=10&from=random&min_doc_freq=100
```

would result in (depending on the data):
 
```
 {
   "terms": [
      "random",
      "random_chance",
      "randomized",
      "randomly",
      "randomness",
      "rands",
      "randwick",
      "randy",
      "raney",
      "rang"
   ]
}
```

Parameters:

- size: number of terms to return

- from: term to start with. Starts from the next term that is greater if the term is not found in the dictionary

- min_doc_freq: skip all terms where document frequency is < min_doc_freq. document frequency for term is computed per shard not over the whole index.



Analyzed text mapper
--------

A mapper that analyzes the given text and stores the tokes in a field.

For example: 

"This plugin is completely untested. USE IT AT YOUR OWN RISK!" 

would become

["this","plugin","is","completely","untested","use","it", "at","your", "own","risk"]


Full example:

```
POST test
{
  "mappings": {
    "doc": {
      "properties": {
        "text": {
          "type": "string",
          "fields": {
            "analyzed": {
              "type":"analyzed_text",
              "store": true
            }
          }
        }
      }
    }
  }
}

PUT test/doc/1
{
    "text": "This plugin is completely untested. USE IT AT YOUR OWN RISK!"
}

GET test/_search
{
  "fields": ["text.analyzed"]
}
```

Result:

```
{
   "took": 7,
   "timed_out": false,
   "_shards": {
      "total": 5,
      "successful": 5,
      "failed": 0
   },
   "hits": {
      "total": 1,
      "max_score": 1,
      "hits": [
         {
            "_index": "test",
            "_type": "doc",
            "_id": "1",
            "_score": 1,
            "fields": {
               "text.analyzed": [
                  "this",
                  "plugin",
                  "is",
                  "completely",
                  "untested",
                  "use",
                  "it",
                  "at",
                  "your",
                  "own",
                  "risk"
               ]
            }
         }
      ]
   }
}
```

License
-------

    This software is licensed under the Apache 2 license, quoted below.

    Copyright 2009-2014 Elasticsearch <http://www.elasticsearch.org>

    Licensed under the Apache License, Version 2.0 (the "License"); you may not
    use this file except in compliance with the License. You may obtain a copy of
    the License at

        http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
    WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
    License for the specific language governing permissions and limitations under
    the License.
