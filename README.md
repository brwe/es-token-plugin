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

Prepare spec request - THIS IS NOT DONE YET
--------

In order to convert a document into a numeric vector we need some specification how is is supposed to work.
This specification can then be used to either retrieve vectors for each document or to apply a PMML model to a document.

To generate a specification, use the following endpoint:

```
POST INDEX/TYPE/_prepare_spec
{
  HERE BE THE REQUEST
}
```

The request accepts a map of String: Object where the string is a field name that must be present in the mapping of INDEX/TYPE and the object describes what to do with the field.

Text fields are converted to numeric vectors by looking up the occurrences or the term frequencies of a token in the field. The selection of tokens that should be used have to be given in advance.

There are three options on how these tokens can be given:

1. Significant terms aggregation


```
 POST INDEX/TYPE/_prepare_spec
 {
   "NAME_OF_TEXT_FIELD": {
      "tokens": "significant_terms",
      "request": "source of the significant_terms request"
   }
 }
```

This will execute a significant terms aggregation as specified in the "request" field and use all tokens that are returned by significant_terms.

1. All terms


```
 POST INDEX/TYPE/_prepare_spec
 {
   "NAME_OF_TEXT_FIELD": {
      "tokens": "all_terms",
      "min_doc_freq": "minimum document frequency"
   }
 }
```

This will use all terms in the index that exceed the minimum document frequency given.

1. Given


```
 POST INDEX/TYPE/_prepare_spec
 {
   "NAME_OF_TEXT_FIELD": {
      "tokens": "given",
      "token_list": ["token_1", "token_2",...]
   }
 }
```

This will use the tokens given in the "token_list".




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
