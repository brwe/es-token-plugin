Token plugin
=========================================

This plugin is experimental and barely tested. 

All terms endpoint
==========

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
==========

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

Prepare spec request 
==========

In order to convert a document into a numeric vector we need some specification how is is supposed to work.
This specification can then be used to either retrieve vectors for each document or to apply a PMML model to a document.

To generate a specification, use the following endpoint:

```
POST INDEX/TYPE/_prepare_spec
{
  "features": [
    {},
    {},
    ...
  ],
  "sparse": BOOLEAN
}
```

The parameter "sparse" defines if the vector returned should be in sparse format or dense.

Return value for sparse 

The "features" array is an array of feature definitions each of which describes how a single field will be converted to an entry into a vector.
So far only string fields are supported.
The following parameters are mandatory for each definition:

`field`: the field this is supposed to look at
`tokens`: where the tokens come from. can be "significant_terms", "all_terms" or "given" Depending on this parameter other parameters are required, see below
`number`: can be "tf" if the resulting number in the vector should be the term frequency or "occurrence" in case the entry in the vector should be 1 if the token appears in the document or 0 otherwise
`type`: The type of the field, currently only "string" is supported



"tokens": "significant_terms"
-------------------



```
 POST INDEX/TYPE/_prepare_spec
 {
   "features": [
     {
       "field": FIELDNAME,
       "type": "string",
       "tokens": "significant_terms",
       "number": "tf"| "occurence",
       "index": INDEX_NAME,
       "request": A string with a significnat terms aggregation
       
     },
     {},
     ...
   ],
   "sparse": BOOLEAN
 }
```

This will execute a significant terms aggregation as specified in the "request" field and use all tokens that are returned by significant_terms. The aggregation will be performed on the index given in the "index" parameter.



"tokens": "all_terms"
-------------------

```
 POST INDEX/TYPE/_prepare_spec
 {
  "features": [
       {
         "field": FIELDNAME,
         "type": "string",
         "tokens": "all_terms",
         "number": "tf"| "occurence",
         "index": INDEX_NAME,
         "min_doc_freq": Minimum document frequency for each term, if a tersm doc freq is below it will be skipped
       },
       {},
       ...
     ],
     "sparse": BOOLEAN
 }
```

This will use all terms in the index on the given field that exceed the minimum document frequency given.

"tokens": "given"
-------------------

```
 POST INDEX/TYPE/_prepare_spec
 {
  "features": [
       {
         "field": FIELDNAME,
         "type": "string",
         "tokens": "all_terms",
         "number": "tf"| "occurence",
         "terms": ["term1", "term2", ...]
       },
       {},
       ...
     ],
     "sparse": BOOLEAN
 }
```

This will use the tokens given in the "terms" list.





Model scripts
=============


Models must be stored as a PMML string in any index as a document in the following format:

```
{
    "pmml": "HER BE THE UGLY XML AS A STRING"
}
```

To use a model for prediction on a document, execute a request with a native script and add parameters that point to the spec and the stored model like this:



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
