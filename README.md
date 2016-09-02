Token plugin
=========================================

This plugin is experimental and barely tested. 

All terms endpoint
==========

Retrieves all tokens in an index from a user given term onwards.

Example:

```
GET sentiment140/_allterms/text?size=10&from=random&min_doc_freq=100
```

would result in (depending on the data):
 
```
 {
   "terms": [
     "rather",
     "ray",
     "re",
     "reach",
     "read",
     "reading",
     "ready",
     "real",
     "realised",
     "reality",
     "realize"
   ]
 }
```

Parameters:

- `size`: number of terms to return

- `from`: term to start with. Starts from the next term that is greater if the term is not found in the dictionary

- `min_doc_freq`: skip all terms where document frequency is < `min_doc_freq`. document frequency for term is computed per shard not over the whole index.


Analyzed text field
==========

Similar to the [_analyze api](https://www.elastic.co/guide/en/elasticsearch/reference/current/indices-analyze.html). Allows you to retrieve a field in a document as the it would appear after analysis is done.

Example:

```
 GET movie-reviews/_search
 {
   "stored_fields": [],
   "ext": {
     "analyzed_text": {
       "analyzer": "standard",
       "field": "text"
     }
   }
 }

```

would return

```
...
    "hits": [
      {
        "_index": "sentiment140",
        "_type": "tweets",
        "_id": "AVOkpeizcWk5UWep0S36",
        "_score": 1,
        "_source": {
          "label": "negative",
          "text": "\"I hate this time zone diff. thing!! ALL THE ACTION HAPPENS WHILE I'M SLEEPING !!!!!!  \""
        },
        "fields": {
          "analyzed_text": [
            [
              "i",
              "hate",
              "this",
              "time",
              "zone",
              "diff",
              "thing",
              "all",
              "the",
              "action",
              "happens",
              "while",
              "i'm",
              "sleeping"
            ]
          ]
        }
      }
...
```

The request takes the same parameters as the [_analyze api](https://www.elastic.co/guide/en/elasticsearch/reference/current/indices-analyze.html).


Term vectors within search and scroll
=====================================

To use the term vector api per docuemnt as returned with search per document, use the following syntax:


```
GET test/_search
{
  "ext": {
    "termvectors": {
      //term vector parameters as described here: https://www.elastic.co/guide/en/elasticsearch/reference/current/docs-termvectors.html
      ...
    }
  },
  "query": ...
}

```


For example:


```

PUT test 
{
  "mappings": {
    "doc": {
      "properties": {
        "text": {
          "term_vector": "yes",
          "type": "text"
        }
      }
    }
  }
}

POST test/doc/1
{
  "text": "I am a happy hippo"
}

GET test/_search
{
  "ext": {
    "termvectors": {
      "per_field_analyzer": {
        "text": "whitespace"
      }
    }
  },
  "query": {
      "match_all": {}
  }
}

```

will result in 

```
{
  "took": 3,
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
        "_source": {
          "text": "I am a happy hippo"
        },
        "fields": {
          "termvectors": [
            {
              "text": {
                "field_statistics": {
                  "sum_doc_freq": 5,
                  "doc_count": 1,
                  "sum_ttf": 5
                },
                "terms": {
                  "I": {
                    "term_freq": 1,
                    "tokens": [
                      {
                        "position": 0,
                        "start_offset": 0,
                        "end_offset": 1
                      }
                    ]
                  },
                  "a": {
                    "term_freq": 1,
                    "tokens": [
                      {
                        "position": 2,
                        "start_offset": 5,
                        "end_offset": 6
                      }
                    ]
                  },
                  "am": {
                    "term_freq": 1,
                    "tokens": [
                      {
                        "position": 1,
                        "start_offset": 2,
                        "end_offset": 4
                      }
                    ]
                  },
                  "happy": {
                    "term_freq": 1,
                    "tokens": [
                      {
                        "position": 3,
                        "start_offset": 7,
                        "end_offset": 12
                      }
                    ]
                  },
                  "hippo": {
                    "term_freq": 1,
                    "tokens": [
                      {
                        "position": 4,
                        "start_offset": 13,
                        "end_offset": 18
                      }
                    ]
                  }
                }
              }
            }
          ]
        }
      }
    ]
  }
}
```

Note that currently term vectors must be stored for each field even if we want to analyze them on the fly due to this bug: https://github.com/elastic/elasticsearch/issues/17076
 
 


Using machine learning models with this plugin
==============================================

In order to apply trained machine learning models in elasticsearch with this plugin models need to be trained outside elasticsearch and stored in elasticsearch in PMML format.

The basic workflow is as follows:

1. Specify how documents should be converted to feature vectors (`_prepare_spec` api)
2. Retrieve document vectors with any tool you like (sci kit learn, spark, etc) using the spec (`pmml_vector` script)
3. store trained model in elasticsearch (`_store_model` api)
4. use model on new documents (`pmml_model` script)

Currently this plugin only supports retrieving term frequencies fo terms in string fields.



Prepare spec request (`_prepare_spec`)
======================================

In order to convert a document into a numeric vector we need some specification how is is supposed to work.
This specification can then be used to either retrieve vectors for each document or to apply a PMML model to a document.

To generate a specification, use the following endpoint:

```
POST _prepare_spec
{
  "features": [
    {},
    {},
    ...
  ],
  "sparse": BOOLEAN
}
```

The parameter `sparse` defines if the vector returned should be in sparse format or dense.



The `features` array is an array of feature definitions each of which describes how a single field will be converted to an entry into a vector.

So far only string fields are supported.
String fields are converted to vectors by counting the number of times a word occurs in a field or optionally if it occurs or not. 

The following parameters are mandatory for each definition:

`field`: the field this is supposed to look at
`tokens`: where the tokens come from. can be `significant_terms`, `all_terms` or `given`. Depending on this parameter other parameters are required, see below
`number`: can be `tf` if the resulting number in the vector should be the term frequency or `occurrence` in case the entry in the vector should be 1 if the token appears in the document or 0 otherwise
`type`: The type of the field, currently only `string` is supported



"tokens": "significant_terms"
-------------------



```
 POST _prepare_spec
 {
   "features": [
     {
       "field": FIELDNAME,
       "type": "string",
       "tokens": "significant_terms",
       "number": "tf"| "occurrence",
       "index": INDEX_NAME,
       "request": A string with a significnat terms aggregation. this should be json but is needs to be an actual string, so escaped " etc.
       
     },
     {},
     ...
   ],
   "sparse": BOOLEAN
 }
```

This will execute a significant terms aggregation as specified in the `request` field and use all tokens that are returned by `significant_terms`. The aggregation will be performed on the index given in the "index" parameter.



"tokens": "all_terms"
-------------------

```
 POST _prepare_spec
 {
  "features": [
       {
         "field": FIELDNAME,
         "type": "string",
         "tokens": "all_terms",
         "number": "tf"| "occurrence",
         "index": INDEX_NAME,
         "min_doc_freq": Minimum document frequency for each term, if a tersm doc freq is below it will be skipped
       },
       {},
       ...
     ],
     "sparse": BOOLEAN
 }
```

This will use `_allterms` in the index on the given field that exceed the minimum document frequency (`min_doc_freq`) given.

"tokens": "given"
-------------------

```
 POST _prepare_spec
 {
  "features": [
       {
         "field": FIELDNAME,
         "type": "string",
         "tokens": "given",
         "number": "tf"| "occurrence",
         "terms": ["term1", "term2", ...]
       },
       {},
       ...
     ],
     "sparse": BOOLEAN
 }
```

This will use the tokens given in the `terms` list.

Return value
------------
`_prepare_spec` will create an indexed script with language `pmml_vector` which can later be used to retrieve a vector per document (see "Vector scripts" below).
The return value looks like this:

```
{
  "index": ".scripts",
  "type": "pmml_vector",
  "id": "AVNgPhq-EcToqOJ2nbcv",
  "length": 32528
}
```

`length`: the number of entries in each vector

`id`: the id of the script which later needs to be used whe retrieving the vectors (see "Vector scripts" below). 

Optionally an id can be given with the `_prepare_spec` request like this:

```
POST _prepare_spec?id=my_custom_id
{
....
}
```

in which case the result will be :

```
{
  "spec": {
     ...
  },
  "length": 32528
}
```

Vector scripts
==============

The spec that is created with the `_prepare_spec` can be used in any place where scripts are used, like for example so:

```
GET sentiment140/_search
{
  "script_fields": {
    "vector": {
      "script": {
        "lang": "native",
        "inline": "doc_to_vector",
        "params": {
          "spec": {
           ...
          }
        }
      }
    }
  }
}
```
where the spec is the spec that `_prepare_spec` returned.


Result looks like this for sparse vectors:


```
   "hits": [
      {
        "_index": "sentiment140",
        "_type": "tweets",
        "_id": "AVNb-VfJYKIbrxwdVhA-",
        "_score": 1,
        "fields": {
          "vector": [
            {
              "indices": [
                881,
                7539,
                8659,
                13402,
                22831
              ],
              "values": [
                1,
                1,
                1,
                1,
                1
              ]
            }
          ]
        }
      },
      ...
```

indices is the index in the vector (in this example the whole vector would have 32528 entries most of whihc are 0 so that makes no sense to return). values is the actual vector entries.

For dense vectors it looks like this:

```
{
...
        "_index": "sentiment140",
        "_type": "tweets",
        "_id": "AVNb-VfJYKIbrxwdVhBX",
        "_score": 1,
        "fields": {
          "vector": [
            {
              "values": [
                0,
                0,
                2
              ]
            }
          ]
        }
      },
      ...
```


Store a trained model
=====================

Use the _store_model api to store a trained model. 
The request needs two parameters: the model in pmml format and the vector spec (created with _prepare_spec).

Request looks like this:

POST _store_model
{
  "model": "here be the xml that defines the model",
  "spec": "here be the script that defines the document-vector transformation"
}

or alternatively:


```
POST _store_model?spec_id=my_custom_id
{
  "model": "here be the xml that defines the model"
}
```

where spec_id points to the vector spec we created before.

There is currently no validation whether model and spec actually fit.

Return value is:

```
{
  "index": ".scripts",
  "type": "pmml_model",
  "id": "AVNgvTjoEcToqOJ2nbc_",
  "version": 1
}
```
where "id" again is the id of a script with lang pmml_model. Id can also be defines via the id parameter:

```
POST _store_model?spec_id=my_custom_id&id=my_custom_model_id
{
  "model": "here be the xml that defines the model"
}
```

in which case the result would be:

```
{
  "index": ".scripts",
  "type": "pmml_model",
  "id": "my_custom_model_id",
  "version": 1
}
```


Model scripts
=============

To apply the stored model, use the model id from the `_store_model` api whenever you could use a script like this:


```
GET sentiment140/_search
{
  "aggs": {
    "label": {
      "terms": {
        "script": {
          "id": "lr_tweets",
          "lang": "pmml_model"
        }
      }
    }
  }
}
```


Analyzer Processor
=============

Splits a field into an array of tokens using an analyzer. Only works on string fields. The analyzer processor supports the following options:

Parameters:

- `field`: the field to be analyzed

- `target_field` the field to assign the analyzed tokens, by default `field` is updated in-place

- `analyzer`: the name of the analyzer to use


```
{
  "analyzer": {
    "field": "my_field",
    "analyzer": "standard"
  }
}
```

Custom analyzers can be specified using `ingest.analysis` setting. It is using standard <<analysis,analysis>> format for
defining custom named analyzers. The `ingest.analysis` setting can be changed dynamically and will have an immediate effect on all
currently registered analyzer processor.


```
PUT _cluster/settings
{
  "persistent": {
    "ingest": {
      "analysis": {
        "analyzer": {
          "my_lowercase_analyzer": {
             "tokenizer": "keyword",
             "filter": "lowercase"
          }
        }
      }
    }
  }
}

PUT _ingest/pipeline/lowercase_title
{
  "description": "lower-casing analysis processor",
  "processors" : [
    {
      "analyzer" : {
        "field" : "title",
        "target_field": "title_lower",
        "analyzer" : "my_lowercase_analyzer"
      }
    }
  ]
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
