Token plugin
=========================================

This plugin is experimental and currently only implements a mapper that analyzes the given text and stores the tokes in a field.

For example: 

"This plugin is completely untested. USE IT AT YOUR OWN RISK!" would become
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
