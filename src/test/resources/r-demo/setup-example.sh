#!/bin/bash

# get the test data, see http://www.cs.cornell.edu/People/pabo/movie-review-data/
mkdir data
cd data
wget -nc  http://www.cs.cornell.edu/people/pabo/movie-review-data/review_polarity.tar.gz 
tar xf review_polarity.tar.gz

wget -nc http://cs.stanford.edu/people/alecmgo/trainingandtestdata.zip .
unzip -o trainingandtestdata.zip

# from http://ai.stanford.edu/~amaas/data/sentiment/
wget -nc http://ai.stanford.edu/~amaas/data/sentiment/aclImdb_v1.tar.gz
tar xvf aclImdb_v1.tar.gz

wget -nc http://archive.ics.uci.edu/ml/machine-learning-databases/adult/adult.data

cd ..
# later we might be able to download but now build latest from source until release
elasticsearchversion=2.2.1

# Install elasticsearch 2.2.1 once it is out. we cannot use it now because token plugin must be based on >2.2.0 because of https://github.com/elastic/elasticsearch/pull/16822
# wget -nc https://download.elastic.co/elasticsearch/elasticsearch/elasticsearch-2.2.1.tar.gz
# tar xvf elasticsearch-2.2.1.tar.gz
# build plugin  

# for now we compile elasticsearch from 2.2 branch 
mkdir elasticsearch
cd elasticsearch
git clone https://github.com/elastic/elasticsearch .
git fetch
git checkout master
git reset --hard origin/master
gradle clean -DskipTests  assemble

cd ..
find elasticsearch/distribution/zip/build/distributions/ -name "*zip" -exec cp '{}' . \;
# get name to variable
ESRELEASE="$(ls | grep zip)"
strl=$(expr length $ESRELEASE - 4)
releasename="${ESRELEASE:0:$strl}"
rm -rf "$releasename"
unzip "$ESRELEASE"

strl=$(expr length $ESRELEASE - 4)
releasename="${ESRELEASE:0:$strl}"
versionlength="$(expr length $releasename - 14)"
releaseversion="${releasename:14:$versionlength}"

cd ../../../..
gradle clean -DskipTests assemble -Drepos.mavenlocal=true

echo "$(pwd)"

src/test/resources/r-demo/$releasename/bin/elasticsearch-plugin remove es-token-plugin
currentpath="$(pwd)"
src/test/resources/r-demo/$releasename/bin/elasticsearch-plugin install "file://localhost$currentpath/build/distributions/es-token-plugin-$releaseversion.zip"


# enable dynamic scripting
echo 'script.inline: on' >> src/test/resources/r-demo/$releasename/config/elasticsearch.yml
echo 'script.ingest: on' >> src/test/resources/r-demo/$releasename/config/elasticsearch.yml

# often reload scripts. TODO: check what the medium is needed for
echo 'resource.reload.interval.medium: 1s' >> src/test/resources/r-demo/$releasename/config/elasticsearch.yml

