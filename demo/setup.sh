#!/bin/bash


# download and unpack elasticsearch
RELEASE_VERSION=5.0.2
RELEASE_NAME=elasticsearch-${RELEASE_VERSION}
wget -nc https://artifacts.elastic.co/downloads/elasticsearch/${RELEASE_NAME}.tar.gz
tar xvf ${RELEASE_NAME}.tar.gz

# download and unpack kibana
wget -nc https://artifacts.elastic.co/downloads/kibana/kibana-${RELEASE_VERSION}-darwin-x86_64.tar.gz
tar xvf kibana-${RELEASE_VERSION}-darwin-x86_64.tar.gz

# build plugin
cd ..
git fetch
git checkout master
/Users/britta/gradle-2.13/bin/gradle clean assemble

echo "$(pwd)"


# demo-resources/$RELEASE_NAME/bin/elasticsearch-plugin remove ml
cd demo
currentpath="$(pwd)"
yes | ./$RELEASE_NAME/bin/elasticsearch-plugin remove ml
yes | ./${RELEASE_NAME}/bin/elasticsearch-plugin install "file://localhost$currentpath/../build/distributions/ml-$RELEASE_VERSION.zip"


# enable dynamic scripting for pmml
echo 'script.engine.pmml_model.stored: true' >> ./$RELEASE_NAME/config/elasticsearch.yml
echo 'script.engine.pmml_model.inline: true' >> ./$RELEASE_NAME/config/elasticsearch.yml
echo 'script.engine.pmml_model.file: true' >> ./$RELEASE_NAME/config/elasticsearch.yml

# generated models can be quite large
echo 'script.max_size_in_bytes: 1000000000' >> ./$RELEASE_NAME/config/elasticsearch.yml

# often reload scripts. TODO: check what the medium is needed for
echo 'resource.reload.interval.medium: 1s' >> ./$RELEASE_NAME/config/elasticsearch.yml
