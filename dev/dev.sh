#!/bin/sh

helm uninstall lit-data
docker build -t localhost:5000/lit-data:registry ../api
docker push localhost:5000/lit-data:registry
helm install lit-data ../helm-charts/literacy-data-api
