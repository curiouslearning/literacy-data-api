# literacy-data-api
a simple API for fetching data from literacy apps

# Overview

The API returns user data for a specific app `app_id`. It will return all events matching the id, and any of the optional provided parameters.

# Request

`app_id`: **REQUIRED** package name. returns 400 error if no id is supplied or if id is not formatted like `com.example.app`.

`attribution_id`: **OPTIONAL** A marketing attribution tag for filtering results. When provided data will only be returned for users that have been tagged with this id. If a user has been tagged with multiple attribution tags, making a request with *either* tag will return *all* events for that user that match the other provided parameters. (e.g. given user X with tags `recruitment_campaign_a` and `remarketing_campaign_1`, the requests `app_id=com.example.app&attribution_id=remarketing_campaign_1` and `app_id=com.example.app&attribution_id=recruitment_campaign_a` will both return all events for user X created by app `com.example.app`)

`from`: **OPTIONAL** A Unix timestamp in seconds-from-epoch. If provided, will only return events with timestamps equal to or greater than the provided value. Default is `0`.

`token`: **OPTIONAL** pagination token received from previous response. The API will only return the 1st 1000 results of a response set. If more results exists, make a second request with the included token in `nextCursor` to receive the next 1000 results.

`event`: **OPTIONAL** event name to filter by. If provided, return only events with a matching `name` parameter.

# Response

```
{
  "nextCursor": string || null,
  "data": [
    {
        "attribution_url": string,
        "app_id": string,
        "ordered_id": string,
        "user": {
          "id": string,
          "metadata": {
            "continent": string,
            "country": string,
            "region": string,
            "city": string,
          },
          "ad_attribution": {
            "source": enum [Facebook, Google, Direct],
            "data": {
              "advertising_id": string,
            },
          },
        },
        "event": {
          "name": string,
          "date": string {YYYMMDD},
          "timestamp": timestamp,
          "value_type": string,
          "value": string,
          "level": string,
          "profile": enum(0,1,2,3,"unknown"),
          "rawData": {
            "action": string,
            "label": string,
            "screen": string,
            "value": string,
          }
        }
      }
    ]
  }
```
events are returned in *ascending* order.

# Developer Instructions

Included here are notes on how to set up the development environment, create builds of the project, and any prerequisite software needed to get started

## Prerequisites

- Node v14.7x
- npm 6.12x
- [Docker Desktop](https://www.docker.com/) (for running the development environment)
- [kind CLI](https://kind.sigs.k8s.io/) for creating the development environment
- [Helm CLI ](https://helm.sh/docs/intro/install) (for creating and deploying production builds)
- [kubectl CLI](https://kubernetes.io/docs/tasks/tools/#kubectl) (for deploying builds)

## Getting Started

After ensuring you have all prerequisites installed, clone or download the project from this repository to your local machine. From the `/api` directory, run `npm install` to install all dependencies. 

## Unit Tests

From the `/api` directory run `npm run test`

## Database Access

In order to access BigQuery, you must have a Google Service Account with the proper permissions. Download the private key `.json` file for your account, place it in `/api/keys` (this directory is .gitignored for security purposes) and name it `bigquery-serviceaccount.json`. 

## Running the development environment

Change into the top level `/dev` directory (`cd dev`). From this directory, run `dev-env.sh` to create the development server in Docker Desktop. To build the project, run `dev.sh`. Follow the instructions in the terminal to begin port-forwarding in the cluster so you can access the application from localhost in your browser. You can rebuild the project at any time by re-running `dev.sh` and restarting port-forwarding.

__Tip__: To inspect the application logs while running the development environment run the following commands in a _new_ terminal window or tab:

1. `kubectl get pods` (returns a list of all K8s pods running)
2. `kubectl logs pods/{podName}` (show logs for the given pod)

## Deploying to Production

Before deploying to a production cluster for the first time, you must create a secret with your Google Service Account private key on the cluster. From your terminal, run the following command 

`kubectl create secret generic bigquery-serviceaccount --from-file={absolute/path/to/repo}/api/keys`

This command must be re-run only if your secret information changes, if you create a new cluster, or if the secret is somehow removed from your production cluster.

Once your secret is created, run the following command from the top level `helm-charts/` directory

`helm install --values values/prod.yaml {deployment-name} ./literacy-data-api`

You can check the status of the pods by running `kubectl get pods` and `kubectl describe pods/{podName}`