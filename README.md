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
