# literacy-data-api
a simple API for fetching data from literacy apps
```
Request: 
app_id=[id]&atribution_url=[url]&from=[ordered_id]


Response:
[
{ 
    “atribution_url”: string
    “app_id”: string,
    “ordered_id”: timestamp, 
    “user”: {
        “id”: string,
        “metadata”: {}, 
        “ad_attribution”: {
            “source”: enum[google_web, google_app, fb_web, fb_app, etc.],
            “data”: {} (fbclid, gclid, advertiser_id, etc.),
       }
    }
    “event”: {
        “timestamp”: timestamp,
        “variable”: string, 
        “value”: string,
        “value_type”: enum[continuous, ordinal, categorical], 
    }
}
]
```
