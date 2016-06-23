"""
Schemas for Event and Impression input data structures
Events (events.csv)

Num	Name            Type            Description
=== ==============  =============== ======================================
1	timestamp	    integer	        Unix timestamp when the event happened.
2	event_id	    string (UUIDv4)	Unique ID for the event.
3	advertiser_id	integer	        The advertiser ID that the user interacted with.
4	user_id	        string (UUIDv4)	An anonymous user ID that generated the event.
5	event_type	    string	        The type of event. Potential values: click, visit, purchase


Impressions (impressions.csv)

Num Name            Type            Description
=== ==============  =============== ======================================
1	timestamp	    integer	        Unix timestamp when the impression was served.
2	advertiser_id	integer	        The advertiser ID that owns the ad that was displayed.
3	creative_id	    integer	        The creative (or ad) ID that was displayed.
4	user_id	        string (UUIDv4)	An anonymous user ID this ad was displayed to.
"""

event_columns = dict(
    timestamp=0,
    event_id=1,
    advertiser_id=2,
    user_id=3,
    event_type=4
)

impression_columns = dict(
    timestamp=0,
    advertiser_id=1,
    creative_id=2,
    user_id=3
)
