# MongoDB Schema

``` json

"namespace" {
    "id": "23423452edas53",
    "path": "chatml",
    "owner": {
        "kind": "user",
        "id": "asdas231234kdsfhj"
    },
    "created_at": "20051023-12:00:00T5000",
    "updated_at": "20051023-12:00:00T5000"
}

"conversation" : {
    "id": "23423423",
    "namespace" : {
        "id": "212342134",
        "path": "chatml"
    },
    "title": "Nice Convo",
    "path": "cool-conversation",
    "topic": "Here you can find http://cool.io/ the documentation",
    "creator_id": "23423423",
    "owner_id": "23423423",
    "avatar": {
        "file_size": 213423,
        "original_filename": "foo.png",
        "content_type": "image/png",
        "md5": "2343453sd8a0123984",
        "sha256": "2343453sd8a0123984"
    },
    "conversation_type": "personal",
    "privacy": "public",
    "messages_count": 25,
    "participants_count": 4,
    "retention": {
        "mode": "by_day",
        "value": 365,
    },
    "last_active_at": "20051023-12:00:00T5000",
    "archived_at": null,
    "created_at": "20051023-12:00:00T5000",
    "updated_at": "20051023-12:00:00T5000",
    "participants": [
        {
            "user_id": "5467356", 
            "last_activity_at": "20051023-12:00:00T5000", 
            "joined_at": "20051023-12:00:00T5000"
        },
    ]
}

"organization" : {
    "id": "23423423452345234",
    "name": "Chatml Corporation",
    "namespace" : {
        "id": "212342134",
        "path": "chatml"
    },
    "members": [
        {"user_id": "5467356", "owner": true, "member_since": "20051023-12:00:00T5000"},
        {"user_id": "7467334", "member_since": "20051023-12:00:00T5000"},
        {"user_id": "3454673", "member_since": "20051023-12:00:00T5000"},
    ],
    "teams": [
        {"team_id": "7467334"},
        {"team_id": "3454673"},
    ],
}

"team" : {
    "id": "23452345",
    "organization_id": "234345623456",
    "name": "developers",
    "description": "this is the coolest team",
    "members": [
        {"user_id": "5467356", "member_since": "20051023-12:00:00T5000"},
        {"user_id": "7467334", "member_since": "20051023-12:00:00T5000"},
        {"user_id": "3454673", "member_since": "20051023-12:00:00T5000"},
    ],
    "created_at": "20051023-12:00:00T5000",
    "updated_at": "20051023-12:00:00T5000"
}
```
