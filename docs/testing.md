

curl -i -H "Content-Type: application/json" -X POST -d '{"username":"bob","email":"bob@radsense.com","password":"abc"}' http://localhost:3000/users

curl -i -H "Content-Type: application/json" -X POST -d '{"username":"mcastilho","email":"mcastilho@radsense.com","password":"abc"}' http://localhost:3000/users

curl -i -X POST -d '{"username":"bob","email":"bob@radsense.com","password":"abc"}' http://localhost:3000/users

curl -i -H "Content-Type: application/json" -X POST -d '{"login":"bob","password":"abc"}' http://localhost:3000/authorize

curl -i -H "Content-Type: application/json" -X GET \
    -H "Authorization: Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpYXQiOjE0MzUwMDcwMzcsInVpZCI6IjU1ODIxMGNhNTEwYzc2MjBkODAwMDAwMiIsInVuYW1lIjoiYm9iIn0.Ixc1aVhwvD0HpAAU4bBSsdHjUP_68K6ZCVOC5iFiYQ0" \
    http://localhost:3000/me

curl -i -H "Content-Type: application/json" -X GET \
    -H "Authorization: Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpYXQiOjE0MzQ1OTQ2NzQsInVpZCI6IjU1ODIxMGNhNTEwYzc2MjBkODAwMDAwMiIsInVuYW1lIjoiYm9iIn0.mjep_f09jZOBOpIrciExU35FRWUYJRSGau7SkvfvYew" \
    http://localhost:3000/users/bob


curl -i -H "Content-Type: application/json" -X GET \
    -H "Authorization: Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpYXQiOjE0MzQ1OTQ2NzQsInVpZCI6IjU1ODIxMGNhNTEwYzc2MjBkODAwMDAwMiIsInVuYW1lIjoiYm9iIn0.mjep_f09jZOBOpIrciExU35FRWUYJRSGau7SkvfvYew" \
    http://localhost:3000/users/mcastilho

curl -i -H "Content-Type: application/json" -X GET \
    -H "Authorization: Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpYXQiOjE0MzUwMDg2NzIsInVpZCI6IjU1ODg2MjZjNTEwYzc2ZGYwNTAwMDAwMiIsInVuYW1lIjoiYm9iIn0.xwZjNCm3R2DUlZZyunA6Vac-KNvLVjeFVwePI8-S3a0" \
    http://localhost:3000/me/emails

curl -i -H "Content-Type: application/json" -X POST \
    -H "Authorization: Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpYXQiOjE0MzUwMDg2NzIsInVpZCI6IjU1ODg2MjZjNTEwYzc2ZGYwNTAwMDAwMiIsInVuYW1lIjoiYm9iIn0.xwZjNCm3R2DUlZZyunA6Vac-KNvLVjeFVwePI8-S3a0" \
    -d '{"email": "bob2@gmail.com"}' \
    http://localhost:3000/me/emails
