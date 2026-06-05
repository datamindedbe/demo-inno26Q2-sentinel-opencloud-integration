curl -s http://localhost:9090/readyz | jq .


JOHN=$(curl -s -X POST \
  http://localhost:8180/realms/s3sentinel/protocol/openid-connect/token \
  -d client_id=s3sentinel \
  -d username=john \
  -d password=admin123 \
  -d grant_type=password \
  | jq -r .access_token)


curl -X PUT http://localhost:8080/inno-days-test/first-product/private/report6.csv   -H "Authorization: Bearer $JOHN"   --upload-file /tmp/report.csv

JANE=$(curl -s -X POST \
  http://localhost:8180/realms/s3sentinel/protocol/openid-connect/token \
  -d client_id=s3sentinel \
  -d username=jane \
  -d password=reader123 \
  -d grant_type=password \
  | jq -r .access_token)

curl -X PUT http://localhost:8080/inno-days-test/first-product/private/report5.csv   -H "Authorization: Bearer $JANE"   --upload-file /tmp/report.csv



first-product
client_id: first-product
client_secret: 45eZojfuwgVH1pAUg0DQyvp4XTNyPgfnVcys0mfPIDBvVhAG7kae6Oq5f2I2pkNY


tok=$(curl --request POST \
  --url https://zitadel.sentinel.playground.dataminded.cloud/oauth/v2/token \
  --header 'Content-Type: application/x-www-form-urlencoded' \
  --data grant_type=client_credentials \
  --data scope='openid profile' \
  --user "first-product:45eZojfuwgVH1pAUg0DQyvp4XTNyPgfnVcys0mfPIDBvVhAG7kae6Oq5f2I2pkNY" | jq -r .id_token)

curl -X PUT http://localhost:8080/inno-days-test/first-product/private/report5.csv   -H "Authorization: Bearer $tok"   --upload-file /tmp/report.csv