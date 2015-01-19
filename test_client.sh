curl -L http://127.0.0.1:2379/version
echo 
curl -L http://127.0.0.1:4001/v2/keys/running/hello/123 -XPUT -d value="10.1.1.1:1080"
echo
