#!/bin/bash
# ./cert.sh foo@foo.com 127.0.0.1
# Found: https://gist.github.com/ncw/9253562#file-makecert-sh

if [ "$1" == "" ]; then
    echo "Need email as argument"
    exit 1
fi

if [ "$2" == "" ]; then
    echo "Need CN as argument"
    exit 1
fi

PRIVKEY="test"
EMAIL=$1
CN=$2

rm -rf tmp
mkdir tmp
cd tmp

echo "make CA"
openssl req -new -x509 -days 3650 -keyout ca.key -out ca.pem \
    -config ../openssl.conf -extensions ca \
    -subj "/CN=ca" \
    -passout pass:$PRIVKEY

echo "make server cert"
openssl genrsa -out server.key 2048
openssl req -new -sha256 -key server.key -out server.req \
    -subj "/emailAddress=${EMAIL}/C=DE/ST=NRW/L=Earth/O=Random Company/OU=IT/CN=${CN}"
openssl x509 -req -days 3650 -sha256 -in server.req -CA ca.pem -CAkey ca.key -CAcreateserial -passin pass:$PRIVKEY -out server.pem \
    -extfile ../openssl.conf -extensions server
    

echo "make client cert"
openssl genrsa -out client.key 2048
openssl req -new -sha256 -key client.key -out client.req \
    -subj "/emailAddress=${EMAIL}/C=DE/ST=NRW/L=Earth/O=Random Company/OU=IT/CN=${CN}"
openssl x509 -req -days 3650 -sha256 -in client.req -CA ca.pem -CAkey ca.key -CAserial ca.srl -passin pass:$PRIVKEY -out client.pem \
    -extfile ../openssl.conf -extensions client

cd ..
mv tmp/* certs
rm -rf tmp
