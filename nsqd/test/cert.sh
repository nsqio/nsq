#!/bin/bash
# call this script with an email address (valid or not).
# like:
# ./cert.sh foo@foo.com
# Found: https://gist.github.com/ncw/9253562#file-makecert-sh

if [ "$1" == "" ]; then
    echo "Need email as argument"
    exit 1
fi

EMAIL=$1

rm -rf tmp 
mkdir tmp
cd tmp

echo "make CA"
PRIVKEY="test"
openssl req -new -x509 -days 365 -keyout ca.key -out ca.pem -subj "/C=DE/ST=NRW/L=Earth/O=Random Company/OU=IT/CN=www.random.com/emailAddress=KryptoKings@random.com" -passout pass:$PRIVKEY

echo "make server cert"
openssl req -new -nodes -x509 -out server.pem -keyout server.key -days 3650 -subj "/C=DE/ST=NRW/L=Earth/O=Random Company/OU=IT/CN=www.random.com/emailAddress=${EMAIL}"

echo "make client cert"
#openssl req -new -nodes -x509 -out client.pem -keyout client.key
#-days 3650 -subj "/C=DE/ST=NRW/L=Earth/O=Random
#Company/OU=IT/CN=www.random.com/emailAddress=${EMAIL}"

openssl genrsa -out client.key 2048
echo "00" > ca.srl
openssl req -sha1 -key client.key -new -out client.req -subj "/C=DE/ST=NRW/L=Earth/O=Random Company/OU=IT/CN=client.com/emailAddress=${EMAIL}"
# Adding -addtrust clientAuth makes certificates Go can't read
openssl x509 -req -days 365 -in client.req -CA ca.pem -CAkey ca.key -passin pass:$PRIVKEY -out client.pem # -addtrust clientAuth

openssl x509 -extfile ../openssl.conf -extensions ssl_client -req -days 365 -in client.req -CA ca.pem -CAkey ca.key -passin pass:$PRIVKEY -out client.pem

cd ..
mv tmp/* certs
rm -rf tmp
