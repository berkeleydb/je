#!/bin/sh

rm keys.store

pw=unittest

# make self-signed key pair
keytool -genkeypair -keystore keys.store -storepass ${pw} -keypass ${pw} \
        -alias mykey -dname 'cn="Unit Test"' -keyAlg RSA -validity 36500

# export cert
rm mykey.cert
keytool -export -alias mykey -file mykey.cert -keystore keys.store \
        -storepass ${pw}

# make a second self-signed key pair
keytool -genkeypair -keystore keys.store -storepass ${pw} -keypass ${pw} \
        -alias otherkey1 -dname 'cn="Other Test 1"' -keyAlg RSA -validity 36500

# export cert
rm otherkey2.cert
keytool -export -alias otherkey1 -file otherkey1.cert -keystore keys.store \
        -storepass ${pw}

# make a third self-signed key pair
# this one is not added to the truststore
keytool -genkeypair -keystore keys.store -storepass ${pw} -keypass ${pw} \
        -alias otherkey2 -dname 'cn="Other Test 2"' -keyAlg RSA -validity 36500

# export cert
rm otherkey2.cert
keytool -export -alias otherkey2 -file otherkey2.cert -keystore keys.store \
        -storepass ${pw}

rm trust.store

# import mykey.cert and otherkey1.cert into truststore
keytool -import -alias mykey -file mykey.cert -keystore trust.store \
        -storepass ${pw} -noprompt
keytool -import -alias otherkey1 -file otherkey1.cert -keystore trust.store \
        -storepass ${pw} -noprompt

