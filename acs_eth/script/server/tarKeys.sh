#!/bin/bash

rm -rf crypto
rm crypto.tar.gz
mkdir crypto
cp ../../src/crypto/cmd/bls/tbls_sk* ./crypto/
tar -czf crypto.tar.gz ./crypto
