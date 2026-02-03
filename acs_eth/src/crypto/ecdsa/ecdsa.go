package ecdsa

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"encoding/pem"
	"fmt"
	"io/ioutil"
	"path/filepath"
)

// GenerateEcdsaKey generates a pair of private and public keys and stores them in the given folder
func GenerateEcdsaKey(keystorePath string) (*ecdsa.PrivateKey, error) {
	priv, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return nil, err
	}
	pkcs8Encoded, err := x509.MarshalPKCS8PrivateKey(priv)
	if err != nil {
		return nil, err
	}
	pemEncoded := pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: pkcs8Encoded})
	keyFile := filepath.Join(keystorePath, "priv_sk")
	err = ioutil.WriteFile(keyFile, pemEncoded, 0600)
	if err != nil {
		return nil, err
	}
	return priv, err
}

// LoadKey loads the private key in the given path
func LoadKey(keystorePath string) (*ecdsa.PrivateKey, error) {
	keyBytes, err := ioutil.ReadFile(filepath.Join(keystorePath, "priv_sk"))
	if err != nil {
		return nil, err
	}
	pemBlock, _ := pem.Decode(keyBytes)
	key, err := x509.ParsePKCS8PrivateKey(pemBlock.Bytes)
	caKey := key.(*ecdsa.PrivateKey)
	return caKey, nil
}

// SignECDSA signs the digest of a message
func SignECDSA(k *ecdsa.PrivateKey, digest []byte) ([]byte, error) {
	r, s, err := ecdsa.Sign(rand.Reader, k, digest)
	if err != nil {
		return nil, err
	}
	s, err = ToLowS(&k.PublicKey, s)
	if err != nil {
		return nil, err
	}
	return MarshalECDSASignature(r, s)
}

// VerifyECDSA verifies the digest of a message
func VerifyECDSA(k *ecdsa.PublicKey, signature, digest []byte) (bool, error) {
	r, s, err := UnmarshalECDSASignature(signature)
	if err != nil {
		return false, fmt.Errorf("Failed unmashalling signature [%s]", err)
	}
	lowS, err := IsLowS(k, s)
	if err != nil {
		return false, err
	}
	if !lowS {
		return false, fmt.Errorf("invalid S. Must be smaller than half the order [%s][%s]", s, GetCurveHalfOrdersAt(k.Curve))
	}
	return ecdsa.Verify(k, digest, r, s), nil
}
