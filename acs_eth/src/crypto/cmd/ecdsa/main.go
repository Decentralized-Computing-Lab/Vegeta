package main

import (
	"fmt"

	"go.themix.io/crypto/ecdsa"
	"go.themix.io/crypto/sha256"
)

func main() {
	_, err := ecdsa.GenerateEcdsaKey("./")
	if err != nil {
		fmt.Println("Failed to generate a key: ", err)
		return
	}
	priv, _ := ecdsa.LoadKey("./")
	hash, err := sha256.ComputeHash([]byte{'a', 'b', 'c'})
	if err != nil {
		fmt.Println("Failed to compute hash of a message: ", err)
	}
	sig, err := ecdsa.SignECDSA(priv, hash)
	if err != nil {
		fmt.Println("Failed to sign a mesage: ", err)
	}
	b, err := ecdsa.VerifyECDSA(&priv.PublicKey, sig, hash)
	if err != nil {
		fmt.Println("Failed to verify a message: ", err)
	}
	if !b {
		fmt.Println("Message verification failed")
	} else {
		fmt.Println("Message verification succeeded")
	}
}
