package bls

import (
	"fmt"
	"io/ioutil"
	"log"
	"path/filepath"
	"strconv"

	"go.dedis.ch/kyber/v3"
	"go.dedis.ch/kyber/v3/pairing/bn256"
	"go.dedis.ch/kyber/v3/share"
	"go.dedis.ch/kyber/v3/sign/bls"
	"go.dedis.ch/kyber/v3/sign/tbls"
)

// BlsSig is related to BLS threshold signature
type BlsSig struct {
	prikey *share.PriShare
	pubkey *share.PubPoly
}

// GenerateBlsKey generates threshold BLS private and public keys,
// and stores coefficients in the given directory
func GenerateBlsKey(keystorePath string, n int, t int) error {
	suite := bn256.NewSuite()
	secret := suite.G1().Scalar().Pick(suite.RandomStream())
	priPoly := share.NewPriPoly(suite.G2(), t, secret, suite.RandomStream())
	for i, coeff := range priPoly.Coefficients() {
		keyFile := filepath.Join(keystorePath, "tbls_sk"+strconv.Itoa(i))
		bytes, err := coeff.MarshalBinary()
		if err != nil {
			return err
		}
		err = ioutil.WriteFile(keyFile, bytes, 0600)
		if err != nil {
			return err
		}
	}
	return nil
}

// LoadBlsKey loads the private and public keys in the given path
func LoadBlsKey(keystorePath string, n int, t int) (*share.PriPoly, *share.PubPoly, error) {
	suite := bn256.NewSuite()
	coeffs := make([]kyber.Scalar, t)
	for i := 0; i < t; i++ {
		keyBytes, err := ioutil.ReadFile(filepath.Join(keystorePath, "tbls_sk"+strconv.Itoa(i)))
		if err != nil {
			return nil, nil, err
		}
		if i == 0 {
			coeffs[i] = suite.G1().Scalar()
		} else {
			coeffs[i] = suite.G2().Scalar()
		}
		coeffs[i].UnmarshalBinary(keyBytes)
	}
	priPoly := share.CoefficientsToPriPoly(suite.G2(), coeffs)
	pubPoly := priPoly.Commit(suite.G2().Point().Base())
	return priPoly, pubPoly, nil
}

// InitBLS loads the private and public keys in the given path and return *BlsSig
func InitBLS(keystorePath string, n int, t int, id int) (*BlsSig, error) {
	suite := bn256.NewSuite()
	coeffs := make([]kyber.Scalar, t)
	for i := 0; i < t; i++ {
		keyBytes, err := ioutil.ReadFile(filepath.Join(keystorePath, "tbls_sk"+strconv.Itoa(i)))
		if err != nil {
			return nil, err
		}
		if i == 0 {
			coeffs[i] = suite.G1().Scalar()
		} else {
			coeffs[i] = suite.G2().Scalar()
		}
		coeffs[i].UnmarshalBinary(keyBytes)
	}
	priPoly := share.CoefficientsToPriPoly(suite.G2(), coeffs)
	pubPoly := priPoly.Commit(suite.G2().Point().Base())
	prishare := priPoly.Shares(n)[id]
	blsSig := &BlsSig{prikey: prishare, pubkey: pubPoly}
	return blsSig, nil
}

// Sign signs the corresponding part of threshold signature
func (blsSig *BlsSig) Sign(msg []byte) []byte {
	suite := bn256.NewSuite()
	sig, err := tbls.Sign(suite, blsSig.prikey, msg)
	if err != nil {
		fmt.Println("Error when tbls sign: ", err)
	}
	return sig
}

// Recover recovers the threshold signature
func (blsSig *BlsSig) Recover(msg []byte, sigShares [][]byte, t int, n int) []byte {
	suite := bn256.NewSuite()
	sig, err := tbls.Recover(suite, blsSig.pubkey, msg, sigShares[0:t], t, n)
	if err != nil {
		log.Fatal("Fatal: error when tbls recover: ", err)
	}
	err = bls.Verify(suite, blsSig.pubkey.Commit(), msg, sig)
	if err != nil {
		log.Panic("Error when tbls verify: ", err)
	}
	return sig
}
