package bls

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"go.dedis.ch/kyber/v3/pairing/bn256"
	"go.dedis.ch/kyber/v3/share"
	"go.dedis.ch/kyber/v3/sign/bls"
	"go.dedis.ch/kyber/v3/sign/tbls"
)

func TestTBLS(test *testing.T) {
	msg := []byte("Hello threshold Boneh-Lynn-Shacham")
	suite := bn256.NewSuite()
	n := 10
	t := n/2 + 1
	secret := suite.G1().Scalar().Pick(suite.RandomStream())
	priPoly := share.NewPriPoly(suite.G2(), t, secret, suite.RandomStream())
	pubPoly := priPoly.Commit(suite.G2().Point().Base())
	sigShares := make([][]byte, 0)
	for _, x := range priPoly.Shares(n) {
		sig, err := tbls.Sign(suite, x, msg)
		require.Nil(test, err)
		sigShares = append(sigShares, sig)

		err = tbls.Verify(suite, pubPoly, msg, sig)
		require.Nil(test, err)
	}

	sig, err := tbls.Recover(suite, pubPoly, msg, sigShares, t, n)
	require.Nil(test, err)

	err = bls.Verify(suite, pubPoly.Commit(), msg, sig)
	require.Nil(test, err)
}

func TestTBLSLoad(test *testing.T) {
	var err error
	msg := []byte("Hello threshold Boneh-Lynn-Shacham")
	n := 4
	t := 2
	priPoly, pubPoly, err := LoadBlsKey("", n, t)

	if err != nil {
		fmt.Println("load BLS private key shares error: ", err)
	}

	suite := bn256.NewSuite()
	sigShares := make([][]byte, 0)
	for _, x := range priPoly.Shares(n) {
		sig, err := tbls.Sign(suite, x, msg)
		require.Nil(test, err)
		sigShares = append(sigShares, sig)

		err = tbls.Verify(suite, pubPoly, msg, sig)
		require.Nil(test, err)
	}

	suite = bn256.NewSuite()
	// recover aggregated signature
	sig, err := tbls.Recover(suite, pubPoly, msg, sigShares[0:t], t, n)
	require.Nil(test, err)

	err = bls.Verify(suite, pubPoly.Commit(), msg, sig)
	require.Nil(test, err)
}
