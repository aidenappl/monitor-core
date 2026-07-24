package tools

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/base64"
	"errors"
	"fmt"
	"io"

	"github.com/aidenappl/monitor-core/env"
)

// Encrypt seals plaintext with AES-256-GCM using env.CryptoKey (MON_CRYPTO_KEY,
// exactly 32 bytes). A fresh random nonce is prepended to the ciphertext and the
// whole thing is base64-encoded. Used by Phase 3 to encrypt SSO client secrets
// and cached IdP tokens at rest.
func Encrypt(text string) (string, error) {
	if len(env.CryptoKey) != 32 {
		return "", fmt.Errorf("MON_CRYPTO_KEY must be exactly 32 bytes for AES-256, got %d", len(env.CryptoKey))
	}

	block, err := aes.NewCipher([]byte(env.CryptoKey))
	if err != nil {
		return "", err
	}

	aesGCM, err := cipher.NewGCM(block)
	if err != nil {
		return "", err
	}

	nonce := make([]byte, aesGCM.NonceSize())
	if _, err := io.ReadFull(rand.Reader, nonce); err != nil {
		return "", err
	}

	ciphertext := aesGCM.Seal(nonce, nonce, []byte(text), nil)
	return base64.StdEncoding.EncodeToString(ciphertext), nil
}

// Decrypt reverses Encrypt.
func Decrypt(encrypted string) (string, error) {
	if len(env.CryptoKey) != 32 {
		return "", fmt.Errorf("MON_CRYPTO_KEY must be exactly 32 bytes for AES-256, got %d", len(env.CryptoKey))
	}

	data, err := base64.StdEncoding.DecodeString(encrypted)
	if err != nil {
		return "", err
	}

	block, err := aes.NewCipher([]byte(env.CryptoKey))
	if err != nil {
		return "", err
	}

	aesGCM, err := cipher.NewGCM(block)
	if err != nil {
		return "", err
	}

	nonceSize := aesGCM.NonceSize()
	if len(data) < nonceSize {
		return "", errors.New("ciphertext too short")
	}

	nonce, ciphertext := data[:nonceSize], data[nonceSize:]
	plaintext, err := aesGCM.Open(nil, nonce, ciphertext, nil)
	if err != nil {
		return "", err
	}

	return string(plaintext), nil
}
