
use hmac::{Hmac, Mac, digest::KeyInit};
use subtle::ConstantTimeEq;

pub fn validate_hmac(
    signing_secret: &str,
    timestamp: &str,
    body: &str,
    signature: &str,
) -> bool {
    let mac = <Hmac<sha2::Sha256> as KeyInit>::new_from_slice(signing_secret.as_bytes());
    let mut mac = match mac {
        Ok(m) => m,
        Err(_) => return false,
    };
    let payload = format!("{}{}", timestamp, body);
    mac.update(payload.as_bytes());
    let expected = hex::encode(mac.finalize().into_bytes());
    expected.as_bytes().ct_eq(signature.as_bytes()).into()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_secret_with_anything_does_not_match() {
        assert!(!validate_hmac("", "1700000000", "{}", "deadbeef"));
    }

    #[test]
    fn known_signature_matches() {
        let secret = "testsecret";
        let timestamp = "1700000000";
        let body = "{}";
        let mac = <Hmac<sha2::Sha256> as KeyInit>::new_from_slice(secret.as_bytes()).unwrap();
        let mut mac = mac;
        mac.update(format!("{}{}", timestamp, body).as_bytes());
        let sig = hex::encode(mac.finalize().into_bytes());
        assert!(validate_hmac(secret, timestamp, body, &sig));
    }

    #[test]
    fn mismatched_signature_rejected() {
        let secret = "testsecret";
        assert!(!validate_hmac(secret, "1700000000", "{}", "0000000000000000000000000000000000000000000000000000000000000000"));
    }
}
