pub mod sha1;
use sha2::{Sha256, Digest};

pub fn scramble_native(scr: &[u8], password: &[u8]) -> Option<Vec<u8>> {
    if password.len() == 0 {
        return None;
    }

    let sha_pass = sha1::sha1(password);
    let double_sha_pass = sha1::sha1(&sha_pass[..]);
    let hash = sha1::sha1(
        &scr.to_vec()
            .into_iter()
            .chain(double_sha_pass.into_iter())
            .collect::<Vec<u8>>()[..],
    );

    let mut output = [0u8; 20];

    for i in 0usize..20 {
        output[i] = sha_pass[i] ^ hash[i];
    }

    Some(output.to_vec())
}

/// Scramble algorithm used in cached_sha2_password fast path.
///
/// XOR(SHA256(password), SHA256(SHA256(SHA256(password)), nonce))
pub fn scramble_sha256(nonce: &[u8], password: &[u8]) -> Option<Vec<u8>> {
    fn sha256_1(bytes: impl AsRef<[u8]>) -> Vec<u8> {
        let mut hasher = Sha256::default();
        hasher.input(bytes.as_ref());
        Vec::from(&hasher.result()[..])
    }

    fn sha256_2(bytes1: impl AsRef<[u8]>, bytes2: impl AsRef<[u8]>) -> Vec<u8> {
        let mut hasher = Sha256::default();
        hasher.input(bytes1.as_ref());
        hasher.input(bytes2.as_ref());
        Vec::from(&hasher.result()[..])
    }

    fn xor(left: impl AsRef<[u8]>, right: impl AsRef<[u8]>) -> Vec<u8> {
        let left = left.as_ref();
        let right = right.as_ref();
        assert_eq!(left.len(), right.len());

        left.iter().zip(right.iter()).map(|(l, r)| l ^ r).collect()
    }

    if password.len() == 0 {
        return None;
    }

    Some(xor(sha256_1(password), sha256_2(sha256_1(sha256_1(password)), nonce)))
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn should_compute_scrambled_password() {
        let scr = [
            0x4e, 0x52, 0x33, 0x48, 0x50, 0x3a, 0x71, 0x49,
            0x59, 0x61, 0x5f, 0x39, 0x3d, 0x64, 0x62, 0x3f,
            0x53, 0x64, 0x7b, 0x60,
        ];
        let password = [0x47, 0x21, 0x69, 0x64, 0x65, 0x72, 0x32, 0x37];
        let output1 = scramble_native(&scr, &password);
        let output2 = scramble_sha256(&scr, &password);
        assert!(output1.is_some());
        assert!(output2.is_some());
        assert_eq!(
            output1.unwrap(),
            vec![
                0x09, 0xcf, 0xf8, 0x85, 0x5e, 0x9e, 0x70, 0x53,
                0x40, 0xff, 0x22, 0x70, 0xd8, 0xfb, 0x9f, 0xad,
                0xba, 0x90, 0x6b, 0x70,
            ]
        );
        assert_eq!(
            output2.unwrap(),
            vec![
                0x4f, 0x97, 0xbb, 0xfd, 0x20, 0x24, 0x01, 0xc4, 0x2a, 0x69, 0xde, 0xaa,
                0xe5, 0x3b, 0xda, 0x07, 0x7e, 0xd7, 0x57, 0x85, 0x63, 0xc1, 0xa8, 0x0e,
                0xb8, 0x16, 0xc8, 0x21, 0x19, 0xb6, 0x8d, 0x2e,
            ]
        );
    }
}
