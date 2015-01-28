mod sha1;

pub fn scramble(scr: &[u8], password: &[u8]) -> Option<Vec<u8>> {
    if password.len() == 0 {
        return None;
    }

    let sha_pass = sha1::sha1(password);
    let double_sha_pass = sha1::sha1(&sha_pass[]);
    let hash = sha1::sha1(&scr.to_vec()
                              .into_iter()
                              .chain(double_sha_pass.into_iter())
                              .collect::<Vec<u8>>()[]);

    let mut output = [0u8; 20];

    for i in 0us..20 {
        output[i] = sha_pass[i] ^ hash[i];
    }

    Some(output.to_vec())
}

#[cfg(test)]
mod test {
    use super::scramble;

    #[test]
    fn should_compute_scrambled_password() {
        let scr = [0x4e_u8, 0x52_u8, 0x33_u8, 0x48_u8, 0x50_u8, 0x3a_u8,
                   0x71_u8, 0x49_u8, 0x59_u8, 0x61_u8, 0x5f_u8, 0x39_u8,
                   0x3d_u8, 0x64_u8, 0x62_u8, 0x3f_u8, 0x53_u8, 0x64_u8,
                   0x7b_u8, 0x60_u8];
        let password = [0x47_u8, 0x21_u8, 0x69_u8, 0x64_u8, 0x65_u8,
                        0x72_u8, 0x32_u8, 0x37_u8];
        let output = scramble(&scr, &password);
        assert!(output.is_some());
        assert_eq!(output.unwrap(),
                   vec![0x09_u8, 0xcf_u8, 0xf8_u8, 0x85_u8, 0x5e_u8,
                        0x9e_u8, 0x70_u8, 0x53_u8, 0x40_u8, 0xff_u8,
                        0x22_u8, 0x70_u8, 0xd8_u8, 0xfb_u8, 0x9f_u8,
                        0xad_u8, 0xba_u8, 0x90_u8, 0x6b_u8, 0x70_u8]);
    }
}
