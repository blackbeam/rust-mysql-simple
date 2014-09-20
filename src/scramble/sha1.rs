use std::io::{BufReader, BufWriter};

static K1: u32 = 0x5A827999u32;
static K2: u32 = 0x6ED9EBA1u32;
static K3: u32 = 0x8F1BBCDCu32;
static K4: u32 = 0xCA62C1D6u32;

fn circular_shift(bits: u32, word: u32) -> u32 {
    return word << (bits as uint) | word >> ((32u32 - bits) as uint);
}

#[allow(unused_must_use)]
pub fn sha1(message: &[u8]) -> Vec<u8> {
    let mut hash: [u32, ..5] = [0x67452301,
                                0xEFCDAB89,
                                0x98BADCFE,
                                0x10325476,
                                0xC3D2E1F0];
    let mut msg = Vec::from_slice(message);
    let msg_bit_len = msg.len() * 8;
    let offset = (msg.len() * 8) % 512;
    if offset < 448 {
        msg.push(128u8);
        for _ in range(0, (448 - (offset + 8)) / 8) {
            msg.push(0u8);
        }
    } else if offset >= 448 {
        msg.push(128u8);
        for _ in range(0, (512 - (offset + 8)) / 8 + 56) {
            msg.push(0u8);
        }
    }
    {
        msg = msg.append([0u8, ..8]);
        let len = msg.len();
        let mut writer = BufWriter::new(msg.slice_from_mut(len - 8));
        writer.write_be_u64(msg_bit_len as u64);
    }

    for i in range(0, msg.len() * 8 / 512) {
        let mut w = [0u32, ..80];
        let part = msg.slice(i * 64, (i+1) * 64);
        {
            let mut reader = BufReader::new(part);
            for j in range(0u, 16u) {
                w[j] = reader.read_be_u32().unwrap();
            }
        }
        for j in range(16u, 80u) {
            let val = w[j - 3] ^ w[j - 8] ^ w[j - 14] ^ w[j - 16];
            w[j] = circular_shift(1, val);
        }
        let mut a = hash[0];
        let mut b = hash[1];
        let mut c = hash[2];
        let mut d = hash[3];
        let mut e = hash[4];
        let mut temp: u32;
        for t in range(0u, 20u) {
            temp = circular_shift(5, a) + (b & c | !b & d) + e + w[t] + K1;
            e = d;
            d = c;
            c = circular_shift(30, b);
            b = a;
            a = temp;
        }
        for t in range(20u, 40u) {
            temp = circular_shift(5, a) + (b ^ c ^ d) + e + w[t] + K2;
            e = d;
            d = c;
            c = circular_shift(30, b);
            b = a;
            a = temp;
        }
        for t in range(40u, 60u) {
            temp = circular_shift(5, a) + (b & c | b & d | c & d) + e + w[t] + K3;
            e = d;
            d = c;
            c = circular_shift(30, b);
            b = a;
            a = temp;
        }
        for t in range(60u, 80u) {
            temp = circular_shift(5, a) + (b ^ c ^ d) + e + w[t] + K4;
            e = d;
            d = c;
            c = circular_shift(30, b);
            b = a;
            a = temp;
        }
        hash[0] += a;
        hash[1] += b;
        hash[2] += c;
        hash[3] += d;
        hash[4] += e;
    }

    let mut output = [0u8, ..20];
    {
        let mut writer = BufWriter::new(output);
        writer.write_be_u32(hash[0]);
        writer.write_be_u32(hash[1]);
        writer.write_be_u32(hash[2]);
        writer.write_be_u32(hash[3]);
        writer.write_be_u32(hash[4]);
    }
    Vec::from_slice(output)
}

#[cfg(test)]
mod test {
    use super::sha1;
    #[test]
    fn test_sha1() {
        assert_eq!(sha1([115u8, 104u8, 97u8]), vec![0xd8u8, 0xf4u8, 0x59u8, 0x03u8, 0x20u8, 0xe1u8,
                                                    0x34u8, 0x3au8, 0x91u8, 0x5bu8, 0x63u8, 0x94u8,
                                                    0x17u8, 0x06u8, 0x50u8, 0xa8u8,  0xf3u8, 0x5du8,
                                                    0x69u8, 0x26u8]);
        assert_eq!(sha1([65u8, ..57]), vec![0xe8u8, 0xd6u8, 0xeau8, 0x5cu8, 0x62u8, 0x7fu8, 0xc8u8,
                                            0x67u8, 0x6fu8, 0xa6u8, 0x62u8, 0x67u8, 0x7bu8, 0x02u8,
                                            0x86u8, 0x40u8, 0x84u8, 0x4du8, 0xc3u8, 0x5cu8]);
        assert_eq!(sha1([65u8, ..56]), vec![0x6bu8, 0x45u8, 0xe3u8, 0xcfu8, 0x1eu8, 0xb3u8, 0x32u8,
                                            0x4bu8, 0x9fu8, 0xd4u8, 0xdfu8, 0x3bu8, 0x83u8, 0xd8u8,
                                            0x9cu8, 0x4cu8, 0x2cu8, 0x4cu8, 0xa8u8, 0x96u8]);
        assert_eq!(sha1([65u8, ..64]), vec![0x30u8, 0xb8u8, 0x6eu8, 0x44u8, 0xe6u8, 0x00u8, 0x14u8,
                                            0x03u8, 0x82u8, 0x7au8, 0x62u8, 0xc5u8, 0x8bu8, 0x08u8,
                                            0x89u8, 0x3eu8, 0x77u8, 0xcfu8, 0x12u8, 0x1fu8]);
        assert_eq!(sha1([65u8, ..65]), vec![0x82u8, 0x6bu8, 0x7eu8, 0x7au8, 0x7au8, 0xf8u8, 0xa5u8,
                                            0x29u8, 0xaeu8, 0x1cu8, 0x74u8, 0x43u8, 0xc2u8, 0x3bu8,
                                            0xf1u8, 0x85u8, 0xc0u8, 0xadu8, 0x44u8, 0x0cu8]);
    }
}

