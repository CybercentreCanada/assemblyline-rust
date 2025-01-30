use std::sync::OnceLock;


pub fn find_subsequence(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    haystack.windows(needle.len()).position(|window| window == needle)
}

pub fn safe_str(s: &str) -> String {
    escape_str(s, false)
}


// reversable=true
fn escape_str(s: &str, reversible: bool) -> String {
    // if isinstance(s, bytes):
    //     return escape_str_strict(s, reversible)
    // elif not isinstance(s, str):
    //     if force_str:
    //         return str(s)
    //     return s
    escape_str_strict(s.as_bytes(), reversible)
    // try:
    //     return escape_str_strict(s.encode("utf-16", 'surrogatepass').decode("utf-16").encode("utf-8"), reversible)
    // except Exception:
    //     return escape_str_strict(s.encode("utf-8", errors="backslashreplace"), reversible)
}

fn escape_bytes(s: &[u8], reversible: bool) -> String {
    escape_str_strict(s, reversible)
}

// According to wikipedia, RFC 3629 restricted UTF-8 to end at U+10FFFF.
// This removed the 6, 5 and (irritatingly) half of the 4 byte sequences.
//
// The start byte for 2-byte sequences should be a value between 0xc0 and
// 0xdf but the values 0xc0 and 0xc1 are invalid as they could only be
// the result of an overlong encoding of basic ASCII characters. There
// are similar restrictions on the valid values for 3 and 4-byte sequences.
// _valid_utf8 = re.compile(rb"""((?:
//     [\x09\x0a\x0d\x20-\x7e]|         # 1-byte (ASCII excluding control chars).
//     [\xc2-\xdf][\x80-\xbf]|          # 2-bytes (excluding overlong sequences).
//     [\xe0][\xa0-\xbf][\x80-\xbf]|    # 3-bytes (excluding overlong sequences).

//     [\xe1-\xec][\x80-\xbf]{2}|       # 3-bytes.
//     [\xed][\x80-\x9f][\x80-\xbf]|    # 3-bytes (up to invalid code points).
//     [\xee-\xef][\x80-\xbf]{2}|       # 3-bytes (after invalid code points).

//     [\xf0][\x90-\xbf][\x80-\xbf]{2}| # 4-bytes (excluding overlong sequences).
//     [\xf1-\xf3][\x80-\xbf]{3}|       # 4-bytes.
//     [\xf4][\x80-\x8f][\x80-\xbf]{2}  # 4-bytes (up to U+10FFFF).
//     )+)""", re.VERBOSE)

fn valid_utf8() -> &'static regex::bytes::Regex {
    static VALID: OnceLock<regex::bytes::Regex> = OnceLock::new();
    VALID.get_or_init(|| {
        regex::bytes::Regex::new("((?:[\\x09\\x0a\\x0d\\x20-\\x7e]|[\\xc2-\\xdf][\\x80-\\xbf]|[\\xe0][\\xa0-\\xbf][\\x80-\\xbf]|[\\xe1-\\xec][\\x80-\\xbf]{2}|[\\xed][\\x80-\\x9f][\\x80-\\xbf]|[\\xee-\\xef][\\x80-\\xbf]{2}|[\\xf0][\\x90-\\xbf][\\x80-\\xbf]{2}|[\\xf1-\\xf3][\\x80-\\xbf]{3}|[\\xf4][\\x80-\\x8f][\\x80-\\xbf]{2})+)").unwrap()
    })
}

// // reversible=True
fn escape_str_strict(s: &[u8], reversible: bool) -> String {
    let mut bytes = Vec::with_capacity(s.len());
    for (index, codepoint) in valid_utf8().split(s).enumerate() {
        bytes.extend(_escape(index, codepoint, reversible));
    }
    String::from_utf8_lossy(&bytes).to_string()
}

fn _escape(index: usize, code: &[u8], reversible: bool) -> Vec<u8> {
    let mut out = vec![];
    if index % 2 != 0 {
        if reversible {
            for byte in code {
                if *byte == b'\\' {
                    out.extend_from_slice(b"\\\\");
                } else {
                    out.push(*byte)
                }
            }
        } else { 
            out.extend_from_slice(code)
        }
    } else {
        for x in code {
            out.extend(format!("\\x{x:02x}").as_bytes());
        }
    }
    out
}

pub fn dotdump_bytes(s: &[u8]) -> String {
    let mut out = String::with_capacity(s.len());
    for &byte in s {
        if !(32..=126).contains(&byte) {
            out.push('.');
        } else if let Some(char) = char::from_u32(byte as u32) {
            out.push(char);
        } else {
            out.push('.');
        }
    }
    out
}

pub fn dotdump(s: &str) -> String {
    dotdump_bytes(s.as_bytes())
}

#[cfg(test)]
mod test {

    use crate::string_utils::dotdump_bytes;

    // def test_named_constants():
    //     named_const_test = str_utils.NamedConstants("test", [
    //         ("a", 1),
    //         ("B", 2),
    //         ("c", 3)
    //     ])
    //     # success tests
    //     assert named_const_test.name_for_value(1) == 'a'
    //     assert named_const_test.contains_value(1) is True
    //     assert named_const_test['a'] == 1
    //     assert named_const_test.c == 3
    //     # failure tests
    //     assert named_const_test.contains_value(4) is False
    //     with pytest.raises(KeyError):
    //         assert named_const_test.name_for_value(4) is None
    //         assert named_const_test['b'] == 2
    //         assert named_const_test.C == 3

    #[test]
    fn test_dotdump() {
        let result = dotdump_bytes(&[1, 8, 22, 33, 66, 99, 126, 127, 255]);
        assert_eq!(result, "...!Bc~..");
    }

    // #[test]
    // fn test_safe_str() {
    //     let test_str = "helloÌ\x02Í\u{dcf9}";
    //     let test_bytes = b"hello\xc3\x8c\x02\xc3\x8d\u{dcf9}";
    //     let expected_result = "hello\xcc\\x02\xcd\\udcf9";
    //     // let expected_result = [104, 101, 108, 108, 111, 195, 140, 92, 120, 48, 50, 195, 141, 92, 117, 100, 99, 102, 57]

    //     assert_eq!(escape_bytes(test_bytes, false), expected_result);
    //     assert_eq!(safe_str(test_str), expected_result);
    // }

    // #[test]
    // fn test_safe_str_emoji() {
    //     let test_str = "Smile! \u{d83d}\u{de00}";
    //     let test_bytes = b"Smile! \xf0\x9f\x98\x80";
    //     let expected_result = "Smile! 😀";

    //     assert_eq!(escape_bytes(test_bytes, false), expected_result);
    //     assert_eq!(safe_str(test_str), expected_result);
    // }

    // def test_translate_str():
    //     teststr = 'Стамболийски'
    //     encoded_test_str = teststr.encode('ISO-8859-5')
    //     result = str_utils.translate_str(encoded_test_str)
    //     assert result['language'] == 'Bulgarian'
    //     assert result['encoding'] == 'ISO-8859-5'
    //     result = str_utils.translate_str('abcdéfg')
    //     assert result['language'] == 'unknown'
    //     assert result['encoding'] == 'utf-8'


    // def test_truncate():
    //     # Bytes are converted to strings
    //     assert str_utils.truncate(b"blah") == "blah"
    //     # Strings are handled normally
    //     assert str_utils.truncate("blah") == "blah"
    //     # Long strings are truncated at a length of 100 by default
    //     assert str_utils.truncate(
    //         "blahblahblahblahblahblahblahblahblahblahblahblahblahblahblahblahblahblahblahblahblahblahblahblahblahblahblah"
    //     ) == "blahblahblahblahblahblahblahblahblahblahblahblahblahblahblahblahblahblahblahblahblahblahblahblahblah..."
    //     # You can also decide the length at which to truncate
    //     assert str_utils.truncate(b"blahblahblahblah", 10) == "blahblahbl..."
    //     # We should handle other value types also
    //     assert str_utils.truncate(1234) == "1234"
    //     assert str_utils.truncate(1234.1) == "1234.1"
    //     assert str_utils.truncate(12345678901234567890, 15) == "123456789012345..."


    // def test_remove_bidir_unicode_controls():
    //     test_str = 'a\u202Db\u202Ac\u200Ed\u200Fe\u202Efg\u202B'
    //     assert str_utils.remove_bidir_unicode_controls(test_str) == 'abcdefg'

    //     other_test_str = 'abcdéfg'
    //     assert str_utils.remove_bidir_unicode_controls(other_test_str) == 'abcdéfg'


    // def test_wrap_bidir_unicode_string():
    //     test_str = 'a\u202Db\u202Acde\u202Efg\u202B'
    //     a = str_utils.wrap_bidir_unicode_string(test_str)
    //     assert a == '\u202aa\u202db\u202Acde\u202efg\u202b\u202c\u202c\u202c\u202c\u202c'

    //     byte_str = b'\u202Dabcdefg'
    //     assert str_utils.wrap_bidir_unicode_string(byte_str) == b'\u202Dabcdefg'

    //     fail_search_str = 'abcdefg'
    //     assert str_utils.wrap_bidir_unicode_string(fail_search_str) == 'abcdefg'

    //     already_closed_str = 'abc\u202Adef\u202cg'
    //     assert str_utils.wrap_bidir_unicode_string(already_closed_str) == '\u202Aabc\u202Adef\u202cg\u202C'
}