//! A statement's fingerprint and command, from its text alone.
//!
//! The fingerprint groups statements that differ only in their values:
//! string, number and dollar-quoted literals become `?`, comments go,
//! whitespace collapses and unquoted words fold to lower case, as
//! PostgreSQL folds them. It is a 64-bit FNV-1a hash of that; a quoted
//! identifier keeps its case. Parameters (`$1`) stay as they are, so a
//! prepared statement and its simple-protocol twin with literals share it.
//! No parse: this runs on the events' own thread for every statement.

use std::hash::Hasher;

use fnv::FnvHasher;

/// The fingerprint of `text`.
pub(crate) fn fingerprint(text: &str) -> u64 {
    let mut hasher = FnvHasher::default();
    normalize(text, |bytes| hasher.write(bytes));
    hasher.finish()
}

/// Where the statement's first keyword is (`select`, `WITH`, `COPY`).
pub(crate) fn command(text: &str) -> Option<(usize, usize)> {
    let bytes = text.as_bytes();
    let mut i = skip_space_and_comments(bytes, 0);
    while i < bytes.len() && bytes[i] == b'(' {
        i = skip_space_and_comments(bytes, i + 1);
    }
    let start = i;
    while i < bytes.len() && bytes[i].is_ascii_alphabetic() {
        i += 1;
    }
    (i > start).then_some((start, i))
}

fn skip_space_and_comments(bytes: &[u8], mut i: usize) -> usize {
    loop {
        while i < bytes.len() && bytes[i].is_ascii_whitespace() {
            i += 1;
        }
        match comment_end(bytes, i) {
            Some(end) => i = end,
            None => return i,
        }
    }
}

/// The end of a comment starting at `i`, if one does.
fn comment_end(bytes: &[u8], i: usize) -> Option<usize> {
    match (bytes.get(i), bytes.get(i + 1)) {
        (Some(b'-'), Some(b'-')) => {
            let mut j = i + 2;
            while j < bytes.len() && bytes[j] != b'\n' {
                j += 1;
            }
            Some(j)
        }
        (Some(b'/'), Some(b'*')) => {
            // Block comments nest in PostgreSQL.
            let (mut j, mut depth) = (i + 2, 1);
            while j < bytes.len() && depth > 0 {
                if bytes[j] == b'/' && bytes.get(j + 1) == Some(&b'*') {
                    depth += 1;
                    j += 2;
                } else if bytes[j] == b'*' && bytes.get(j + 1) == Some(&b'/') {
                    depth -= 1;
                    j += 2;
                } else {
                    j += 1;
                }
            }
            Some(j)
        }
        _ => None,
    }
}

fn is_word(byte: u8) -> bool {
    byte.is_ascii_alphanumeric() || byte == b'_' || byte == b'$' || byte >= 0x80
}

/// The end of a quoted run starting at `i` (the opening quote), with the
/// quote doubled to escape it, and backslashes escaping in `E''` strings.
fn quoted_end(bytes: &[u8], i: usize, quote: u8, backslash: bool) -> usize {
    let mut j = i + 1;
    while j < bytes.len() {
        if backslash && bytes[j] == b'\\' {
            j += 2;
            continue;
        }
        if bytes[j] == quote {
            if bytes.get(j + 1) == Some(&quote) {
                j += 2;
                continue;
            }
            return j + 1;
        }
        j += 1;
    }
    bytes.len()
}

/// A dollar-quote tag (`$$` or `$tag$`) starting at `i`: its length.
fn dollar_tag(bytes: &[u8], i: usize) -> Option<usize> {
    let mut j = i + 1;
    while j < bytes.len() && (bytes[j].is_ascii_alphanumeric() || bytes[j] == b'_') {
        if j == i + 1 && bytes[j].is_ascii_digit() {
            return None; // $1: a parameter
        }
        j += 1;
    }
    (bytes.get(j) == Some(&b'$')).then_some(j + 1 - i)
}

fn normalize(text: &str, mut out: impl FnMut(&[u8])) {
    let bytes = text.as_bytes();
    let mut i = 0;
    let mut space = false;
    let mut first = true;
    let mut emit = |bytes: &[u8], space: &mut bool, first: &mut bool| {
        if *space && !*first {
            out(b" ");
        }
        *space = false;
        *first = false;
        out(bytes);
    };

    while i < bytes.len() {
        let byte = bytes[i];
        if byte.is_ascii_whitespace() {
            space = true;
            i += 1;
            continue;
        }
        if let Some(end) = comment_end(bytes, i) {
            space = true;
            i = end;
            continue;
        }
        match byte {
            b'\'' => {
                i = quoted_end(bytes, i, b'\'', false);
                emit(b"?", &mut space, &mut first);
            }
            b'e' | b'E' if bytes.get(i + 1) == Some(&b'\'') && !prev_is_word(bytes, i) => {
                i = quoted_end(bytes, i + 1, b'\'', true);
                emit(b"?", &mut space, &mut first);
            }
            b'"' => {
                let end = quoted_end(bytes, i, b'"', false);
                emit(&bytes[i..end], &mut space, &mut first);
                i = end;
            }
            b'$' if !prev_is_word(bytes, i) && dollar_tag(bytes, i).is_some() => {
                let tag_len = dollar_tag(bytes, i).unwrap_or(1);
                let tag = &bytes[i..i + tag_len];
                let body = i + tag_len;
                let end = bytes[body..]
                    .windows(tag_len)
                    .position(|window| window == tag)
                    .map(|p| body + p + tag_len)
                    .unwrap_or(bytes.len());
                emit(b"?", &mut space, &mut first);
                i = end;
            }
            b'0'..=b'9' if !prev_is_word(bytes, i) => {
                let mut j = i;
                while j < bytes.len()
                    && (bytes[j].is_ascii_digit()
                        || bytes[j] == b'.'
                        || ((bytes[j] == b'e' || bytes[j] == b'E')
                            && bytes
                                .get(j + 1)
                                .is_some_and(|b| b.is_ascii_digit() || *b == b'-' || *b == b'+'))
                        || ((bytes[j] == b'-' || bytes[j] == b'+')
                            && j > i
                            && (bytes[j - 1] == b'e' || bytes[j - 1] == b'E')))
                {
                    j += 1;
                }
                emit(b"?", &mut space, &mut first);
                i = j;
            }
            _ if is_word(byte) => {
                let mut j = i;
                while j < bytes.len() && is_word(bytes[j]) {
                    j += 1;
                }
                let mut word = [0u8; 64];
                let run = &bytes[i..j];
                if run.len() <= word.len() {
                    for (k, b) in run.iter().enumerate() {
                        word[k] = b.to_ascii_lowercase();
                    }
                    emit(&word[..run.len()], &mut space, &mut first);
                } else {
                    emit(&run.to_ascii_lowercase(), &mut space, &mut first);
                }
                i = j;
            }
            _ => {
                emit(&bytes[i..i + 1], &mut space, &mut first);
                i += 1;
            }
        }
    }
}

fn prev_is_word(bytes: &[u8], i: usize) -> bool {
    i > 0 && is_word(bytes[i - 1])
}

#[cfg(test)]
mod test {
    use super::*;

    fn normalized(text: &str) -> String {
        let mut out = vec![];
        normalize(text, |bytes| out.extend_from_slice(bytes));
        String::from_utf8(out).unwrap()
    }

    #[test]
    fn test_values_go_and_the_shape_stays() {
        assert_eq!(
            normalized("SELECT  *\n FROM \"Users\" WHERE id = 42 AND name = 'o''neil' -- note\n"),
            r#"select * from "Users" where id = ? and name = ?"#
        );
        assert_eq!(
            normalized("insert into t values (1.5e-3, E'a\\'b', $$x$$, $tag$y$tag$, $1)"),
            "insert into t values (?, ?, ?, ?, $1)"
        );
        assert_eq!(
            normalized("select t1.c2 from t1 /* a /* nested */ one */"),
            "select t1.c2 from t1"
        );
    }

    #[test]
    fn test_fingerprint_groups_by_shape() {
        assert_eq!(
            fingerprint("SELECT * FROM t WHERE id = 1"),
            fingerprint("select *   from t where id = 2")
        );
        assert_ne!(
            fingerprint("SELECT a FROM t"),
            fingerprint("SELECT b FROM t")
        );
        assert_ne!(
            fingerprint(r#"SELECT "A" FROM t"#),
            fingerprint(r#"SELECT "a" FROM t"#)
        );
    }

    #[test]
    fn test_command() {
        let word = |text: &'static str| command(text).map(|(start, end)| &text[start..end]);
        assert_eq!(word("  select 1"), Some("select"));
        assert_eq!(
            word("/* hint */ -- x\nWITH a AS (select 1) select * from a"),
            Some("WITH")
        );
        assert_eq!(word("(SELECT 1) UNION (SELECT 2)"), Some("SELECT"));
        assert_eq!(word("   "), None);
    }
}
