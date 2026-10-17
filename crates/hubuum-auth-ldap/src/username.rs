use stringprep::tables::case_fold_for_nfkc;
use unicode_normalization::{UnicodeNormalization, char::is_combining_mark};

/// Group equivalent LDAP case-ignore usernames for login throttling.
///
/// Applies RFC 4518 character mapping, case folding, compatibility normalization,
/// and insignificant-space handling. This is a throttle key, not an identity or
/// authentication decision: the original username must still be sent to LDAP.
pub fn login_rate_limit_key(username: &str) -> String {
    let normalized: String = username
        .chars()
        .filter_map(map_character)
        .flat_map(case_fold_for_nfkc)
        .nfkc()
        .collect();
    let mut characters = normalized.chars().peekable();
    let mut key = String::with_capacity(normalized.len());
    let mut pending_space = false;
    while let Some(character) = characters.next() {
        // A space with a combining mark is significant (RFC 4518 section 2.6.1).
        if character == ' '
            && characters
                .peek()
                .is_none_or(|next| !is_combining_mark(*next))
        {
            pending_space = !key.is_empty();
        } else {
            if pending_space {
                key.push(' ');
                pending_space = false;
            }
            key.push(character);
        }
    }
    key
}

fn map_character(character: char) -> Option<char> {
    // RFC 4518 section 2.2 defines these lists independently of the host's
    // Unicode version. In particular, format controls also map to nothing.
    match character {
        '\u{0009}'..='\u{000d}'
        | '\u{0085}'
        | '\u{0020}'
        | '\u{00a0}'
        | '\u{1680}'
        | '\u{2000}'..='\u{200a}'
        | '\u{2028}'
        | '\u{2029}'
        | '\u{202f}'
        | '\u{205f}'
        | '\u{3000}' => Some(' '),
        '\u{00ad}'
        | '\u{1806}'
        | '\u{034f}'
        | '\u{180b}'..='\u{180d}'
        | '\u{fe00}'..='\u{fe0f}'
        | '\u{fffc}'
        | '\u{200b}'
        | '\u{0000}'..='\u{0008}'
        | '\u{000e}'..='\u{001f}'
        | '\u{007f}'..='\u{0084}'
        | '\u{0086}'..='\u{009f}'
        | '\u{06dd}'
        | '\u{070f}'
        | '\u{180e}'
        | '\u{200c}'..='\u{200f}'
        | '\u{202a}'..='\u{202e}'
        | '\u{2060}'..='\u{2063}'
        | '\u{206a}'..='\u{206f}'
        | '\u{feff}'
        | '\u{fff9}'..='\u{fffb}'
        | '\u{1d173}'..='\u{1d17a}'
        | '\u{e0001}'
        | '\u{e0020}'..='\u{e007f}' => None,
        _ => Some(character),
    }
}

#[cfg(test)]
mod tests {
    use rstest::rstest;

    use super::login_rate_limit_key;

    #[rstest]
    #[case::ascii(" Alice ", "alice")]
    #[case::soft_hyphen("al\u{00ad}ice", "alice")]
    #[case::format_control("al\u{200e}ice", "alice")]
    #[case::variation_selector("al\u{fe0f}ice", "alice")]
    #[case::tag("al\u{e0061}ice", "alice")]
    #[case::case_fold("Straße", "strasse")]
    #[case::compatibility("Ａｌｉｃｅ", "alice")]
    #[case::composition("E\u{0301}lodie", "élodie")]
    #[case::spaces(" Alice\t\u{00a0} Smith\n", "alice smith")]
    #[case::empty("", "")]
    fn equivalent_directory_names_share_a_key(#[case] username: &str, #[case] expected: &str) {
        assert_eq!(login_rate_limit_key(username), expected);
    }

    #[test]
    fn space_with_combining_mark_remains_significant() {
        assert_ne!(
            login_rate_limit_key(" \u{0301}alice"),
            login_rate_limit_key("\u{0301}alice")
        );
    }
}
