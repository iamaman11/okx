//! Bounds on amounts accepted by the hypothetical portfolio contract.

pub fn is_positive_decimal(value: &str) -> bool {
    let (whole, fraction) = match value.split_once('.') {
        Some((_, "")) => return false,
        Some(parts) => parts,
        None => (value, ""),
    };
    let mut digits = whole.bytes().chain(fraction.bytes());
    !whole.is_empty()
        && !(whole.len() > 1 && whole.starts_with('0'))
        && whole.len().saturating_add(fraction.len()) <= 28
        && digits.clone().all(|digit| digit.is_ascii_digit())
        && digits.any(|digit| digit != b'0')
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hypothetical_amounts_keep_exact_ascii_bounds() {
        for amount in [
            "5",
            "5.0",
            "5.00",
            "0.0000000001",
            "9999999999999999999999999999",
        ] {
            assert!(is_positive_decimal(amount), "{amount}");
        }
        for amount in [
            "",
            "0",
            "0.0",
            "-0",
            "1e-10",
            " 5",
            "NaN",
            "01",
            "+5",
            "1.",
            ".5",
            "5,00",
            "99999999999999999999999999999",
            "1.0000000000000000000000000000",
        ] {
            assert!(!is_positive_decimal(amount), "{amount}");
        }
    }
}
