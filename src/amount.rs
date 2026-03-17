use crate::error::AppError;

pub fn parse_amount_to_cents(input: &str) -> Result<i64, AppError> {
    let amount_cents = parse_amount_core(input, false)?;
    if amount_cents <= 0 {
        return Err(AppError::Validation("amount must be positive".to_string()));
    }
    Ok(amount_cents)
}

pub fn parse_signed_amount_to_cents(input: &str) -> Result<i64, AppError> {
    let amount_cents = parse_amount_core(input, true)?;
    if amount_cents == 0 {
        return Err(AppError::Validation("amount cannot be zero".to_string()));
    }
    Ok(amount_cents)
}

pub fn parse_balance_to_cents(input: &str) -> Result<i64, AppError> {
    parse_amount_core(input, true)
}

fn parse_amount_core(input: &str, allow_negative: bool) -> Result<i64, AppError> {
    let trimmed = input.trim();
    if trimmed.is_empty() {
        return Err(AppError::Validation("amount cannot be empty".to_string()));
    }

    let (sign, unsigned) = if let Some(value) = trimmed.strip_prefix('-') {
        if !allow_negative {
            return Err(AppError::Validation("amount must be positive".to_string()));
        }
        (-1_i64, value)
    } else {
        (1_i64, trimmed.strip_prefix('+').unwrap_or(trimmed))
    };

    let mut parts = unsigned.split('.');
    let whole_part = parts
        .next()
        .ok_or_else(|| AppError::Validation("amount cannot be empty".to_string()))?;
    let fractional_part = parts.next();

    if parts.next().is_some() {
        return Err(AppError::Validation(
            "amount can contain at most one decimal point".to_string(),
        ));
    }

    if whole_part.is_empty() || !whole_part.chars().all(|ch| ch.is_ascii_digit()) {
        return Err(AppError::Validation(
            "amount must contain only digits and an optional decimal point".to_string(),
        ));
    }

    let dollars: i64 = whole_part.parse().map_err(|_| {
        AppError::Validation("amount is too large to fit in 64-bit cents".to_string())
    })?;

    let cents = match fractional_part {
        None => 0,
        Some(part) if part.is_empty() => 0,
        Some(part) if part.len() == 1 && part.chars().all(|ch| ch.is_ascii_digit()) => {
            part.parse::<i64>().map_err(|_| {
                AppError::Validation("amount is too large to fit in 64-bit cents".to_string())
            })? * 10
        }
        Some(part) if part.len() == 2 && part.chars().all(|ch| ch.is_ascii_digit()) => {
            part.parse::<i64>().map_err(|_| {
                AppError::Validation("amount is too large to fit in 64-bit cents".to_string())
            })?
        }
        Some(_) => {
            return Err(AppError::Validation(
                "amount can use at most two decimal places".to_string(),
            ))
        }
    };

    let absolute_cents = dollars
        .checked_mul(100)
        .and_then(|value| value.checked_add(cents))
        .ok_or_else(|| {
            AppError::Validation("amount is too large to fit in 64-bit cents".to_string())
        })?;

    sign.checked_mul(absolute_cents).ok_or_else(|| {
        AppError::Validation("amount is too large to fit in 64-bit cents".to_string())
    })
}

pub fn format_cents(amount_cents: i64) -> String {
    let sign = if amount_cents < 0 { "-" } else { "" };
    let absolute = amount_cents.abs();
    let units = absolute / 100;
    let cents = absolute % 100;
    format!("{sign}{units}.{cents:02}")
}

#[cfg(test)]
mod tests {
    use super::{
        format_cents, parse_amount_to_cents, parse_balance_to_cents, parse_signed_amount_to_cents,
    };

    #[test]
    fn parses_whole_amounts() {
        assert_eq!(parse_amount_to_cents("12").unwrap(), 1200);
    }

    #[test]
    fn parses_fractional_amounts() {
        assert_eq!(parse_amount_to_cents("12.5").unwrap(), 1250);
        assert_eq!(parse_amount_to_cents("12.50").unwrap(), 1250);
    }

    #[test]
    fn parses_signed_amounts() {
        assert_eq!(parse_signed_amount_to_cents("12.50").unwrap(), 1250);
        assert_eq!(parse_signed_amount_to_cents("-12.50").unwrap(), -1250);
    }

    #[test]
    fn parses_balance_amounts_with_zero_and_negative_values() {
        assert_eq!(parse_balance_to_cents("0.00").unwrap(), 0);
        assert_eq!(parse_balance_to_cents("-12.50").unwrap(), -1250);
    }

    #[test]
    fn rejects_negative_amounts() {
        assert!(parse_amount_to_cents("-1.00").is_err());
    }

    #[test]
    fn rejects_too_many_decimal_places() {
        assert!(parse_amount_to_cents("1.999").is_err());
    }

    #[test]
    fn formats_cents() {
        assert_eq!(format_cents(1234), "12.34");
        assert_eq!(format_cents(-987), "-9.87");
    }
}
