pub(super) fn normalize_seconds_nanos(second: i64, nanos: i64) -> Option<(i64, u32)> {
    let mut seconds = second.checked_add(nanos / 1_000_000_000)?;
    let mut nanos = nanos % 1_000_000_000;
    if nanos < 0 {
        seconds = seconds.checked_sub(1)?;
        nanos += 1_000_000_000;
    }
    Some((seconds, nanos.try_into().unwrap()))
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_normalize_seconds_nanos() {
        assert_eq!(normalize_seconds_nanos(0, 0), Some((0, 0)));
        assert_eq!(normalize_seconds_nanos(0, 1_000_000_000), Some((1, 0)));
        assert_eq!(normalize_seconds_nanos(0, -1_000_000_000), Some((-1, 0)));
        assert_eq!(normalize_seconds_nanos(0, 1_000_000_001), Some((1, 1)));
        assert_eq!(
            normalize_seconds_nanos(0, -1_000_000_001),
            Some((-2, 999_999_999))
        );

        assert_eq!(normalize_seconds_nanos(1, 0), Some((1, 0)));
        assert_eq!(normalize_seconds_nanos(1, 1_000_000_000), Some((2, 0)));
        assert_eq!(normalize_seconds_nanos(1, -1_000_000_000), Some((0, 0)));
        assert_eq!(normalize_seconds_nanos(1, 1_000_000_001), Some((2, 1)));
        assert_eq!(
            normalize_seconds_nanos(1, -1_000_000_001),
            Some((-1, 999_999_999))
        );

        assert_eq!(normalize_seconds_nanos(-1, 0), Some((-1, 0)));
        assert_eq!(normalize_seconds_nanos(-1, 1_000_000_000), Some((0, 0)));
        assert_eq!(normalize_seconds_nanos(-1, -1_000_000_000), Some((-2, 0)));
        assert_eq!(normalize_seconds_nanos(-1, 1_000_000_001), Some((0, 1)));
        assert_eq!(
            normalize_seconds_nanos(-1, -1_000_000_001),
            Some((-3, 999_999_999))
        );

        assert_eq!(normalize_seconds_nanos(i64::MAX, 0), Some((i64::MAX, 0)));
        assert_eq!(normalize_seconds_nanos(i64::MAX, 1_000_000_000), None);
        assert_eq!(
            normalize_seconds_nanos(i64::MAX, -1_000_000_000),
            Some((i64::MAX - 1, 0))
        );
        assert_eq!(normalize_seconds_nanos(i64::MAX, 1_000_000_001), None);
        assert_eq!(
            normalize_seconds_nanos(i64::MAX, -1_000_000_001),
            Some((i64::MAX - 2, 999_999_999))
        );

        assert_eq!(normalize_seconds_nanos(i64::MIN, 0), Some((i64::MIN, 0)));
        assert_eq!(
            normalize_seconds_nanos(i64::MIN, 1_000_000_000),
            Some((i64::MIN + 1, 0))
        );
        assert_eq!(normalize_seconds_nanos(i64::MIN, -1_000_000_000), None);
        assert_eq!(
            normalize_seconds_nanos(i64::MIN, 1_000_000_001),
            Some((i64::MIN + 1, 1))
        );
        assert_eq!(normalize_seconds_nanos(i64::MIN, -1_000_000_001), None);
    }
}
