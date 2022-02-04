use std::{fmt, time::Duration};

const ONE_MINUTE: u64 = 60;
const ONE_HOUR: u64 = ONE_MINUTE * 60;
const ONE_DAY: u64 = ONE_HOUR * 24;
const ONE_WEEK: u64 = ONE_DAY * 7;
const ONE_YEAR: u64 = ONE_DAY * 365;

pub struct TimeDuration(pub f64);

impl fmt::Display for TimeDuration {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		let floor = ((self.0 * 1000.0).floor()) as u64;
		let duration = Duration::from_millis(floor);
		let millis = duration.subsec_millis();
		let secs = duration.as_secs();
		let years = (secs / ONE_YEAR) as u32;
		let years_rem = secs % ONE_YEAR;
		let weeks = (years_rem / ONE_WEEK) as u32;
		let weeks_rem = years_rem % ONE_WEEK;
		let days = (weeks_rem / ONE_DAY) as u32;
		let days_rem = weeks_rem % ONE_DAY;
		let hours = (days_rem / ONE_HOUR) as u32;
		let hours_rem = days_rem % ONE_HOUR;
		let minutes = (hours_rem / ONE_MINUTE) as u32;
		let seconds = (hours_rem % ONE_MINUTE) as u32;

		for (d, unit) in [
			(years, "y"),
			(weeks, "w"),
			(days, "d"),
			(hours, "h"),
			(minutes, "m"),
			(seconds, "s"),
			(millis, "ms"),
		] {
			if d != 0 {
				write!(f, "{}{}", d, unit)?;
			}
		}
		Ok(())
	}
}

#[cfg(test)]
mod test {

	use super::*;

	#[test]
	fn durations() {
		assert_eq!(&TimeDuration((5 * ONE_MINUTE) as f64).to_string(), "5m");
		assert_eq!(&TimeDuration((ONE_MINUTE + 30) as f64).to_string(), "1m30s");
		assert_eq!(
			&TimeDuration(
				(2 * ONE_YEAR + 25 * ONE_WEEK + 3 * ONE_DAY + 10 * ONE_HOUR + 30 * ONE_MINUTE + 45)
					as f64
			)
			.to_string(),
			"2y25w3d10h30m45s"
		);
	}
}
