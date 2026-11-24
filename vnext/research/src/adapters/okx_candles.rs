use crate::application::replay::InputBar;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{
    error::Error,
    io::Read,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

const ENDPOINT: &str = "https://www.okx.com/api/v5/market/history-candles";
const MAX_BODY: usize = 128 * 1024;

#[derive(Deserialize)]
pub enum Interval {
    #[serde(rename = "1m")]
    Minute,
    #[serde(rename = "3m")]
    ThreeMinutes,
    #[serde(rename = "5m")]
    FiveMinutes,
    #[serde(rename = "15m")]
    FifteenMinutes,
    #[serde(rename = "30m")]
    ThirtyMinutes,
    #[serde(rename = "1H")]
    Hour,
    #[serde(rename = "4H")]
    FourHours,
    #[serde(rename = "1Dutc")]
    DayUtc,
}

impl Interval {
    fn query_value(&self) -> &'static str {
        match self {
            Self::Minute => "1m",
            Self::ThreeMinutes => "3m",
            Self::FiveMinutes => "5m",
            Self::FifteenMinutes => "15m",
            Self::ThirtyMinutes => "30m",
            Self::Hour => "1H",
            Self::FourHours => "4H",
            Self::DayUtc => "1Dutc",
        }
    }

    pub(crate) fn milliseconds(&self) -> i64 {
        match self {
            Self::Minute => 60_000,
            Self::ThreeMinutes => 180_000,
            Self::FiveMinutes => 300_000,
            Self::FifteenMinutes => 900_000,
            Self::ThirtyMinutes => 1_800_000,
            Self::Hour => 3_600_000,
            Self::FourHours => 14_400_000,
            Self::DayUtc => 86_400_000,
        }
    }
}

#[derive(Serialize)]
pub struct Capture {
    instrument: String,
    interval: &'static str,
    acquired_at_ms: u128,
    response_sha256: String,
}

#[derive(Deserialize)]
struct CandlePage {
    code: String,
    data: Vec<[String; 9]>,
}

pub fn fetch(
    instrument: &str,
    interval: Interval,
    limit: u16,
) -> Result<(Capture, Vec<InputBar>), Box<dyn Error>> {
    fetch_page(ENDPOINT, instrument, interval, limit)
}

fn fetch_page(
    endpoint: &str,
    instrument: &str,
    interval: Interval,
    limit: u16,
) -> Result<(Capture, Vec<InputBar>), Box<dyn Error>> {
    if !(1..=100).contains(&limit) {
        return Err("history candle limit must be between 1 and 100".into());
    }
    if instrument.is_empty()
        || instrument.len() > 64
        || !instrument
            .bytes()
            .all(|byte| byte.is_ascii_uppercase() || byte.is_ascii_digit() || byte == b'-')
    {
        return Err("invalid instrument identifier".into());
    }

    let client = reqwest::blocking::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .timeout(Duration::from_secs(8))
        .build()?;
    let response = client
        .get(endpoint)
        .query(&[
            ("instId", instrument),
            ("bar", interval.query_value()),
            ("limit", &limit.to_string()),
        ])
        .send()?;
    if !response.status().is_success() {
        return Err(format!("OKX history candles HTTP status {}", response.status()).into());
    }
    let mut body = Vec::new();
    response.take(MAX_BODY as u64 + 1).read_to_end(&mut body)?;
    if body.len() > MAX_BODY {
        return Err("history candle response exceeds size limit".into());
    }

    // Record observed availability conservatively: confirmation proves the
    // candle is closed, not that it was available at its theoretical close.
    let acquired_at_ms = SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis();
    let available_at_ms = i64::try_from(acquired_at_ms)?;
    let bars = decode_closed_bars(&body, limit, interval.milliseconds(), available_at_ms)?;
    let capture = Capture {
        instrument: instrument.into(),
        interval: interval.query_value(),
        acquired_at_ms,
        response_sha256: format!("{:x}", Sha256::digest(&body)),
    };
    Ok((capture, bars))
}

fn decode_closed_bars(
    body: &[u8],
    limit: u16,
    interval_ms: i64,
    available_at_ms: i64,
) -> Result<Vec<InputBar>, Box<dyn Error>> {
    let page: CandlePage = serde_json::from_slice(body)?;
    if page.code != "0" {
        return Err(format!("OKX history candles failed with code {}", page.code).into());
    }
    if page.data.len() > usize::from(limit) {
        return Err("OKX returned more candles than requested".into());
    }
    let mut bars = Vec::with_capacity(page.data.len());
    for [timestamp, _, _, _, close, _, _, _, confirmation] in page.data {
        match confirmation.as_str() {
            "0" => continue,
            "1" => {}
            _ => return Err("invalid candle confirmation".into()),
        }
        let timestamp_ms: i64 = timestamp.parse()?;
        let close: f64 = close.parse()?;
        if timestamp_ms <= 0
            || timestamp_ms % interval_ms != 0
            || !close.is_finite()
            || close <= 0.0
            || timestamp_ms
                .checked_add(interval_ms)
                .is_none_or(|closed_at| closed_at > available_at_ms)
        {
            return Err("invalid closed candle".into());
        }
        bars.push(InputBar {
            timestamp_ms,
            available_at_ms,
            close: Some(close),
        });
    }

    if bars.is_empty() {
        return Err("OKX returned no closed candles".into());
    }
    bars.sort_unstable_by_key(|bar| bar.timestamp_ms);
    if bars
        .windows(2)
        .any(|pair| pair[1].timestamp_ms - pair[0].timestamp_ms != interval_ms)
    {
        return Err("closed candles contain a gap or duplicate".into());
    }
    Ok(bars)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use std::{
        io::{BufRead, BufReader, Write},
        net::TcpListener,
    };

    fn row(timestamp: &str, close: &str, confirmation: &str) -> [String; 9] {
        [timestamp, "1", "1", "1", close, "0", "0", "0", confirmation].map(str::to_owned)
    }

    #[test]
    fn fetches_public_page_and_records_the_exact_response_digest() {
        let body = json!({"code":"0", "data":[
            row("180000", "8", "0"),
            row("120000", "4", "1"),
            row("60000", "2", "1")
        ]})
        .to_string();
        let expected_digest = format!("{:x}", Sha256::digest(&body));
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let endpoint = format!(
            "http://{}/api/v5/market/history-candles",
            listener.local_addr().unwrap()
        );
        let server = std::thread::spawn(move || {
            let (mut socket, _) = listener.accept().unwrap();
            let mut reader = BufReader::new(&socket);
            let mut request = String::new();
            loop {
                let mut line = String::new();
                assert_ne!(reader.read_line(&mut line).unwrap(), 0);
                if line == "\r\n" {
                    break;
                }
                request.push_str(&line);
            }
            assert!(request.starts_with(
                "GET /api/v5/market/history-candles?instId=BTC-USDT&bar=1m&limit=3 HTTP/1.1"
            ));
            assert!(!request.to_ascii_lowercase().contains("ok-access"));
            write!(
                socket,
                "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                body.len()
            )
            .unwrap();
        });

        let (capture, bars) = fetch_page(&endpoint, "BTC-USDT", Interval::Minute, 3).unwrap();
        server.join().unwrap();
        assert_eq!(capture.response_sha256, expected_digest);
        assert_eq!(capture.instrument, "BTC-USDT");
        assert!(capture.acquired_at_ms > 0);
        assert!(bars.iter().all(|bar| bar.available_at_ms as u128 == capture.acquired_at_ms));
        let series: Vec<_> = bars
            .iter()
            .map(|bar| (bar.timestamp_ms, bar.close))
            .collect();
        assert_eq!(series, [(60_000, Some(2.0)), (120_000, Some(4.0))]);
    }

    #[test]
    fn rejects_gaps_duplicate_bars_and_invalid_closed_values() {
        for second in [
            row("180000", "3", "1"),
            row("60000", "3", "1"),
            row("120000", "NaN", "1"),
            row("120001", "3", "1"),
            row("120000", "3", "unknown"),
        ] {
            let body = json!({"code":"0", "data":[row("60000", "2", "1"), second]});
            assert!(decode_closed_bars(body.to_string().as_bytes(), 2, 60_000, i64::MAX).is_err());
        }
    }

    #[test]
    fn rejects_observations_not_closed_at_the_recorded_acquisition_time() {
        let body = json!({"code":"0", "data":[row("60000", "2", "1")]});
        assert!(decode_closed_bars(body.to_string().as_bytes(), 1, 60_000, 119_999).is_err());
        assert!(decode_closed_bars(body.to_string().as_bytes(), 1, 60_000, 120_000).is_ok());
    }

    #[test]
    fn rejects_application_errors_and_oversized_pages() {
        assert!(decode_closed_bars(br#"{"code":"50011","data":[]}"#, 1, 60_000, i64::MAX).is_err());
        let body = json!({"code":"0","data":[row("60000","2","1"),row("120000","3","1")]});
        assert!(decode_closed_bars(body.to_string().as_bytes(), 1, 60_000, i64::MAX).is_err());
    }
}
