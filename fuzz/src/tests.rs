use rstest::rstest;

use super::database_config;

#[rstest]
#[case::ipv4("host=127.0.0.1 dbname=hubuum_regex_fuzz", true)]
#[case::ipv6("host=::1 dbname=hubuum_regex_fuzz", true)]
#[case::localhost("host=localhost dbname=hubuum_regex_fuzz", true)]
#[case::url("postgres://postgres@127.0.0.1/hubuum_regex_fuzz", true)]
#[case::url_remote_override(
    "postgres://postgres@localhost/hubuum_regex_fuzz?hostaddr=192.0.2.1",
    false
)]
#[case::loopback_override("host=localhost hostaddr=127.0.0.1 dbname=hubuum_regex_fuzz", true)]
#[case::remote_override("host=localhost hostaddr=192.0.2.1 dbname=hubuum_regex_fuzz", false)]
#[case::remote_fallback(
    "host=localhost,localhost hostaddr=127.0.0.1,192.0.2.1 dbname=hubuum_regex_fuzz",
    false
)]
#[case::remote_host("host=192.0.2.1 dbname=hubuum_regex_fuzz", false)]
#[case::other_database("host=127.0.0.1 dbname=application", false)]
#[case::missing_host("dbname=hubuum_regex_fuzz", false)]
#[case::unix_socket("host=/tmp dbname=hubuum_regex_fuzz", false)]
#[case::malformed("this is not a database URL", false)]
fn database_is_a_dedicated_loopback_fixture(#[case] url: &str, #[case] accepted: bool) {
    assert_eq!(database_config(url).is_ok(), accepted);
}
