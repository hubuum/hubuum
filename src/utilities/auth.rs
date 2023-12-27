extern crate rand;
extern crate scrypt;

use rand::distributions::Alphanumeric;
use rand::{thread_rng, Rng};

use scrypt::{
    password_hash::{rand_core::OsRng, PasswordHasher, SaltString},
    Scrypt,
};

pub fn hash_password(password: &str) -> Result<String, scrypt::password_hash::Error> {
    let salt = SaltString::generate(&mut OsRng);
    let password_hash = Scrypt
        .hash_password(password.as_bytes(), &salt)?
        .to_string();

    Ok(password_hash)
}

pub fn generate_random_password(length: usize) -> String {
    let mut rng = thread_rng();
    std::iter::repeat(())
        .map(|()| rng.sample(Alphanumeric))
        .map(char::from)
        .take(length)
        .collect()
}
