# Portable secret archives use the same rickai/secrets image, AES256 GPG
# format, Bitwarden CLI state, and "Services secrets passphrase" item as
# Heromaton and Laba.  Deliberately do not forward SECRETS_PASSPHRASE here:
# for this service Bitwarden is the only interactive passphrase source.
secrets +cmd:
    scripts/secrets-tool.sh {{cmd}}
