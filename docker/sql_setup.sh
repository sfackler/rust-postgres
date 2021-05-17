#!/bin/bash
set -e

cat > "$PGDATA/server.key" <<-EOKEY
-----BEGIN PRIVATE KEY-----
MIIEvgIBADANBgkqhkiG9w0BAQEFAASCBKgwggSkAgEAAoIBAQDX71FGKHUepupP
7c4VGoeX+Dkn5PZjDkwv+DYsTxpcZ0x/S4fHSz+aEX7cP8M0vxPmxOwJPoAm2Vt2
RTva+8TrGClzWPT8YHzyN7r8jdP9crBQDAidYuQdHxPpXji9TMyRAGk51Qy0U+K/
Wjx787LsWF3DFTgvJdw7YaD9EZzG7sqXvCaWAQ8sxUdj0moSm42ftVFx4Ztv5JEo
nAgKkuJ+0KklLJ08/KTp8OMm95PJzHPl+MDxqDv3jqvg5eVUnm+nE9kgZvTBtNNH
YJM3VZ0rjQDThNlLz9PdHWpfKlBTv1uVGg2SWfVpiaqxq4n686QZPHS0fu/ZQejx
h0bZ8qeBAgMBAAECggEBAKKZ3XOdJ4RrYGnLwrF1hsFS84ctDLPOomRE3lZDQrBu
QNZiQ944ta4ImqSzhwUDFbNiefMEE3AtoIfQ3p+pksENMrlfNSuOZMfoW2+uRQHH
CSldxmbtfqTHMDE8+DDj0e8mhhY8bhKkUEyTYJReEE+UwxYRtnsaYVp9y8KFLq9E
4f6NDMzzSpw/ujkcACtx0DxeWZfaP6Ms4ydh2uDEvzUwnmw4kpsgo4NtPLHDNx/y
kshfSpayYBKJ08qpzUAOXpi2UIRzvrYZE5cAcXtK6Jw02VnpNIr6+q/DAE58at7W
RwvswhNdpVVVwn04o68c4GUsGQFG/Qve8hDLdN+KygECgYEA//PE9FAeuSy2KWxS
HKSYZ422Sx/M4tuAQrX//yCFizxEhs9SF3ybZX4SCHGeQxeogIqOrFKKrXpzpCcH
3fB4LjRpdUKdv11sxFoo0Jw6wtY3N+24yM3jrpDsQqcCxUxm+qOgwGyKRJCkLxK3
RNkAAmoT2mONeaMyWLg5g4wVW5ECgYEA1/miyi6PmT3+y4DCdNYdJll3NRmR4DGk
HDYOd+Qb+DSoBhcxz/bqBDDdXr6FT3nZEkTxKAsaPjarzKjK2J88fvnF2JRnM5Oy
HKRNk3a9KxM//UwUgoLCdg/qZe4EXX9LJr06G6YGgg0uG6Cjsa+rZ33FiucBYrEL
aevQ+cReNPECgYEAsCDlRWHk4nQ8HiEmGAPDxG6mJOgLK4j0p/Np5/xPKVMdrM75
pKPgo2SvsBPPXkfnchzmtPpP57S94xXguf8CFHmIoGJo/wihEjUgpPz9CpoygVAa
ukPEC5o6mlsm8vHyY0M6GXAXbbtC4Am3B69z7DVm1/9tmWiN+rM7EKTTBaECgYBi
qOUWmyJ6DHoCmLU8DjuOszvjg+TBl6uyP3doiUnFnrhK3/mfWNoaRAA8MahQYAcr
c1b+xeOdG/hrK4hOYJ+QGaWphFGInCW3M89EV++eZ9LJcSHFZNpUeHzJR2uzEl1Q
Owz6aGN8sWyorj9ZAji4tBmzlEdrwBjIsDLshinK4QKBgEbmw1Dp1ZQEZcNiNKBl
EEzce+yHf8FSaC4KQSOnZIK30ZoHGLkQfr+C+8qKeDe4WYn3yf5zhjG7ssyxgWrB
S8GdV0OgrtvO5zhDH72KqddZe+api/34Zh2zY/2PKG2gBZ+ubsRpgptVK2ny5pmj
WN5CmfEv9kwQmSKzzSGUJ59l
-----END PRIVATE KEY-----
EOKEY
chmod 0600 "$PGDATA/server.key"

cat > "$PGDATA/server.crt" <<-EOCERT
-----BEGIN CERTIFICATE-----
MIICojCCAYoCCQD51cTqxXxVZDANBgkqhkiG9w0BAQsFADASMRAwDgYDVQQDDAdy
b290LWNhMB4XDTIxMDUxNzIxMDExM1oXDTIyMDUxNzIxMDExM1owFDESMBAGA1UE
AwwJbG9jYWxob3N0MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEA1+9R
Rih1HqbqT+3OFRqHl/g5J+T2Yw5ML/g2LE8aXGdMf0uHx0s/mhF+3D/DNL8T5sTs
CT6AJtlbdkU72vvE6xgpc1j0/GB88je6/I3T/XKwUAwInWLkHR8T6V44vUzMkQBp
OdUMtFPiv1o8e/Oy7FhdwxU4LyXcO2Gg/RGcxu7Kl7wmlgEPLMVHY9JqEpuNn7VR
ceGbb+SRKJwICpLiftCpJSydPPyk6fDjJveTycxz5fjA8ag7946r4OXlVJ5vpxPZ
IGb0wbTTR2CTN1WdK40A04TZS8/T3R1qXypQU79blRoNkln1aYmqsauJ+vOkGTx0
tH7v2UHo8YdG2fKngQIDAQABMA0GCSqGSIb3DQEBCwUAA4IBAQAqJLwuK1HIq2p0
I4N7HUjApiyxYWKqAeiC/65sLyU5TUfTgiSxTJRh625NwEYXzGTGbY674quIYK7I
uUIDrWTu2GBT1DIZJG78xbYfeWoHtKrTZ+MYy70FK448dI4lv0lZbmub0HircR2M
9MVqhWw8ik5FrpiR2DcwTkwNuNlVSu+hr/c/ljhvNP7dBfIxc9Og6xp1tyHW2hce
Vm/3HFjJqBfLw/lbZ6rx5wJA3E13r0LpnwuKQlgPYyaighfgetJdxorj37gCxLn3
77qfpOnFfk/mgY+bLFu9ncR2svab4CGRXPey9Kb6wP+OCwnh0vCBioocUFRANkLb
bEjAYyqo
-----END CERTIFICATE-----
EOCERT

cat > "$PGDATA/root.crt" <<-EOCERT
-----BEGIN CERTIFICATE-----
MIIDHTCCAgWgAwIBAgIJAPuMcWp8Si1PMA0GCSqGSIb3DQEBCwUAMBIxEDAOBgNV
BAMMB3Jvb3QtY2EwHhcNMjEwNTE3MjEwMTEzWhcNMzEwNTE1MjEwMTEzWjASMRAw
DgYDVQQDDAdyb290LWNhMIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEA
vhPvG4K6R1GAe3/MwLWsb3uQX3Zs8Z+1R15+h1Sx1SwgXLuLDwxMxg0dkip/R0ic
XyJFeVntOQqZfZpwd3iD47AZx6c4/Hn+U6OQtfvY6FmYNfmhngAnk9nr5te4Fu+S
n7YwUJ0+pfC8b6idM5XB2YBnO1azqP5Sa230gSqxBlzjOqUC8rlvF1woDej49E3l
pzP7jD6yrZ3Z3SvF1+ZhW+6CvWi4cm8xfMaTCCvwoR3E7ia6OGUmNP/rkyxXSUHV
O1ELw0FY63+J46BzONR/MRuoXBm2SF07WY9+kS35SIOK1PjO0ndNRlXYv7xzCxDW
4EnfoTfDLwZ6vOBvcST9VwIDAQABo3YwdDAPBgNVHRMBAf8EBTADAQH/MB0GA1Ud
DgQWBBTbVbnrDPLMcVV0ExWd/EexzCMhzDBCBgNVHSMEOzA5gBTbVbnrDPLMcVV0
ExWd/EexzCMhzKEWpBQwEjEQMA4GA1UEAwwHcm9vdC1jYYIJAPuMcWp8Si1PMA0G
CSqGSIb3DQEBCwUAA4IBAQA83u6ILbpsQRwyb074exRo2vLC0pjtOBeRLyhi95zk
TtilDHNP5oYf4pmrTAagv+i5eOwwAvoaXil1+mAtckUkV0FRoxAX9U6ZTUFge9HE
G0VLfhqmzlExRl7O6Jr/O7fC6hOz5YDD0SdAaLGx35J9kbWyOLXAWCte3FImetdB
72lbGD8M9J9Sm12aN+e9a8xovFQQG8Sah4XVTubs3Yw8QOhs+kxIrw3LzRt3Nisa
ASCK93sHNpRUfePn/9x+2VAd6p1r4ypDJAH9Tr1E7duPBe+2YwBjMMDviA7eCiFA
Xi7zm5vUeHGuQOBUIz6HE7RGMhQNkORbQiopzVFOBkys
-----END CERTIFICATE-----
EOCERT

cat >> "$PGDATA/postgresql.conf" <<-EOCONF
port = 5433
ssl = on
ssl_cert_file = 'server.crt'
ssl_key_file = 'server.key'
ssl_ca_file = 'root.crt'
EOCONF

cat > "$PGDATA/pg_hba.conf" <<-EOCONF
# TYPE  DATABASE        USER            ADDRESS                 METHOD
host    all             pass_user       0.0.0.0/0            password
host    all             md5_user        0.0.0.0/0            md5
host    all             scram_user      0.0.0.0/0            scram-sha-256
host    all             pass_user       ::0/0                password
host    all             md5_user        ::0/0                md5
host    all             scram_user      ::0/0                scram-sha-256

hostssl all             ssl_user        0.0.0.0/0            trust
hostssl all             ssl_user        ::0/0                trust
host    all             ssl_user        0.0.0.0/0            reject
host    all             ssl_user        ::0/0                reject

hostssl all             cert_user_ca    0.0.0.0/0            trust clientcert=verify-ca
hostssl all             cert_user_ca    ::0/0                trust clientcert=verify-ca
host    all             cert_user_ca    0.0.0.0/0            reject
host    all             cert_user_ca    ::0/0                reject

hostssl all             cert_user_full  0.0.0.0/0            trust clientcert=verify-full
hostssl all             cert_user_full  ::0/0                trust clientcert=verify-full
host    all             cert_user_full  0.0.0.0/0            reject
host    all             cert_user_full  ::0/0                reject

# IPv4 local connections:
host    all             postgres        0.0.0.0/0            trust
# IPv6 local connections:
host    all             postgres        ::0/0                trust
# Unix socket connections:
local   all             postgres                             trust
EOCONF

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" <<-EOSQL
    CREATE ROLE pass_user PASSWORD 'password' LOGIN;
    CREATE ROLE md5_user PASSWORD 'password' LOGIN;
    SET password_encryption TO 'scram-sha-256';
    CREATE ROLE scram_user PASSWORD 'password' LOGIN;
    CREATE ROLE ssl_user LOGIN;
    CREATE ROLE cert_user_ca LOGIN;
    CREATE ROLE cert_user_full LOGIN;
    CREATE EXTENSION hstore;
    CREATE EXTENSION citext;
EOSQL
