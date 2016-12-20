#[cfg(all(feature = "bit-vec", not(feature = "with-bit-vec")))]
const _CHECK: BitVecFeatureRenamedSeeDocs = ();

#[cfg(all(feature = "chrono", not(feature = "with-chrono")))]
const _CHECK: ChronoFeatureRenamedSeeDocs = ();

#[cfg(all(feature = "eui48", not(feature = "with-eui48")))]
const _CHECK: Eui48FeatureRenamedSeeDocs = ();

#[cfg(all(feature = "openssl", not(feature = "with-openssl")))]
const _CHECK: OpensslFeatureRenamedSeeDocs = ();

#[cfg(all(feature = "rustc-serialize", not(feature = "with-rustc-serialize")))]
const _CHECK: RustcSerializeFeatureRenamedSeeDocs = ();

#[cfg(all(feature = "security-framework", not(feature = "with-security-framework")))]
const _CHECK: SecurityFrameworkFeatureRenamedSeeDocs = ();

#[cfg(all(feature = "serde_json", not(feature = "with-serde_json")))]
const _CHECK: SerdeJsonFeatureRenamedSeeDocs = ();

#[cfg(all(feature = "time", not(feature = "with-time")))]
const _CHECK: TimeFeatureRenamedSeeDocs = ();

#[cfg(all(feature = "unix_socket", not(feature = "with-unix_socket")))]
const _CHECK: UnixSocketFeatureRenamedSeeDocs = ();

#[cfg(all(feature = "uuid", not(feature = "with-uuid")))]
const _CHECK: UuidFeatureRenamedSeeDocs = ();
