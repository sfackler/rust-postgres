{ pkgs ? import <nixpkgs> {} }:

with pkgs;

mkShell {
  LIBCLANG_PATH="${pkgs.llvmPackages.libclang}/lib";
  buildInputs = with pkgs; [
    openssl
    pkg-config
    clangStdenv
    llvmPackages.libclang
    kerberos
  ];
}
