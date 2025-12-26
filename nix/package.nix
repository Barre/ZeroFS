{
  lib,
  craneLib,
  cmake,
  pkg-config,
  openssl,
}:

let
  protoFilter = path: _type: builtins.match ".*\\.proto$" path != null;
  src = lib.cleanSourceWith {
    src = ./..;
    filter = path: type:
      (protoFilter path type) || (craneLib.filterCargoSources path type);
  };

  inherit (craneLib.crateNameFromCargoToml { cargoToml = ../zerofs/Cargo.toml; }) pname version;

  commonArgs = {
    inherit src pname version;
    cargoLock = ../zerofs/Cargo.lock;
    # Hashes for git dependencies (required when commit is not on default branch)
    outputHashes = {
      "git+https://github.com/slatedb/slatedb.git?rev=22e8070316a3897f4eb5b52a3e5831a8af4485b7#22e8070316a3897f4eb5b52a3e5831a8af4485b7" = "sha256-XSA/t9AF6zC/N6i1IzPTaYJg2Fuo+3Q202tyuNDM2g8=";
    };
    postUnpack = ''
      cd $sourceRoot/zerofs
      sourceRoot="."
    '';
    strictDeps = true;
    nativeBuildInputs = [ cmake pkg-config ];
    buildInputs = [ openssl ];
  };

  cargoArtifacts = craneLib.buildDepsOnly commonArgs;
in
craneLib.buildPackage (commonArgs // {
  inherit cargoArtifacts;
})
