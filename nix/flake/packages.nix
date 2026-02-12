{ inputs, ... }:
{
  perSystem = { pkgs, ... }:
    let
      craneLib = inputs.crane.mkLib pkgs;
      zerofs = pkgs.callPackage "${inputs.self}/nix/package.nix" { inherit craneLib; };
    in {
      packages = {
        inherit zerofs;
        default = zerofs;
      };
    };
}
